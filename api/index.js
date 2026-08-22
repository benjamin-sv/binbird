const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const readline = require("readline");
const sharp = require("sharp");

const {
  S3Client,
  ListObjectsV2Command,
  HeadObjectCommand,
  GetObjectCommand,
  PutObjectCommand
} = require("@aws-sdk/client-s3");
const {
  DynamoDBClient,
  QueryCommand,
  GetItemCommand,
  PutItemCommand,
  DeleteItemCommand,
  UpdateItemCommand,
  BatchGetItemCommand,
  BatchWriteItemCommand
} = require("@aws-sdk/client-dynamodb");
const {
  SQSClient,
  SendMessageBatchCommand
} = require("@aws-sdk/client-sqs");

const REGION = process.env.AWS_REGION || "ap-southeast-2";
const DATA_BUCKET = process.env.DATA_BUCKET || "bird-detection-data";
const DETECTIONS_PREFIX = normalizePrefix(process.env.DETECTIONS_PREFIX || "detections/");
const PROGRESS_TABLE = process.env.PROGRESS_TABLE;
const ACCESS_KEY = process.env.ACCESS_KEY || "";
const IMAGE_URL_SECRET = process.env.IMAGE_URL_SECRET || ACCESS_KEY || "dev-secret";
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";
const IMAGE_URL_SECONDS = Number(process.env.IMAGE_URL_SECONDS || "3600");
const S3_IMAGE_URL_SECONDS = Number(process.env.S3_IMAGE_URL_SECONDS || "3600");
const INDEX_VERSION = 2;
const CROP_CACHE_VERSION = "yx-v1";
const CROP_WORKER_MAX_GROUPS = Number(process.env.CROP_WORKER_MAX_GROUPS || "40");
const CROP_WORKER_MAX_DETECTIONS = Number(process.env.CROP_WORKER_MAX_DETECTIONS || "8000");
const CROP_WORKER_MAX_MILLIS = Number(process.env.CROP_WORKER_MAX_MILLIS || "720000");
const CROP_UPLOAD_CONCURRENCY = Number(process.env.CROP_UPLOAD_CONCURRENCY || "8");
const CROP_SHARD_GROUPS = Number(process.env.CROP_SHARD_GROUPS || "10");
const CROP_QUEUE_URL = process.env.CROP_QUEUE_URL || "";
const NO_SUBCATEGORY = "__none__";

const s3 = new S3Client({ region: REGION });
const dynamo = new DynamoDBClient({ region: REGION });
const sqs = new SQSClient({ region: REGION });

const config = JSON.parse(fs.readFileSync(path.join(__dirname, "config.json"), "utf8"));
const configuredCategorySets = normalizeCategorySets(config);
const falseDetectionCategory = normalizeCategory(
  config.false_detection_category
    || (config.categories || []).find((item) => item.is_false_detection)
    || {
      id: 0,
      key: "nothing",
      label: "Nothing / false detection",
      shortcut: "N",
      requires_subcategory: false,
      is_false_detection: true
    }
);
const allConfiguredCategories = configuredCategoriesList(configuredCategorySets, falseDetectionCategory);
const categoriesByKey = new Map(allConfiguredCategories.map((item) => [item.key, item]));
const categoriesById = new Map(allConfiguredCategories.map((item) => [String(item.id), item]));
const subcategoriesById = new Map((config.subcategories || []).map((item) => [String(item.id), item]));
const sourceLabelOverrides = config.source_label_overrides || {};
const categoryLookupCache = new Map();
const indexCache = new Map();
const sourceImageCache = new Map();

exports.handler = async (event) => {
  try {
    if (Array.isArray(event?.Records) && event.Records[0]?.eventSource === "aws:sqs") {
      for (const record of event.Records) {
        await handleCropWorker(JSON.parse(record.body));
      }
      return { batchItemFailures: [] };
    }

    if (event?.worker === "crop-generation") {
      return await handleCropWorker(event);
    }

    if (event.requestContext?.http?.method === "OPTIONS") {
      return empty(204);
    }

    const route = event.rawPath || "/";
    const method = event.requestContext?.http?.method || "GET";

    if (route === "/api/image" && method === "GET") {
      return await handleImage(event);
    }

    if (!isAuthorized(event)) {
      return json(401, { error: "Access key required or invalid.", code: "UNAUTHORIZED" });
    }

    if (route === "/api/config" && method === "GET") {
      const publicConfig = { ...config };
      delete publicConfig.source_label_overrides;
      return json(200, publicConfig);
    }
    if (route === "/api/jobs" && method === "GET") {
      return json(200, { jobs: await listJobs() });
    }
    if (route === "/api/summary" && method === "GET") {
      const jobId = requiredQuery(event, "job_id");
      const index = await getJobIndex(jobId);
      const cropStatus = await ensureCropGenerationStarted(jobId, index);
      return json(200, summaryWithCropStatus(index.summary, cropStatus));
    }
    if (route === "/api/crops/status" && method === "GET") {
      const jobId = requiredQuery(event, "job_id");
      const index = await getJobIndex(jobId);
      const status = await getCropStatus(jobId);
      return json(200, { crop_generation: publicCropStatus(status, index.summary.total) });
    }
    if (route === "/api/crops/start" && method === "POST") {
      const body = parseBody(event);
      const jobId = requireBody(body, "job_id");
      const index = await getJobIndex(jobId);
      const status = await startCropGeneration(jobId, index, Boolean(body.force));
      return json(200, { crop_generation: publicCropStatus(status, index.summary.total) });
    }
    if (route === "/api/progress" && method === "GET") {
      const jobId = requiredQuery(event, "job_id");
      return json(200, await progressSnapshot(jobId));
    }
    if (route === "/api/detections" && method === "GET") {
      return json(200, await handleDetections(event));
    }
    if (route === "/api/detection_ids" && method === "GET") {
      return json(200, await handleDetectionIds(event));
    }
    if (route === "/api/detection" && method === "GET") {
      return json(200, await handleDetection(event));
    }
    if (route === "/api/detections/by-id" && method === "POST") {
      return json(200, await handleDetectionsById(event));
    }
    if (route === "/api/progress/selection" && method === "POST") {
      return json(200, await handleSelection(event));
    }
    if (route === "/api/progress/review" && method === "POST") {
      return json(200, await handleReview(event));
    }
    if (route === "/api/progress/undo" && method === "POST") {
      return json(200, await handleUndo(event));
    }

    return json(404, { error: "Not found", code: "NOT_FOUND", path: route });
  } catch (error) {
    console.error(error);
    const status = error.statusCode || 500;
    return json(status, {
      error: error.publicMessage || error.message || "Unexpected server error",
      code: error.code || "SERVER_ERROR",
      detail: error.detail
    });
  }
};

async function listJobs() {
  const response = await s3.send(new ListObjectsV2Command({
    Bucket: DATA_BUCKET,
    Prefix: DETECTIONS_PREFIX,
    Delimiter: "/",
    MaxKeys: 1000
  }));
  const prefixes = response.CommonPrefixes || [];
  const jobs = [];
  for (const item of prefixes) {
    const prefix = item.Prefix;
    const jobId = prefix.slice(DETECTIONS_PREFIX.length).replace(/\/$/, "");
    if (!jobId) {
      continue;
    }
    const resultKey = `${prefix}detection_results.txt`;
    try {
      await s3.send(new HeadObjectCommand({ Bucket: DATA_BUCKET, Key: resultKey }));
    } catch (error) {
      continue;
    }
    let count = null;
    let missingCropCount = null;
    try {
      const summary = await readSummary(jobId);
      const cropStatus = await getCropStatus(jobId);
      count = summary.total;
      missingCropCount = cropMissingCount(cropStatus, summary.total);
    } catch (error) {
      // The summary will be created when the job is opened.
    }
    jobs.push({
      id: jobId,
      label: jobId,
      count,
      missing_crop_count: missingCropCount,
      indexed: count !== null
    });
  }
  return jobs.sort((a, b) => b.id.localeCompare(a.id));
}

async function handleDetections(event) {
  const query = event.queryStringParameters || {};
  const jobId = requiredQuery(event, "job_id");
  const offset = clampInt(query.offset, 0, 0, Number.MAX_SAFE_INTEGER);
  const limit = clampInt(query.limit, 300, 1, 1000);
  const items = filterDetections(await getJobIndex(jobId), query);
  const page = items.slice(offset, offset + limit).map((item) => withImageUrl(event, item));
  const flags = await loadFlags(jobId, page.map((item) => item.id));
  return {
    total: items.length,
    offset,
    limit,
    detections: page.map((item) => ({
      ...item,
      selected_wrong: flags.selected.has(item.id),
      reviewed: flags.reviewed.has(item.id)
    }))
  };
}

async function handleDetectionIds(event) {
  const jobId = requiredQuery(event, "job_id");
  const items = filterDetections(await getJobIndex(jobId), event.queryStringParameters || {});
  return { ids: items.map((item) => item.id), count: items.length };
}

async function handleDetection(event) {
  const query = event.queryStringParameters || {};
  const id = requiredQuery(event, "id");
  const jobId = query.job_id || jobIdFromDetectionId(id);
  const index = await getJobIndex(jobId);
  const detection = index.byId[id];
  if (!detection) {
    throw httpError(404, "Detection not found.", "DETECTION_NOT_FOUND", { job_id: jobId, detection_id: id });
  }
  return withImageUrl(event, detection);
}

async function handleDetectionsById(event) {
  const body = parseBody(event);
  const ids = Array.isArray(body.ids) ? body.ids : [];
  const grouped = new Map();
  for (const id of ids) {
    const jobId = jobIdFromDetectionId(id);
    grouped.set(jobId, [...(grouped.get(jobId) || []), id]);
  }
  const detections = [];
  for (const [jobId, jobIds] of grouped.entries()) {
    const index = await getJobIndex(jobId);
    for (const id of jobIds) {
      if (index.byId[id]) {
        detections.push(withImageUrl(event, index.byId[id]));
      }
    }
  }
  return { detections };
}

async function handleSelection(event) {
  const body = parseBody(event);
  const jobId = requireBody(body, "job_id");
  const ids = Array.isArray(body.ids) ? body.ids : [];
  const selected = Boolean(body.selected);
  await writeSelection(jobId, ids, selected);
  return {
    selected_wrong_count: (await queryPrefix(jobPk(jobId), "SELECTED#")).length,
    progress: await progressSnapshot(jobId)
  };
}

async function handleReview(event) {
  const body = parseBody(event);
  const jobId = requireBody(body, "job_id");
  const id = requireBody(body, "id");
  const review = body.review || {};
  const existing = await getReview(jobId, id);
  const stored = {
    ...review,
    reviewed_at: new Date().toISOString()
  };
  await dynamo.send(new PutItemCommand({
    TableName: PROGRESS_TABLE,
    Item: {
      pk: { S: jobPk(jobId) },
      sk: { S: `REVIEW#${id}` },
      detection_id: { S: id },
      review_json: { S: JSON.stringify(stored) },
      reviewed_at: { S: stored.reviewed_at }
    }
  }));
  await writeSelection(jobId, [id], true);
  const eventId = `EVENT#${Date.now().toString().padStart(15, "0")}#${crypto.randomUUID()}`;
  await dynamo.send(new PutItemCommand({
    TableName: PROGRESS_TABLE,
    Item: {
      pk: { S: jobPk(jobId) },
      sk: { S: eventId },
      detection_id: { S: id },
      previous_review_json: existing ? { S: JSON.stringify(existing) } : { NULL: true },
      created_at: { S: stored.reviewed_at }
    }
  }));
  return { review: stored, progress: await progressSnapshot(jobId) };
}

async function handleUndo(event) {
  const body = parseBody(event);
  const jobId = requireBody(body, "job_id");
  const events = await queryPrefix(jobPk(jobId), "EVENT#", 1, false);
  if (!events.length) {
    return {
      undone: false,
      message: "There is no review history to undo for this job.",
      progress: await progressSnapshot(jobId)
    };
  }
  const eventItem = events[0];
  const id = eventItem.detection_id.S;
  const previous = eventItem.previous_review_json?.S
    ? JSON.parse(eventItem.previous_review_json.S)
    : null;
  if (previous) {
    await dynamo.send(new PutItemCommand({
      TableName: PROGRESS_TABLE,
      Item: {
        pk: { S: jobPk(jobId) },
        sk: { S: `REVIEW#${id}` },
        detection_id: { S: id },
        review_json: { S: JSON.stringify(previous) },
        reviewed_at: { S: previous.reviewed_at || new Date().toISOString() }
      }
    }));
  } else {
    await dynamo.send(new DeleteItemCommand({
      TableName: PROGRESS_TABLE,
      Key: { pk: { S: jobPk(jobId) }, sk: { S: `REVIEW#${id}` } }
    }));
  }
  await dynamo.send(new DeleteItemCommand({
    TableName: PROGRESS_TABLE,
    Key: { pk: { S: jobPk(jobId) }, sk: eventItem.sk }
  }));
  return {
    undone: true,
    id,
    restored_review: previous,
    progress: await progressSnapshot(jobId)
  };
}

async function progressSnapshot(jobId) {
  const selected = await queryPrefix(jobPk(jobId), "SELECTED#");
  const reviews = await queryPrefix(jobPk(jobId), "REVIEW#");
  const events = await queryPrefix(jobPk(jobId), "EVENT#", 500, false);
  const reviewMap = {};
  for (const item of reviews) {
    reviewMap[item.detection_id.S] = JSON.parse(item.review_json.S);
  }
  return {
    version: 1,
    job_id: jobId,
    selected_wrong: selected.map((item) => item.detection_id.S),
    reviews: reviewMap,
    review_history: events.map((item) => ({
      id: item.detection_id.S,
      event_id: item.sk.S,
      created_at: item.created_at?.S || null
    }))
  };
}

async function getReview(jobId, id) {
  const response = await dynamo.send(new GetItemCommand({
    TableName: PROGRESS_TABLE,
    Key: { pk: { S: jobPk(jobId) }, sk: { S: `REVIEW#${id}` } }
  }));
  return response.Item?.review_json?.S ? JSON.parse(response.Item.review_json.S) : null;
}

async function writeSelection(jobId, ids, selected) {
  const requests = ids.map((id) => {
    const key = { pk: { S: jobPk(jobId) }, sk: { S: `SELECTED#${id}` } };
    if (!selected) {
      return { DeleteRequest: { Key: key } };
    }
    return {
      PutRequest: {
        Item: {
          ...key,
          detection_id: { S: id },
          selected_at: { S: new Date().toISOString() }
        }
      }
    };
  });
  for (const chunk of chunks(requests, 25)) {
    if (!chunk.length) {
      continue;
    }
    await dynamo.send(new BatchWriteItemCommand({ RequestItems: { [PROGRESS_TABLE]: chunk } }));
  }
}

async function loadFlags(jobId, ids) {
  const selected = new Set();
  const reviewed = new Set();
  const keys = [];
  for (const id of ids) {
    keys.push({ pk: { S: jobPk(jobId) }, sk: { S: `SELECTED#${id}` } });
    keys.push({ pk: { S: jobPk(jobId) }, sk: { S: `REVIEW#${id}` } });
  }
  for (const chunk of chunks(keys, 100)) {
    if (!chunk.length) {
      continue;
    }
    const response = await dynamo.send(new BatchGetItemCommand({
      RequestItems: { [PROGRESS_TABLE]: { Keys: chunk } }
    }));
    for (const item of response.Responses?.[PROGRESS_TABLE] || []) {
      if (item.sk.S.startsWith("SELECTED#")) {
        selected.add(item.detection_id.S);
      } else if (item.sk.S.startsWith("REVIEW#")) {
        reviewed.add(item.detection_id.S);
      }
    }
  }
  return { selected, reviewed };
}

async function queryPrefix(pk, prefix, limit = null, forward = true) {
  const items = [];
  let ExclusiveStartKey = undefined;
  do {
    const response = await dynamo.send(new QueryCommand({
      TableName: PROGRESS_TABLE,
      KeyConditionExpression: "pk = :pk AND begins_with(sk, :prefix)",
      ExpressionAttributeValues: {
        ":pk": { S: pk },
        ":prefix": { S: prefix }
      },
      Limit: limit || undefined,
      ScanIndexForward: forward,
      ExclusiveStartKey
    }));
    items.push(...(response.Items || []));
    if (limit && items.length >= limit) {
      return items.slice(0, limit);
    }
    ExclusiveStartKey = response.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return items;
}

async function getJobIndex(jobId) {
  if (indexCache.has(jobId)) {
    return indexCache.get(jobId);
  }
  try {
    const index = await readIndex(jobId);
    indexCache.set(jobId, index);
    return index;
  } catch (error) {
    const index = await buildIndex(jobId);
    indexCache.set(jobId, index);
    return index;
  }
}

async function readIndex(jobId) {
  const key = indexKey(jobId);
  const response = await s3.send(new GetObjectCommand({ Bucket: DATA_BUCKET, Key: key }));
  const index = JSON.parse(await streamToString(response.Body));
  if (index.version !== INDEX_VERSION) {
    throw new Error(`Cached review index is version ${index.version || "unknown"}; expected ${INDEX_VERSION}.`);
  }
  index.byId = Object.fromEntries(index.detections.map((item) => [item.id, item]));
  return index;
}

async function readSummary(jobId) {
  const response = await s3.send(new GetObjectCommand({ Bucket: DATA_BUCKET, Key: summaryKey(jobId) }));
  return JSON.parse(await streamToString(response.Body));
}

async function buildIndex(jobId) {
  const resultKey = `${DETECTIONS_PREFIX}${jobId}/detection_results.txt`;
  let response;
  try {
    response = await s3.send(new GetObjectCommand({ Bucket: DATA_BUCKET, Key: resultKey }));
  } catch (error) {
    throw httpError(404, "Job detection_results.txt was not found.", "JOB_RESULTS_NOT_FOUND", {
      job_id: jobId,
      detail: `s3://${DATA_BUCKET}/${resultKey}`
    });
  }
  const parsedRows = [];
  const lines = readline.createInterface({
    input: response.Body,
    crlfDelay: Infinity
  });
  for await (const line of lines) {
    if (!line.trim()) {
      continue;
    }
    const parsed = parseDetectionLine(line);
    if (!parsed) {
      continue;
    }
    parsedRows.push(parsed);
  }
  const jobType = detectJobType(parsedRows);
  const detections = parsedRows.map((parsed) => recordDetection(jobId, parsed, jobType));
  const summary = buildSummary(detections, jobType);
  const index = {
    version: INDEX_VERSION,
    job_id: jobId,
    job_type: jobType,
    generated_at: new Date().toISOString(),
    detections,
    summary
  };
  await s3.send(new PutObjectCommand({
    Bucket: DATA_BUCKET,
    Key: indexKey(jobId),
    Body: JSON.stringify(index),
    ContentType: "application/json"
  }));
  await s3.send(new PutObjectCommand({
    Bucket: DATA_BUCKET,
    Key: summaryKey(jobId),
    Body: JSON.stringify(summary),
    ContentType: "application/json"
  }));
  index.byId = Object.fromEntries(detections.map((item) => [item.id, item]));
  return index;
}

function recordDetection(jobId, parsed, jobType) {
  const category = categoryForLabel(parsed.raw_label, Boolean(parsed.predicted_subcategory), jobType);
  const [subcategoryFilter] = subcategoryFilterFor(category.override, parsed.predicted_subcategory);
  const source = parseS3Url(parsed.source_path);
  const detectionId = parsed.detection_id;
  return {
    id: `${jobId}/${detectionId}`,
    detection_id: detectionId,
    job_id: jobId,
    job_type: jobType,
    site: source.site || jobId,
    source_bucket: source.bucket || DATA_BUCKET,
    source_key: source.key || parsed.source_path.replace(/^\/+/, ""),
    bbox: parsed.bbox,
    confidence: parsed.confidence,
    raw_label: parsed.raw_label,
    predicted_category_id: category.item.id,
    predicted_category_key: category.item.key,
    predicted_category_requires_subcategory: category.item.requires_subcategory !== false,
    predicted_subcategory_name: parsed.predicted_subcategory,
    predicted_subcategory_filter: subcategoryFilter
  };
}

function filterDetections(index, query) {
  let items = index.detections;
  if (query.category_id && query.category_id !== "all") {
    items = items.filter((item) => String(item.predicted_category_id) === String(query.category_id));
  }
  if (query.subcategory && query.subcategory !== "all") {
    items = items.filter((item) => item.predicted_subcategory_filter === query.subcategory);
  }
  return items;
}

function buildSummary(detections, jobType = detections[0]?.job_type || "custom") {
  const categoryCounts = {};
  const subcategoryCounts = {};
  const rawLabelCounts = {};
  const dynamicCategories = {};
  const configuredCategories = categoriesForJobType(jobType);
  const configuredCategoryIds = new Set(configuredCategories.map((category) => String(category.id)));
  for (const detection of detections) {
    const hydrated = hydrateDetection(detection);
    const categoryId = String(hydrated.predicted_category_id);
    categoryCounts[categoryId] = (categoryCounts[categoryId] || 0) + 1;
    rawLabelCounts[hydrated.raw_label] = (rawLabelCounts[hydrated.raw_label] || 0) + 1;
    if (!configuredCategoryIds.has(categoryId)) {
      dynamicCategories[categoryId] = dynamicCategories[categoryId] || {
        id: hydrated.predicted_category_id,
        key: hydrated.predicted_category_key,
        label: hydrated.predicted_category_label,
        shortcut: "",
        requires_subcategory: Boolean(hydrated.predicted_category_requires_subcategory),
        dynamic: true
      };
      dynamicCategories[categoryId].requires_subcategory =
        dynamicCategories[categoryId].requires_subcategory ||
        Boolean(hydrated.predicted_category_requires_subcategory);
    }
    if (hydrated.predicted_category_requires_subcategory !== false || hydrated.predicted_subcategory_filter !== NO_SUBCATEGORY) {
      const filters = subcategoryCounts[categoryId] || {};
      subcategoryCounts[categoryId] = filters;
      const filterValue = hydrated.predicted_subcategory_filter;
      filters[filterValue] = filters[filterValue] || {
        value: filterValue,
        label: hydrated.predicted_subcategory_label,
        count: 0
      };
      filters[filterValue].count += 1;
    }
  }
  const categories = [...configuredCategories, ...Object.values(dynamicCategories)].map((category) => ({
    ...category,
    count: categoryCounts[String(category.id)] || 0
  }));
  const subcategoryFilters = {};
  for (const [categoryId, filters] of Object.entries(subcategoryCounts)) {
    subcategoryFilters[categoryId] = Object.values(filters).sort((a, b) => a.label.localeCompare(b.label));
  }
  return {
    index_version: INDEX_VERSION,
    job_type: jobType,
    job_type_label: jobTypeLabel(jobType),
    total: detections.length,
    missing_crop_count: 0,
    categories,
    subcategory_filters: subcategoryFilters,
    raw_label_counts: rawLabelCounts
  };
}

function summaryWithCropStatus(summary, status) {
  return {
    ...summary,
    crop_cache_version: CROP_CACHE_VERSION,
    missing_crop_count: cropMissingCount(status, summary.total),
    crop_generation: publicCropStatus(status, summary.total)
  };
}

async function ensureCropGenerationStarted(jobId, index) {
  const existing = await getCropStatus(jobId);
  if (existing?.status === "complete") {
    return existing;
  }
  if (existing?.status === "running" && !isCropStatusStale(existing)) {
    return existing;
  }
  return startCropGeneration(jobId, index, false);
}

async function startCropGeneration(jobId, index, force) {
  const existing = await getCropStatus(jobId);
  if (!force && existing?.status === "complete") {
    return existing;
  }
  if (!force && existing?.status === "running" && !isCropStatusStale(existing)) {
    return existing;
  }
  const manifest = await getCropManifest(jobId, index);
  const now = new Date().toISOString();
  const startGroupIndex = existing?.version === CROP_CACHE_VERSION
    ? clampInt(existing.next_group_index, 0, 0, manifest.groups.length)
    : 0;
  const initialGenerated = existing?.version === CROP_CACHE_VERSION
    ? clampInt(existing.generated, 0, 0, index.summary.total)
    : 0;
  const shards = cropWorkerShards(startGroupIndex, manifest.groups.length);
  const status = {
    status: "running",
    run_id: crypto.randomUUID(),
    version: CROP_CACHE_VERSION,
    total: index.summary.total,
    total_groups: manifest.groups.length,
    total_shards: shards.length,
    completed_shards: 0,
    generated: initialGenerated,
    next_group_index: startGroupIndex,
    started_at: now,
    updated_at: now,
    message: shards.length
      ? `Queued ${shards.length} crop worker shards.`
      : "All crop groups have already been queued."
  };
  await putCropStatus(jobId, status);
  await enqueueCropWorkerShards(jobId, status.run_id, shards);
  return status;
}

async function handleCropWorker(event) {
  const jobId = event.job_id;
  const runId = event.run_id;
  let groupIndex = Number(event.start_group_index ?? event.group_index ?? 0);
  const endGroupIndex = Number(event.end_group_index ?? groupIndex + CROP_SHARD_GROUPS);
  if (!jobId || !runId) {
    throw new Error("Crop worker requires job_id and run_id.");
  }
  const status = await getCropStatus(jobId);
  if (!status || status.run_id !== runId || status.status !== "running") {
    return { ignored: true, reason: "stale crop worker event" };
  }
  const started = Date.now();
  const index = await getJobIndex(jobId);
  const manifest = await getCropManifest(jobId, index);
  let generated = Number(status.generated || 0);
  let groupsDone = 0;
  let detectionsDone = 0;
  try {
    while (groupIndex < Math.min(endGroupIndex, manifest.groups.length)) {
      const group = manifest.groups[groupIndex];
      const records = index.detections
        .slice(group.start, group.end)
        .map(hydrateDetection);
      await writeCropsForSourceGroup(records);
      generated += records.length;
      groupsDone += 1;
      detectionsDone += records.length;
      groupIndex += 1;
      if (
        groupsDone >= CROP_WORKER_MAX_GROUPS ||
        detectionsDone >= CROP_WORKER_MAX_DETECTIONS ||
        Date.now() - started >= CROP_WORKER_MAX_MILLIS
      ) {
        break;
      }
    }
    return await addCropProgress(jobId, runId, detectionsDone, groupIndex, index.summary.total);
  } catch (error) {
    console.error(error);
    const failed = {
      ...status,
      status: "failed",
      generated,
      next_group_index: groupIndex,
      updated_at: new Date().toISOString(),
      message: error.message || "Crop generation failed."
    };
    await putCropStatus(jobId, failed);
    throw error;
  }
}

async function writeCropsForSourceGroup(detections) {
  if (!detections.length) {
    return;
  }
  const sourceBuffer = await getSourceImageBuffer(detections[0]);
  const decoded = await sharp(sourceBuffer)
    .raw()
    .toBuffer({ resolveWithObject: true });
  await mapWithConcurrency(detections, CROP_UPLOAD_CONCURRENCY, async (detection) => {
    if (!Array.isArray(detection.bbox) || detection.bbox.length !== 4) {
      return;
    }
    const region = cropRegionFromYxBox(detection.bbox, decoded.info.width, decoded.info.height);
    const crop = await sharp(decoded.data, {
      raw: {
        width: decoded.info.width,
        height: decoded.info.height,
        channels: decoded.info.channels
      }
    })
      .extract(region)
      .jpeg({ quality: 90 })
      .toBuffer();
    await s3.send(new PutObjectCommand({
      Bucket: DATA_BUCKET,
      Key: detection.crop_key,
      Body: crop,
      ContentType: "image/jpeg",
      CacheControl: "public, max-age=31536000, immutable"
    }));
  });
}

async function getCropManifest(jobId, index) {
  try {
    const response = await s3.send(new GetObjectCommand({ Bucket: DATA_BUCKET, Key: cropManifestKey(jobId) }));
    return JSON.parse(await streamToString(response.Body));
  } catch (error) {
    // Build below.
  }
  const groups = [];
  let current = null;
  for (let i = 0; i < index.detections.length; i += 1) {
    const detection = index.detections[i];
    const groupKey = `${detection.source_bucket || DATA_BUCKET}\n${detection.source_key}`;
    if (!current || current.group_key !== groupKey) {
      if (current) {
        current.end = i;
        current.count = current.end - current.start;
        groups.push(stripGroupKey(current));
      }
      current = {
        group_key: groupKey,
        source_bucket: detection.source_bucket || DATA_BUCKET,
        source_key: detection.source_key,
        start: i
      };
    }
  }
  if (current) {
    current.end = index.detections.length;
    current.count = current.end - current.start;
    groups.push(stripGroupKey(current));
  }
  const manifest = {
    version: CROP_CACHE_VERSION,
    job_id: jobId,
    generated_at: new Date().toISOString(),
    total_detections: index.summary.total,
    total_groups: groups.length,
    groups
  };
  await s3.send(new PutObjectCommand({
    Bucket: DATA_BUCKET,
    Key: cropManifestKey(jobId),
    Body: JSON.stringify(manifest),
    ContentType: "application/json"
  }));
  return manifest;
}

function stripGroupKey(group) {
  const { group_key, ...rest } = group;
  return rest;
}

function cropWorkerShards(startGroupIndex, totalGroups) {
  const shards = [];
  const shardSize = Math.max(1, CROP_SHARD_GROUPS);
  for (let start = startGroupIndex; start < totalGroups; start += shardSize) {
    shards.push({
      start_group_index: start,
      end_group_index: Math.min(start + shardSize, totalGroups)
    });
  }
  return shards;
}

async function enqueueCropWorkerShards(jobId, runId, shards) {
  if (!CROP_QUEUE_URL && shards.length) {
    throw httpError(500, "Crop worker queue is not configured.", "WORKER_QUEUE_NOT_CONFIGURED");
  }
  for (const chunk of chunks(shards, 10)) {
    await sqs.send(new SendMessageBatchCommand({
      QueueUrl: CROP_QUEUE_URL,
      Entries: chunk.map((shard, index) => ({
        Id: `${shard.start_group_index}-${index}`,
        MessageBody: JSON.stringify({
          worker: "crop-generation",
          job_id: jobId,
          run_id: runId,
          ...shard
        })
      }))
    }));
  }
}

async function getCropStatus(jobId) {
  const response = await dynamo.send(new GetItemCommand({
    TableName: PROGRESS_TABLE,
    Key: { pk: { S: jobPk(jobId) }, sk: { S: cropStatusSk() } }
  }));
  if (!response.Item?.status_json?.S) {
    return null;
  }
  const status = JSON.parse(response.Item.status_json.S);
  const generated = numberAttr(response.Item.generated_count, status.generated || 0);
  const completedShards = numberAttr(response.Item.completed_shards, status.completed_shards || 0);
  const totalShards = numberAttr(response.Item.total_shards, status.total_shards || 0);
  const nextGroupIndex = numberAttr(response.Item.next_group_index, status.next_group_index || 0);
  return {
    ...status,
    generated,
    completed_shards: completedShards,
    total_shards: totalShards,
    next_group_index: nextGroupIndex,
    updated_at: response.Item.updated_at?.S || status.updated_at
  };
}

async function putCropStatus(jobId, status) {
  await dynamo.send(new PutItemCommand({
    TableName: PROGRESS_TABLE,
    Item: {
      pk: { S: jobPk(jobId) },
      sk: { S: cropStatusSk() },
      status_json: { S: JSON.stringify(status) },
      run_id: { S: status.run_id || "" },
      status: { S: status.status },
      generated_count: { N: String(status.generated || 0) },
      completed_shards: { N: String(status.completed_shards || 0) },
      total_shards: { N: String(status.total_shards || 0) },
      next_group_index: { N: String(status.next_group_index || 0) },
      updated_at: { S: status.updated_at || new Date().toISOString() }
    }
  }));
}

async function addCropProgress(jobId, runId, generatedDelta, nextGroupIndex, total) {
  const now = new Date().toISOString();
  const response = await dynamo.send(new UpdateItemCommand({
    TableName: PROGRESS_TABLE,
    Key: { pk: { S: jobPk(jobId) }, sk: { S: cropStatusSk() } },
    ConditionExpression: "run_id = :run_id OR attribute_not_exists(run_id)",
    UpdateExpression: "ADD generated_count :generated, completed_shards :one SET updated_at = :updated_at, next_group_index = :next_group_index",
    ExpressionAttributeValues: {
      ":run_id": { S: runId },
      ":generated": { N: String(generatedDelta || 0) },
      ":one": { N: "1" },
      ":updated_at": { S: now },
      ":next_group_index": { N: String(nextGroupIndex || 0) }
    },
    ReturnValues: "ALL_NEW"
  }));
  const status = response.Attributes?.status_json?.S ? JSON.parse(response.Attributes.status_json.S) : {};
  const generated = numberAttr(response.Attributes?.generated_count, status.generated || 0);
  const completedShards = numberAttr(response.Attributes?.completed_shards, status.completed_shards || 0);
  const totalShards = numberAttr(response.Attributes?.total_shards, status.total_shards || 0);
  if (totalShards && completedShards >= totalShards) {
    const complete = {
      ...status,
      status: "complete",
      generated: total,
      completed_shards: completedShards,
      total_shards: totalShards,
      next_group_index: status.total_groups || nextGroupIndex,
      updated_at: now,
      completed_at: now,
      message: "All crops generated."
    };
    await putCropStatus(jobId, complete);
    return complete;
  }
  const running = {
    ...status,
    generated,
    completed_shards: completedShards,
    total_shards: totalShards,
    next_group_index: nextGroupIndex,
    updated_at: now,
    message: `Generated ${generated} of ${total} crops.`
  };
  await dynamo.send(new UpdateItemCommand({
    TableName: PROGRESS_TABLE,
    Key: { pk: { S: jobPk(jobId) }, sk: { S: cropStatusSk() } },
    UpdateExpression: "SET status_json = :status_json",
    ExpressionAttributeValues: {
      ":status_json": { S: JSON.stringify(running) }
    }
  }));
  return running;
}

function publicCropStatus(status, total) {
  if (!status) {
    return {
      status: "not_started",
      version: CROP_CACHE_VERSION,
      total,
      generated: 0,
      missing: total
    };
  }
  const generated = Math.min(Number(status.generated || 0), total);
  return {
    status: status.status,
    version: CROP_CACHE_VERSION,
    total,
    generated,
    missing: cropMissingCount(status, total),
    total_groups: Number(status.total_groups || 0),
    total_shards: Number(status.total_shards || 0),
    completed_shards: Number(status.completed_shards || 0),
    next_group_index: Number(status.next_group_index || 0),
    updated_at: status.updated_at || null,
    completed_at: status.completed_at || null,
    message: status.message || ""
  };
}

function cropMissingCount(status, total) {
  if (status?.status === "complete") {
    return 0;
  }
  return Math.max(0, Number(total || 0) - Number(status?.generated || 0));
}

function isCropStatusStale(status) {
  if (status?.status !== "running" || !status.updated_at) {
    return false;
  }
  return Date.now() - Date.parse(status.updated_at) > 10 * 60 * 1000;
}

async function handleImage(event) {
  const query = event.queryStringParameters || {};
  const jobId = requiredQuery(event, "job_id");
  const id = requiredQuery(event, "id");
  const expires = requiredQuery(event, "expires");
  const sig = requiredQuery(event, "sig");
  if (!isValidImageSignature(jobId, id, expires, sig)) {
    return json(403, { error: "Image URL expired or invalid.", code: "INVALID_IMAGE_URL" });
  }
  const index = await getJobIndex(jobId);
  const detection = hydrateDetection(index.byId[id]);
  if (!detection) {
    return json(404, { error: "Detection not found.", code: "DETECTION_NOT_FOUND", job_id: jobId, detection_id: id });
  }
  try {
    await s3.send(new HeadObjectCommand({ Bucket: DATA_BUCKET, Key: detection.crop_key }));
  } catch (error) {
    return json(404, {
      error: "Crop is not generated yet. Open the job and wait for whole-job crop generation to finish.",
      code: "CROP_NOT_READY",
      job_id: jobId,
      detection_id: id
    });
  }
  return {
    statusCode: 302,
    headers: {
      "Location": presignedS3GetUrl(DATA_BUCKET, detection.crop_key, S3_IMAGE_URL_SECONDS),
      "Cache-Control": "private, max-age=300",
      ...corsHeaders()
    },
    body: ""
  };
}

async function getSourceImageBuffer(detection) {
  const cacheKey = `${detection.source_bucket}/${detection.source_key}`;
  if (sourceImageCache.has(cacheKey)) {
    return sourceImageCache.get(cacheKey);
  }
  let sourceObject;
  try {
    sourceObject = await s3.send(new GetObjectCommand({
      Bucket: detection.source_bucket,
      Key: detection.source_key
    }));
  } catch (error) {
    throw httpError(404, "Source image was not found.", "MISSING_SOURCE_IMAGE", {
      detection_id: detection.id,
      detail: `s3://${detection.source_bucket}/${detection.source_key}`
    });
  }
  const sourceBuffer = await streamToBuffer(sourceObject.Body);
  sourceImageCache.set(cacheKey, sourceBuffer);
  while (sourceImageCache.size > 2) {
    sourceImageCache.delete(sourceImageCache.keys().next().value);
  }
  return sourceBuffer;
}

function withImageUrl(event, detection) {
  const hydrated = hydrateDetection(detection);
  const expires = String(Math.floor(Date.now() / 1000) + IMAGE_URL_SECONDS);
  const sig = imageSignature(hydrated.job_id, hydrated.id, expires);
  return {
    ...hydrated,
    image_url: `${apiBaseUrl(event)}/api/image?job_id=${encodeURIComponent(hydrated.job_id)}&id=${encodeURIComponent(hydrated.id)}&expires=${expires}&sig=${sig}`
  };
}

function hydrateDetection(detection) {
  if (!detection) {
    return null;
  }
  const category = categoryForStoredDetection(detection);
  const rawLabelDisplay = sourceLabelOverrides[detection.raw_label]?.label || humanizeLabel(detection.raw_label);
  const [subcategoryFilter, subcategoryLabel] = detection.predicted_subcategory_filter
    ? [
        detection.predicted_subcategory_filter,
        detection.predicted_subcategory_label || labelForSubcategoryFilter(detection.predicted_subcategory_filter, detection.predicted_subcategory_name)
      ]
    : subcategoryFilterFor(sourceLabelOverrides[detection.raw_label] || {}, detection.predicted_subcategory);
  return {
    ...detection,
    job_label: detection.job_label || detection.job_id,
    source_image: detection.source_image || `s3://${detection.source_bucket || DATA_BUCKET}/${detection.source_key}`,
    raw_label_display: detection.raw_label_display || rawLabelDisplay,
    predicted_category_key: detection.predicted_category_key || category.key,
    predicted_category_label: detection.predicted_category_label || category.label,
    predicted_category_requires_subcategory:
      detection.predicted_category_requires_subcategory !== undefined
        ? detection.predicted_category_requires_subcategory
        : category.requires_subcategory !== false,
    predicted_subcategory_filter: subcategoryFilter,
    predicted_subcategory_label: subcategoryLabel,
    crop_key: `${DETECTIONS_PREFIX}${detection.job_id}/review/crops-${CROP_CACHE_VERSION}/${detection.detection_id}.jpg`,
    image_missing: Boolean(detection.image_missing)
  };
}

function categoryForStoredDetection(detection) {
  const configured = categoriesById.get(String(detection.predicted_category_id))
    || categoriesByKey.get(detection.predicted_category_key);
  if (configured) {
    return configured;
  }
  return {
    id: detection.predicted_category_id,
    key: detection.predicted_category_key || detection.raw_label,
    label: detection.predicted_category_label || humanizeLabel(detection.predicted_category_key || detection.raw_label),
    requires_subcategory: Boolean(detection.predicted_category_requires_subcategory),
    dynamic: true
  };
}

function labelForSubcategoryFilter(filterValue, predictedSubcategory) {
  if (filterValue === NO_SUBCATEGORY) {
    return "No predicted subcategory";
  }
  if (filterValue && filterValue.startsWith("id:")) {
    const id = filterValue.slice(3);
    return subcategoriesById.get(id)?.name || id;
  }
  if (filterValue && filterValue.startsWith("name:")) {
    return sentenceCase(filterValue.slice(5).replace(/_/g, " "));
  }
  return predictedSubcategory ? sentenceCase(predictedSubcategory.replace(/_/g, " ")) : "No predicted subcategory";
}

function normalizeCategorySets(rawConfig) {
  const sets = {};
  for (const [key, set] of Object.entries(rawConfig.category_sets || {})) {
    sets[key] = {
      id: set.id || key,
      label: set.label || humanizeLabel(key),
      categories: (set.categories || []).filter((item) => !item.is_false_detection).map(normalizeCategory)
    };
  }
  if (!sets.birds && Array.isArray(rawConfig.categories)) {
    sets.birds = {
      id: "birds",
      label: "Bird detections",
      categories: rawConfig.categories.filter((item) => !item.is_false_detection).map(normalizeCategory)
    };
  }
  return sets;
}

function normalizeCategory(category) {
  const key = String(category.key ?? category.id);
  return {
    ...category,
    id: category.id ?? key,
    key,
    label: category.label || humanizeLabel(key),
    shortcut: category.shortcut || "",
    requires_subcategory: category.requires_subcategory !== false
  };
}

function configuredCategoriesList(sets, falseCategory) {
  const seen = new Set();
  const categories = [];
  for (const set of Object.values(sets)) {
    for (const category of set.categories || []) {
      const id = String(category.id);
      if (!seen.has(id)) {
        seen.add(id);
        categories.push(category);
      }
    }
  }
  const falseId = String(falseCategory.id);
  if (!seen.has(falseId)) {
    categories.push(falseCategory);
  }
  return categories;
}

function categoriesForJobType(jobType) {
  const set = configuredCategorySets[jobType];
  const categories = set ? set.categories : [];
  return [...categories, falseDetectionCategory];
}

function categoryLookupForJobType(jobType) {
  const key = jobType || "custom";
  if (!categoryLookupCache.has(key)) {
    const categories = categoriesForJobType(key);
    categoryLookupCache.set(key, {
      byKey: new Map(categories.map((category) => [category.key, category])),
      byId: new Map(categories.map((category) => [String(category.id), category]))
    });
  }
  return categoryLookupCache.get(key);
}

function categoryKeysForJobType(jobType) {
  return new Set(categoriesForJobType(jobType)
    .filter((category) => !category.is_false_detection)
    .map((category) => category.key));
}

function categoryIdsForJobType(jobType) {
  return new Set(categoriesForJobType(jobType)
    .filter((category) => !category.is_false_detection)
    .map((category) => String(category.id)));
}

function detectJobType(parsedRows) {
  const birdKeys = categoryKeysForJobType("birds");
  const birdIds = categoryIdsForJobType("birds");
  const rubbishKeys = categoryKeysForJobType("rubbish");
  let birdMatches = 0;
  let rubbishMatches = 0;
  let hasPredictedSubcategory = false;
  for (const row of parsedRows) {
    if (row.predicted_subcategory) {
      hasPredictedSubcategory = true;
    }
    if (birdKeys.has(row.raw_label)) {
      birdMatches += 1;
    }
    if (rubbishKeys.has(row.raw_label)) {
      rubbishMatches += 1;
    }
    const override = sourceLabelOverrides[row.raw_label];
    if (override?.category_id !== undefined && birdIds.has(String(override.category_id))) {
      birdMatches += 1;
    }
  }
  if (hasPredictedSubcategory || birdMatches > 0) {
    return "birds";
  }
  if (rubbishMatches > 0) {
    return "rubbish";
  }
  return "custom";
}

function jobTypeLabel(jobType) {
  if (!jobType || jobType === "custom") {
    return "Custom detections";
  }
  return configuredCategorySets[jobType]?.label || humanizeLabel(jobType);
}

function presignedS3GetUrl(bucket, key, expiresSeconds) {
  const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
  const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) {
    throw httpError(500, "Lambda execution credentials were not available for S3 image signing.", "S3_SIGNING_UNAVAILABLE");
  }
  const sessionToken = process.env.AWS_SESSION_TOKEN;
  const now = new Date();
  const amzDate = toAmzDate(now);
  const dateStamp = amzDate.slice(0, 8);
  const service = "s3";
  const credentialScope = `${dateStamp}/${REGION}/${service}/aws4_request`;
  const host = `${bucket}.s3.${REGION}.amazonaws.com`;
  const canonicalUri = `/${encodeS3Key(key)}`;
  const query = {
    "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
    "X-Amz-Credential": `${accessKeyId}/${credentialScope}`,
    "X-Amz-Date": amzDate,
    "X-Amz-Expires": String(Math.min(Math.max(Number(expiresSeconds) || 300, 1), 604800)),
    "X-Amz-SignedHeaders": "host"
  };
  if (sessionToken) {
    query["X-Amz-Security-Token"] = sessionToken;
  }
  const canonicalQuery = canonicalQueryString(query);
  const canonicalRequest = [
    "GET",
    canonicalUri,
    canonicalQuery,
    `host:${host}\n`,
    "host",
    "UNSIGNED-PAYLOAD"
  ].join("\n");
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    credentialScope,
    crypto.createHash("sha256").update(canonicalRequest, "utf8").digest("hex")
  ].join("\n");
  const signature = hmac(signingKey(secretAccessKey, dateStamp, REGION, service), stringToSign, "hex");
  return `https://${host}${canonicalUri}?${canonicalQuery}&X-Amz-Signature=${signature}`;
}

function imageSignature(jobId, id, expires) {
  return crypto
    .createHmac("sha256", IMAGE_URL_SECRET)
    .update(`${jobId}\n${id}\n${expires}`)
    .digest("hex");
}

function isValidImageSignature(jobId, id, expires, sig) {
  if (Number(expires) < Math.floor(Date.now() / 1000)) {
    return false;
  }
  const expected = imageSignature(jobId, id, expires);
  const expectedBuffer = Buffer.from(expected);
  const suppliedBuffer = Buffer.from(sig || "");
  if (expectedBuffer.length !== suppliedBuffer.length) {
    return false;
  }
  return crypto.timingSafeEqual(expectedBuffer, suppliedBuffer);
}

function parseDetectionLine(line) {
  const row = line.split(",").map((item) => item.trim());
  if (row.length < 8) {
    return null;
  }
  let predictedSubcategory = null;
  let confidenceIndex;
  let bboxValues;
  if (row.length >= 9 && !isFiniteNumber(row[3]) && isFiniteNumber(row[4])) {
    predictedSubcategory = row[3] || null;
    confidenceIndex = 4;
    bboxValues = row.slice(-4);
  } else if (isFiniteNumber(row[3])) {
    confidenceIndex = 3;
    bboxValues = row.slice(4, 8);
  } else {
    return null;
  }
  const bbox = bboxValues.map((value) => Number(value));
  if (bbox.some((value) => !Number.isFinite(value))) {
    return null;
  }
  return {
    source_path: row[0],
    detection_id: row[1],
    raw_label: row[2],
    predicted_subcategory: predictedSubcategory,
    confidence: Number(row[confidenceIndex]),
    bbox: bbox.map((value) => Math.round(value))
  };
}

function categoryForLabel(rawLabel, hasPredictedSubcategory, jobType = "custom") {
  const override = sourceLabelOverrides[rawLabel] || {};
  if (override.category_id !== undefined) {
    const category = categoriesById.get(String(override.category_id));
    if (category) {
      return { item: category, override };
    }
  }
  const configured = categoryLookupForJobType(jobType).byKey.get(rawLabel) || categoriesByKey.get(rawLabel);
  if (configured) {
    return { item: configured, override: {} };
  }
  return {
    item: {
      id: rawLabel,
      key: rawLabel,
      label: humanizeLabel(rawLabel),
      shortcut: "",
      requires_subcategory: hasPredictedSubcategory,
      dynamic: true
    },
    override: {}
  };
}

function subcategoryFilterFor(override, predictedSubcategory) {
  if (override.predicted_subcategory_key) {
    return [
      String(override.predicted_subcategory_key),
      override.predicted_subcategory_label || humanizeLabel(override.predicted_subcategory_key)
    ];
  }
  if (override.predicted_subcategory_id !== undefined) {
    const subcategoryId = String(override.predicted_subcategory_id);
    const subcategory = subcategoriesById.get(subcategoryId);
    return [
      `id:${subcategoryId}`,
      override.predicted_subcategory_label || subcategory?.name || subcategoryId
    ];
  }
  if (predictedSubcategory) {
    const label = predictedSubcategory.replace(/_/g, " ").trim();
    return [`name:${label.toLowerCase()}`, sentenceCase(label)];
  }
  return [NO_SUBCATEGORY, "No predicted subcategory"];
}

function parseS3Url(value) {
  const match = /^s3:\/\/([^/]+)\/(.+)$/.exec(value || "");
  const key = match ? match[2] : value;
  const parts = (key || "").split("/");
  return {
    bucket: match ? match[1] : null,
    key,
    site: parts.length >= 3 ? parts[2] : null
  };
}

function jobIdFromDetectionId(id) {
  const index = String(id).indexOf("/");
  if (index <= 0) {
    throw httpError(400, "Detection id must include the job id prefix.", "INVALID_DETECTION_ID", { detection_id: id });
  }
  return id.slice(0, index);
}

function requiredQuery(event, name) {
  const value = event.queryStringParameters?.[name];
  if (!value) {
    throw httpError(400, `Missing query parameter: ${name}`, "MISSING_QUERY_PARAMETER");
  }
  return value;
}

function requireBody(body, name) {
  if (!body[name]) {
    throw httpError(400, `Missing request field: ${name}`, "MISSING_REQUEST_FIELD");
  }
  return body[name];
}

function parseBody(event) {
  if (!event.body) {
    return {};
  }
  const text = event.isBase64Encoded
    ? Buffer.from(event.body, "base64").toString("utf8")
    : event.body;
  return JSON.parse(text);
}

function isAuthorized(event) {
  if (!ACCESS_KEY) {
    return true;
  }
  const headers = event.headers || {};
  const supplied = headers["x-prediction-review-key"] || headers["X-Prediction-Review-Key"];
  return supplied === ACCESS_KEY;
}

function apiBaseUrl(event) {
  const headers = event.headers || {};
  const proto = headers["x-forwarded-proto"] || "https";
  const host = headers.host || headers.Host;
  return `${proto}://${host}`;
}

function json(statusCode, payload) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
      ...corsHeaders()
    },
    body: JSON.stringify(payload)
  };
}

function empty(statusCode) {
  return { statusCode, headers: corsHeaders(), body: "" };
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": CORS_ORIGIN,
    "Access-Control-Allow-Headers": "content-type,x-prediction-review-key",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
  };
}

function httpError(statusCode, publicMessage, code, detail) {
  const error = new Error(publicMessage);
  error.statusCode = statusCode;
  error.publicMessage = publicMessage;
  error.code = code;
  error.detail = detail;
  return error;
}

function indexKey(jobId) {
  return `${DETECTIONS_PREFIX}${jobId}/review/index.json`;
}

function summaryKey(jobId) {
  return `${DETECTIONS_PREFIX}${jobId}/review/summary.json`;
}

function cropManifestKey(jobId) {
  return `${DETECTIONS_PREFIX}${jobId}/review/crop-manifest-${CROP_CACHE_VERSION}.json`;
}

function cropStatusSk() {
  return `CROPS#${CROP_CACHE_VERSION}`;
}

function jobPk(jobId) {
  return `JOB#${jobId}`;
}

function normalizePrefix(prefix) {
  return prefix.endsWith("/") ? prefix : `${prefix}/`;
}

function isFiniteNumber(value) {
  return Number.isFinite(Number(value));
}

function clampInt(value, fallback, min, max) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return clamp(parsed, min, max);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function numberAttr(attribute, fallback = 0) {
  if (!attribute?.N) {
    return fallback;
  }
  const value = Number(attribute.N);
  return Number.isFinite(value) ? value : fallback;
}

function cropRegionFromYxBox(bbox, width, height) {
  const [y1, x1, y2, x2] = bbox.map((value) => Number(value));
  const left = clamp(Math.floor(Math.min(x1, x2)), 0, Math.max((width || 1) - 1, 0));
  const top = clamp(Math.floor(Math.min(y1, y2)), 0, Math.max((height || 1) - 1, 0));
  const right = clamp(Math.ceil(Math.max(x1, x2)), left + 1, width || left + 1);
  const bottom = clamp(Math.ceil(Math.max(y1, y2)), top + 1, height || top + 1);
  return { left, top, width: right - left, height: bottom - top };
}

function humanizeLabel(raw) {
  return String(raw || "")
    .replace(/^birds?_/, "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function sentenceCase(value) {
  return value ? value.slice(0, 1).toUpperCase() + value.slice(1) : value;
}

function toAmzDate(date) {
  return date.toISOString().replace(/[:-]|\.\d{3}/g, "");
}

function canonicalQueryString(query) {
  return Object.keys(query)
    .sort()
    .map((key) => `${awsEncode(key)}=${awsEncode(query[key])}`)
    .join("&");
}

function encodeS3Key(key) {
  return String(key).split("/").map(awsEncode).join("/");
}

function awsEncode(value) {
  return encodeURIComponent(String(value)).replace(/[!'()*]/g, (char) =>
    `%${char.charCodeAt(0).toString(16).toUpperCase()}`
  );
}

function signingKey(secretAccessKey, dateStamp, region, service) {
  const dateKey = hmac(`AWS4${secretAccessKey}`, dateStamp);
  const regionKey = hmac(dateKey, region);
  const serviceKey = hmac(regionKey, service);
  return hmac(serviceKey, "aws4_request");
}

function hmac(key, value, encoding = undefined) {
  return crypto.createHmac("sha256", key).update(value, "utf8").digest(encoding);
}

async function mapWithConcurrency(items, limit, fn) {
  const workerCount = Math.max(1, Math.min(Number(limit) || 1, items.length));
  let next = 0;
  const workers = Array.from({ length: workerCount }, async () => {
    while (next < items.length) {
      const index = next;
      next += 1;
      await fn(items[index], index);
    }
  });
  await Promise.all(workers);
}

function chunks(items, size) {
  const result = [];
  for (let i = 0; i < items.length; i += size) {
    result.push(items.slice(i, i + size));
  }
  return result;
}

async function streamToString(stream) {
  const buffer = await streamToBuffer(stream);
  return buffer.toString("utf8");
}

async function streamToBuffer(stream) {
  if (Buffer.isBuffer(stream)) {
    return stream;
  }
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}
