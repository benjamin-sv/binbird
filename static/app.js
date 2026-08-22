const PAGE_SIZE = 120;
const SUBCATEGORY_KEYS = "QWERTYUIOPASDFGHJKLZXCVBNM".split("");
const FALLBACK_CATEGORY_KEYS = "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("");

const state = {
  config: null,
  jobs: [],
  accessKey: window.localStorage.getItem("predictionReviewAccessKey") || "",
  activeJob: null,
  summary: null,
  cropStatus: null,
  cropPollTimer: null,
  progress: null,
  selectedWrong: new Set(),
  reviews: {},
  activePhase: "jobs",
  activeCategoryId: null,
  activeSubcategoryFilter: "",
  detectionsLoaded: 0,
  detectionsTotal: 0,
  loadingDetections: false,
  finishedLoading: false,
  queue: [],
  queueIndex: 0,
  currentDetection: null,
  currentReviewCategory: null,
  currentReviewSubcategory: null,
  categoryShortcutMap: new Map(),
  subcategoryShortcutMap: new Map()
};

const els = {};

function cacheElements() {
  [
    "appTitle",
    "datasetMeta",
    "jobsTab",
    "phaseOneTab",
    "phaseTwoTab",
    "errorBanner",
    "jobScreen",
    "jobCount",
    "jobList",
    "accessKeyInput",
    "accessKeyStatus",
    "categoryCount",
    "categoryList",
    "subcategoryFilter",
    "selectAllButton",
    "clearAllButton",
    "startReviewButton",
    "filterSummary",
    "saveStatus",
    "thumbGrid",
    "gridSentinel",
    "phase1",
    "phase2",
    "previousButton",
    "undoButton",
    "queueStatus",
    "largeImage",
    "largeImageMessage",
    "currentMeta",
    "categoryButtons",
    "subcategoryBlock",
    "subcategoryButtons",
    "exportButton"
  ].forEach((id) => {
    els[id] = document.getElementById(id);
  });
}

async function api(path, options = {}) {
  const apiBase = (window.PREDICTION_REVIEW_API_BASE || "").replace(/\/+$/, "");
  const response = await fetch(`${apiBase}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(state.accessKey ? { "x-prediction-review-key": state.accessKey } : {}),
      ...(options.headers || {})
    },
    ...options
  });
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}));
    if (response.status === 401 || response.status === 403) {
      showError("Access key required or invalid.");
    }
    throw new Error(detail.error || `Request failed: ${response.status}`);
  }
  return response.json();
}

function fmtCount(value) {
  return new Intl.NumberFormat().format(value || 0);
}

function categoryId(value) {
  return String(value);
}

function configuredCategories() {
  return state.config?.categories || [];
}

function summaryCategories() {
  return state.summary?.categories || [];
}

function reviewCategories() {
  const source = summaryCategories().length ? summaryCategories() : configuredCategories();
  const observed = source.filter((category) => category.count > 0 || category.is_false_detection);
  const hasDynamic = observed.some((category) => category.dynamic);
  const categories = hasDynamic ? observed : source;
  const seen = new Set();
  return categories.filter((category) => {
    const id = categoryId(category.id);
    if (seen.has(id)) {
      return false;
    }
    seen.add(id);
    return true;
  });
}

function configuredSubcategoriesForCategory(id) {
  const categoryKey = categoryId(id);
  return (state.config?.subcategories || []).filter((item) => {
    const itemCategory = categoryId(item.category_id);
    return itemCategory === "-1" || itemCategory === categoryKey;
  });
}

function subcategoriesForCategory(id) {
  const categoryKey = categoryId(id);
  const configured = configuredSubcategoriesForCategory(id);
  const seen = new Set(configured.map((item) => item.name.toLowerCase()));
  const inferred = (state.summary?.subcategory_filters?.[categoryKey] || [])
    .filter((item) => item.value !== "__none__" && !seen.has(item.label.toLowerCase()))
    .map((item) => ({
      id: `inferred:${item.value}`,
      name: item.label,
      category_id: id,
      inferred: true
    }));
  return [...configured, ...inferred];
}

function jobQuery(params = {}) {
  const query = new URLSearchParams(params);
  if (state.activeJob) {
    query.set("job_id", state.activeJob.id);
  }
  return query;
}

function setSaveStatus(text) {
  els.saveStatus.textContent = text;
}

function showError(message) {
  els.errorBanner.textContent = message;
  els.errorBanner.classList.remove("is-hidden");
}

function clearError() {
  els.errorBanner.textContent = "";
  els.errorBanner.classList.add("is-hidden");
}

function syncProgress(progress) {
  state.progress = progress;
  state.selectedWrong = new Set(progress?.selected_wrong || []);
  state.reviews = progress?.reviews || {};
  updateSelectionText();
  renderUndoState();
  updateLoadedTiles();
}

function syncCropStatus(status) {
  state.cropStatus = status || null;
  renderCropStatus();
  queueCropStatusPoll();
}

function renderCropStatus() {
  const status = state.cropStatus;
  if (!status || !state.activeJob) {
    return;
  }
  if (status.status === "running") {
    setSaveStatus(`Generating crops ${fmtCount(status.generated)} / ${fmtCount(status.total)}`);
  } else if (status.status === "complete") {
    setSaveStatus("Crops ready");
  } else if (status.status === "failed") {
    setSaveStatus(status.message || "Crop generation failed");
  } else if (status.missing) {
    setSaveStatus(`${fmtCount(status.missing)} crops queued`);
  }
}

function queueCropStatusPoll() {
  if (state.cropPollTimer) {
    window.clearTimeout(state.cropPollTimer);
    state.cropPollTimer = null;
  }
  if (!state.activeJob || state.cropStatus?.status !== "running") {
    return;
  }
  state.cropPollTimer = window.setTimeout(loadCropStatus, 5000);
}

async function loadCropStatus() {
  if (!state.activeJob) {
    return;
  }
  const wasRunning = state.cropStatus?.status === "running";
  try {
    const payload = await api(`/api/crops/status?${jobQuery().toString()}`);
    syncCropStatus(payload.crop_generation);
    if (wasRunning && payload.crop_generation?.status === "complete" && state.activePhase === "phase1") {
      resetDetections();
      loadMoreDetections();
    }
  } catch (error) {
    setSaveStatus(error.message);
  }
}

function renderAppChrome() {
  els.appTitle.textContent = state.config?.project_name || "Prediction Review";
  if (!state.activeJob) {
    els.datasetMeta.textContent = state.jobs.length
      ? `${fmtCount(state.jobs.length)} available jobs`
      : "Select a job";
    return;
  }
  const missing = state.summary?.missing_crop_count
    ? `, ${fmtCount(state.summary.missing_crop_count)} missing crops`
    : "";
  els.datasetMeta.textContent = `${state.activeJob.label} | ${fmtCount(state.summary?.total)} detections${missing}`;
  els.categoryCount.textContent = `${fmtCount(state.summary?.total)} total`;
}

function renderAccessKey() {
  els.accessKeyInput.value = state.accessKey;
  els.accessKeyStatus.textContent = state.accessKey
    ? "Stored in this browser"
    : "Required to load jobs";
}

function setAccessKey(value) {
  state.accessKey = (value || "").trim();
  if (state.accessKey) {
    window.localStorage.setItem("predictionReviewAccessKey", state.accessKey);
  } else {
    window.localStorage.removeItem("predictionReviewAccessKey");
  }
  renderAccessKey();
  loadBootstrap();
}

function renderPhaseTabs() {
  els.jobsTab.classList.toggle("is-active", state.activePhase === "jobs");
  els.phaseOneTab.classList.toggle("is-active", state.activePhase === "phase1");
  els.phaseTwoTab.classList.toggle("is-active", state.activePhase === "phase2");
  els.phaseOneTab.disabled = !state.activeJob;
  els.phaseTwoTab.disabled = !state.activeJob;
  els.jobScreen.classList.toggle("is-hidden", state.activePhase !== "jobs");
  els.phase1.classList.toggle("is-hidden", state.activePhase !== "phase1");
  els.phase2.classList.toggle("is-hidden", state.activePhase !== "phase2");
}

function switchPhase(phase) {
  if (phase !== "jobs" && !state.activeJob) {
    return;
  }
  state.activePhase = phase;
  clearError();
  renderPhaseTabs();
  if (phase === "phase2") {
    prepareQueue();
  }
}

function renderJobs() {
  els.jobCount.textContent = `${fmtCount(state.jobs.length)} jobs`;
  els.jobList.innerHTML = "";
  const fragment = document.createDocumentFragment();
  state.jobs.forEach((job) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "job-card";
    const warning = job.missing_crop_count
      ? `<div class="job-card-warning">${fmtCount(job.missing_crop_count)} missing crops</div>`
      : "";
    button.innerHTML = `
      <span class="job-card-title"></span>
      <span class="job-card-meta">${fmtCount(job.count)} detections</span>
      ${warning}
    `;
    button.querySelector(".job-card-title").textContent = job.label;
    button.addEventListener("click", () => loadJob(job.id));
    fragment.appendChild(button);
  });
  els.jobList.appendChild(fragment);
}

async function loadJob(jobId) {
  clearError();
  setSaveStatus("");
  const job = state.jobs.find((item) => item.id === jobId);
  if (!job) {
    showError(`Job not found: ${jobId}`);
    return;
  }
  try {
    const query = new URLSearchParams({ job_id: jobId }).toString();
    const [summary, progress] = await Promise.all([
      api(`/api/summary?${query}`),
      api(`/api/progress?${query}`)
    ]);
    state.activeJob = job;
    state.summary = summary;
    state.activeCategoryId = null;
    state.activeSubcategoryFilter = "";
    state.cropStatus = summary.crop_generation || null;
    state.queue = [];
    state.queueIndex = 0;
    state.currentDetection = null;
    state.currentReviewCategory = null;
    state.currentReviewSubcategory = null;
    syncProgress(progress);
    syncCropStatus(state.cropStatus);
    renderAppChrome();
    renderCategories();
    renderSubcategoryFilter();
    resetDetections();
    switchPhase("phase1");
    const firstCategory = summaryCategories().find((item) => item.count && categoryId(item.id) !== "0");
    if (firstCategory) {
      selectCategory(firstCategory.id);
    }
  } catch (error) {
    showError(`Could not load job "${job.label}": ${error.message}`);
  }
}

function renderCategories() {
  els.categoryList.innerHTML = "";
  const fragment = document.createDocumentFragment();
  summaryCategories().forEach((category) => {
    if (categoryId(category.id) === "0") {
      return;
    }
    const button = document.createElement("button");
    button.type = "button";
    button.className = "category-item";
    button.disabled = !category.count;
    button.dataset.categoryId = category.id;
    button.classList.toggle("is-active", categoryId(state.activeCategoryId) === categoryId(category.id));
    button.innerHTML = `
      <span class="category-name"></span>
      <span class="count-pill">${fmtCount(category.count)}</span>
    `;
    button.querySelector(".category-name").textContent = category.label;
    button.addEventListener("click", () => selectCategory(category.id));
    fragment.appendChild(button);
  });
  els.categoryList.appendChild(fragment);
}

function renderSubcategoryFilter() {
  const filters = state.summary?.subcategory_filters?.[categoryId(state.activeCategoryId)] || [];
  els.subcategoryFilter.innerHTML = "";
  const allOption = document.createElement("option");
  allOption.value = "";
  allOption.textContent = filters.length ? "All predicted subcategories" : "No subcategories";
  els.subcategoryFilter.appendChild(allOption);
  els.subcategoryFilter.disabled = !filters.length;
  filters.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = `${item.label} (${fmtCount(item.count)})`;
    els.subcategoryFilter.appendChild(option);
  });
  els.subcategoryFilter.value = state.activeSubcategoryFilter;
}

function updateSelectionText() {
  els.startReviewButton.textContent = `Review selected (${fmtCount(state.selectedWrong.size)})`;
}

function selectCategory(id) {
  state.activeCategoryId = id;
  state.activeSubcategoryFilter = "";
  renderCategories();
  renderSubcategoryFilter();
  resetDetections();
  loadMoreDetections();
}

function resetDetections() {
  state.detectionsLoaded = 0;
  state.detectionsTotal = 0;
  state.finishedLoading = false;
  els.thumbGrid.innerHTML = "";
  els.gridSentinel.textContent = state.activeCategoryId === null ? "Select a category" : "Loading";
  els.filterSummary.textContent = "";
}

function detectionQuery(offset = 0, limit = PAGE_SIZE) {
  const params = jobQuery({
    category_id: state.activeCategoryId,
    offset,
    limit
  });
  if (state.activeSubcategoryFilter) {
    params.set("subcategory", state.activeSubcategoryFilter);
  }
  return params;
}

async function loadMoreDetections() {
  if (
    state.loadingDetections ||
    state.finishedLoading ||
    state.activeCategoryId === null ||
    !state.activeJob
  ) {
    return;
  }
  state.loadingDetections = true;
  els.gridSentinel.textContent = "Loading";
  try {
    const payload = await api(`/api/detections?${detectionQuery(state.detectionsLoaded).toString()}`);
    state.detectionsTotal = payload.total;
    appendDetections(payload.detections);
    state.detectionsLoaded += payload.detections.length;
    state.finishedLoading = state.detectionsLoaded >= state.detectionsTotal;
    els.filterSummary.textContent = `${fmtCount(state.detectionsLoaded)} of ${fmtCount(state.detectionsTotal)} loaded`;
    els.gridSentinel.textContent = state.finishedLoading ? "End of filtered detections" : "Loading";
  } catch (error) {
    els.gridSentinel.textContent = error.message;
  } finally {
    state.loadingDetections = false;
  }
}

function appendDetections(detections) {
  const fragment = document.createDocumentFragment();
  detections.forEach((detection) => {
    fragment.appendChild(renderThumbCard(detection));
  });
  els.thumbGrid.appendChild(fragment);
}

function renderThumbCard(detection) {
  const card = document.createElement("article");
  card.className = "thumb-card";
  card.dataset.id = detection.id;
  card.classList.toggle("is-selected", state.selectedWrong.has(detection.id));
  card.classList.toggle("is-reviewed", Boolean(state.reviews[detection.id]));
  card.classList.toggle("is-missing", Boolean(detection.image_missing));

  const button = document.createElement("button");
  button.type = "button";
  button.className = "thumb-button";
  button.addEventListener("click", () => toggleDetection(detection.id));

  if (detection.image_url) {
    const image = document.createElement("img");
    image.src = detection.image_url;
    image.loading = "lazy";
    image.decoding = "async";
    image.alt = detection.raw_label_display;
    image.addEventListener("error", () => {
      const placeholder = document.createElement("div");
      placeholder.className = "thumb-placeholder";
      placeholder.textContent = state.cropStatus?.status === "running"
        ? "Crop generating"
        : "Crop unavailable";
      button.replaceChildren(placeholder);
    }, { once: true });
    button.appendChild(image);
  } else {
    const placeholder = document.createElement("div");
    placeholder.className = "thumb-placeholder";
    placeholder.textContent = "Crop missing";
    button.appendChild(placeholder);
  }

  const mark = document.createElement("span");
  mark.className = "selection-mark";
  mark.textContent = "Wrong";

  const meta = document.createElement("div");
  meta.className = "thumb-meta";
  const score = detection.confidence === null || detection.confidence === undefined
    ? ""
    : `Score ${Number(detection.confidence).toFixed(2)}`;
  const missing = detection.image_missing ? "Crop missing" : "";
  meta.innerHTML = `
    <strong></strong>
    <span></span>
  `;
  meta.querySelector("strong").textContent = detection.site;
  meta.querySelector("span").textContent = [detection.raw_label_display, score, missing].filter(Boolean).join(" | ");

  card.append(button, mark, meta);
  return card;
}

async function toggleDetection(id) {
  const selected = !state.selectedWrong.has(id);
  if (selected) {
    state.selectedWrong.add(id);
  } else {
    state.selectedWrong.delete(id);
  }
  updateTileState(id);
  updateSelectionText();
  try {
    const payload = await api("/api/progress/selection", {
      method: "POST",
      body: JSON.stringify({
        ids: [id],
        selected,
        job_id: state.activeJob.id,
      })
    });
    syncProgress(payload.progress);
    setSaveStatus("Saved");
  } catch (error) {
    setSaveStatus(error.message);
  }
}

function updateTileState(id) {
  const card = els.thumbGrid.querySelector(`[data-id="${CSS.escape(id)}"]`);
  if (!card) {
    return;
  }
  card.classList.toggle("is-selected", state.selectedWrong.has(id));
  card.classList.toggle("is-reviewed", Boolean(state.reviews[id]));
}

function updateLoadedTiles() {
  els.thumbGrid.querySelectorAll("[data-id]").forEach((card) => {
    updateTileState(card.dataset.id);
  });
}

async function setFilteredSelection(selected) {
  if (!state.activeJob || state.activeCategoryId === null) {
    return;
  }
  setSaveStatus("Saving");
  const params = jobQuery({ category_id: state.activeCategoryId });
  if (state.activeSubcategoryFilter) {
    params.set("subcategory", state.activeSubcategoryFilter);
  }
  try {
    const idsPayload = await api(`/api/detection_ids?${params.toString()}`);
    const result = await api("/api/progress/selection", {
      method: "POST",
      body: JSON.stringify({
        ids: idsPayload.ids,
        selected,
        job_id: state.activeJob.id,
      })
    });
    syncProgress(result.progress);
    setSaveStatus("Saved");
  } catch (error) {
    setSaveStatus(error.message);
  }
}

function prepareQueue() {
  const all = Array.from(state.selectedWrong);
  const unreviewed = all.filter((id) => !state.reviews[id]);
  state.queue = unreviewed.length ? unreviewed : all;
  state.queueIndex = Math.min(state.queueIndex, Math.max(0, state.queue.length - 1));
  state.currentReviewCategory = null;
  state.currentReviewSubcategory = null;
  renderCorrectionChoices();
  loadCurrentDetection();
}

async function loadCurrentDetection() {
  renderUndoState();
  if (!state.queue.length) {
    state.currentDetection = null;
    els.queueStatus.textContent = "No selected detections";
    els.largeImage.removeAttribute("src");
    els.largeImage.classList.add("is-hidden");
    els.largeImage.alt = "";
    els.largeImageMessage.textContent = "Select suspected wrong detections in Phase 1.";
    els.largeImageMessage.classList.remove("is-hidden");
    els.currentMeta.innerHTML = "";
    return;
  }
  const id = state.queue[state.queueIndex];
  els.queueStatus.textContent = `${fmtCount(state.queueIndex + 1)} of ${fmtCount(state.queue.length)}`;
  try {
    const detection = await api(`/api/detection?id=${encodeURIComponent(id)}`);
    state.currentDetection = detection;
    state.currentReviewCategory = null;
    state.currentReviewSubcategory = null;
    if (detection.image_url) {
      els.largeImage.onerror = () => {
        els.largeImage.classList.add("is-hidden");
        els.largeImageMessage.textContent = state.cropStatus?.status === "running"
          ? "Crop generation is still running for this job."
          : "This crop is not available.";
        els.largeImageMessage.classList.remove("is-hidden");
      };
      els.largeImage.src = detection.image_url;
      els.largeImage.alt = detection.raw_label_display;
      els.largeImage.classList.remove("is-hidden");
      els.largeImageMessage.classList.add("is-hidden");
    } else {
      els.largeImage.removeAttribute("src");
      els.largeImage.alt = "";
      els.largeImage.classList.add("is-hidden");
      els.largeImageMessage.textContent = "This crop is missing. In S3 mode the API should generate and cache it from the source image and bounding box.";
      els.largeImageMessage.classList.remove("is-hidden");
    }
    renderCurrentMeta(detection);
    renderCorrectionChoices();
    preloadNextImage();
  } catch (error) {
    els.queueStatus.textContent = error.message;
  }
}

function renderCurrentMeta(detection) {
  const bbox = Array.isArray(detection.bbox) ? detection.bbox.join(", ") : "No box";
  const score = detection.confidence === null || detection.confidence === undefined
    ? "No score"
    : Number(detection.confidence).toFixed(3);
  const cropState = detection.image_missing ? "Missing crop" : "Crop loaded";
  els.currentMeta.innerHTML = `
    <div><strong>Site</strong> ${escapeHtml(detection.site)}</div>
    <div><strong>Predicted</strong> ${escapeHtml(detection.raw_label_display)}</div>
    <div><strong>Score</strong> ${escapeHtml(score)}</div>
    <div><strong>Box</strong> ${escapeHtml(bbox)}</div>
    <div><strong>Image</strong> ${escapeHtml(cropState)}</div>
    <div><strong>Source</strong> ${escapeHtml(detection.source_image || "Unknown")}</div>
  `;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function renderCorrectionChoices() {
  renderCategoryButtons();
  renderSubcategoryButtons();
}

function categoryShortcut(category, index) {
  return category.shortcut || FALLBACK_CATEGORY_KEYS[index] || "";
}

function renderCategoryButtons() {
  state.categoryShortcutMap = new Map();
  els.categoryButtons.innerHTML = "";
  const fragment = document.createDocumentFragment();
  reviewCategories().forEach((category, index) => {
    const shortcut = categoryShortcut(category, index);
    if (shortcut && !state.categoryShortcutMap.has(shortcut.toLowerCase())) {
      state.categoryShortcutMap.set(shortcut.toLowerCase(), category);
    }
    const button = document.createElement("button");
    button.type = "button";
    button.className = "choice-button";
    if (category.is_false_detection) {
      button.classList.add("is-danger");
    }
    button.classList.toggle(
      "is-active",
      state.currentReviewCategory && categoryId(state.currentReviewCategory.id) === categoryId(category.id)
    );
    button.innerHTML = `
      <span class="key-pill">${escapeHtml(shortcut)}</span>
      <span class="choice-label"></span>
    `;
    button.querySelector(".choice-label").textContent = category.label;
    button.addEventListener("click", () => chooseReviewCategory(category));
    fragment.appendChild(button);
  });
  els.categoryButtons.appendChild(fragment);
}

function renderSubcategoryButtons() {
  state.subcategoryShortcutMap = new Map();
  els.subcategoryButtons.innerHTML = "";
  if (!state.currentReviewCategory || state.currentReviewCategory.requires_subcategory === false) {
    els.subcategoryBlock.classList.add("is-hidden");
    return;
  }
  els.subcategoryBlock.classList.remove("is-hidden");
  const subcategories = subcategoriesForCategory(state.currentReviewCategory.id);
  const fragment = document.createDocumentFragment();
  subcategories.forEach((subcategory, index) => {
    const shortcut = SUBCATEGORY_KEYS[index] || "";
    if (shortcut) {
      state.subcategoryShortcutMap.set(shortcut.toLowerCase(), subcategory);
    }
    const button = document.createElement("button");
    button.type = "button";
    button.className = "choice-button";
    button.classList.toggle(
      "is-active",
      state.currentReviewSubcategory && categoryId(state.currentReviewSubcategory.id) === categoryId(subcategory.id)
    );
    button.innerHTML = `
      <span class="key-pill">${escapeHtml(shortcut)}</span>
      <span class="choice-label"></span>
    `;
    button.querySelector(".choice-label").textContent = subcategory.name;
    button.addEventListener("click", () => chooseReviewSubcategory(subcategory));
    fragment.appendChild(button);
  });
  els.subcategoryButtons.appendChild(fragment);
}

function chooseReviewCategory(category) {
  if (!state.currentDetection) {
    return;
  }
  state.currentReviewCategory = category;
  state.currentReviewSubcategory = null;
  renderCorrectionChoices();
  if (category.requires_subcategory === false) {
    saveCurrentReview();
  }
}

function chooseReviewSubcategory(subcategory) {
  if (!state.currentReviewCategory || !state.currentDetection) {
    return;
  }
  state.currentReviewSubcategory = subcategory;
  renderSubcategoryButtons();
  saveCurrentReview();
}

async function saveCurrentReview() {
  const detection = state.currentDetection;
  const category = state.currentReviewCategory;
  const subcategory = state.currentReviewSubcategory;
  if (!detection || !category) {
    return;
  }
  if (category.requires_subcategory !== false && !subcategory) {
    return;
  }
  const review = {
    category_id: category.id,
    category_key: category.key,
    category_label: category.label,
    subcategory_id: subcategory ? subcategory.id : null,
    subcategory_name: subcategory ? subcategory.name : null
  };
  setSaveStatus("Saving");
  try {
    const payload = await api("/api/progress/review", {
      method: "POST",
      body: JSON.stringify({
        id: detection.id,
        review,
        job_id: state.activeJob.id,
      })
    });
    syncProgress(payload.progress);
    setSaveStatus("Saved");
    advanceAfterReview(detection.id);
  } catch (error) {
    setSaveStatus(error.message);
  }
}

function advanceAfterReview(reviewedId) {
  const nextIndex = state.queueIndex;
  state.queue = state.queue.filter((id) => id !== reviewedId);
  if (!state.queue.length) {
    state.queueIndex = 0;
    loadCurrentDetection();
    return;
  }
  state.queueIndex = Math.min(nextIndex, state.queue.length - 1);
  loadCurrentDetection();
}

function goPrevious() {
  if (!state.queue.length) {
    return;
  }
  state.queueIndex = Math.max(0, state.queueIndex - 1);
  loadCurrentDetection();
}

async function undoLastReview() {
  if (!state.activeJob) {
    return;
  }
  setSaveStatus("Undoing");
  try {
    const payload = await api("/api/progress/undo", {
      method: "POST",
      body: JSON.stringify({ job_id: state.activeJob.id })
    });
    syncProgress(payload.progress);
    if (!payload.undone) {
      setSaveStatus(payload.message || "Nothing to undo");
      return;
    }
    const existingIndex = state.queue.indexOf(payload.id);
    if (existingIndex >= 0) {
      state.queueIndex = existingIndex;
    } else {
      state.queue.splice(state.queueIndex, 0, payload.id);
    }
    setSaveStatus("Undone");
    loadCurrentDetection();
  } catch (error) {
    setSaveStatus(error.message);
  }
}

function renderUndoState() {
  const canUndo = Boolean(state.progress?.review_history?.length);
  els.undoButton.disabled = !canUndo;
}

function preloadNextImage() {
  const nextId = state.queue[state.queueIndex + 1];
  if (!nextId) {
    return;
  }
  api(`/api/detection?id=${encodeURIComponent(nextId)}`)
    .then((detection) => {
      if (!detection.image_url) {
        return;
      }
      const image = new Image();
      image.src = detection.image_url;
    })
    .catch(() => {});
}

function wireEvents() {
  els.jobsTab.addEventListener("click", () => switchPhase("jobs"));
  els.phaseOneTab.addEventListener("click", () => switchPhase("phase1"));
  els.phaseTwoTab.addEventListener("click", () => switchPhase("phase2"));
  els.accessKeyInput.addEventListener("change", () => setAccessKey(els.accessKeyInput.value));
  els.accessKeyInput.addEventListener("blur", () => setAccessKey(els.accessKeyInput.value));
  els.subcategoryFilter.addEventListener("change", () => {
    state.activeSubcategoryFilter = els.subcategoryFilter.value;
    resetDetections();
    loadMoreDetections();
  });
  els.selectAllButton.addEventListener("click", () => setFilteredSelection(true));
  els.clearAllButton.addEventListener("click", () => setFilteredSelection(false));
  els.startReviewButton.addEventListener("click", () => switchPhase("phase2"));
  els.previousButton.addEventListener("click", goPrevious);
  els.undoButton.addEventListener("click", undoLastReview);
  els.exportButton.addEventListener("click", () => {
    if (state.activeJob) {
      const apiBase = (window.PREDICTION_REVIEW_API_BASE || "").replace(/\/+$/, "");
      window.open(`${apiBase}/api/progress?${jobQuery().toString()}`, "_blank");
    }
  });

  const observer = new IntersectionObserver((entries) => {
    if (entries.some((entry) => entry.isIntersecting)) {
      loadMoreDetections();
    }
  }, { rootMargin: "700px 0px" });
  observer.observe(els.gridSentinel);

  document.addEventListener("keydown", handleKeyboard);
}

function handleKeyboard(event) {
  const tag = event.target?.tagName?.toLowerCase();
  if (tag === "input" || tag === "select" || tag === "textarea") {
    return;
  }
  if (state.activePhase !== "phase2") {
    return;
  }
  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "z") {
    event.preventDefault();
    undoLastReview();
    return;
  }
  if (event.key === "ArrowLeft" || event.key === "Backspace") {
    event.preventDefault();
    goPrevious();
    return;
  }
  const category = state.categoryShortcutMap.get(event.key.toLowerCase());
  if (category) {
    event.preventDefault();
    chooseReviewCategory(category);
    return;
  }
  const subcategory = state.subcategoryShortcutMap.get(event.key.toLowerCase());
  if (subcategory) {
    event.preventDefault();
    chooseReviewSubcategory(subcategory);
  }
}

async function init() {
  cacheElements();
  renderAccessKey();
  renderPhaseTabs();
  updateSelectionText();
  renderUndoState();
  wireEvents();
  if (!state.accessKey) {
    showError("Enter the access key to load review jobs.");
    return;
  }
  await loadBootstrap();
}

async function loadBootstrap() {
  clearError();
  if (!state.accessKey) {
    state.config = null;
    state.jobs = [];
    state.activeJob = null;
    renderAccessKey();
    renderAppChrome();
    renderPhaseTabs();
    renderJobs();
    showError("Enter the access key to load review jobs.");
    return;
  }
  const [config, jobsPayload] = await Promise.all([
    api("/api/config"),
    api("/api/jobs")
  ]);
  state.config = config;
  state.jobs = jobsPayload.jobs || [];
  state.activeJob = null;
  state.activePhase = "jobs";
  renderAppChrome();
  renderPhaseTabs();
  renderJobs();
  if (!state.jobs.length) {
    showError("No S3 detection jobs were found.");
  }
}

init().catch((error) => {
  document.body.innerHTML = `<pre>${escapeHtml(error.stack || error.message)}</pre>`;
});
