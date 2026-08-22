# Prediction Review Tool

Static review app and serverless API for checking object-detection predictions stored in S3.

The app has three screens:

1. Select an S3 detection job.
2. Select detections that may be wrong.
3. Correct selected detections one by one.

Progress is shared per job. There are no separate reviewer queues in the current version, so multiple collaborators see and update the same selected/reviewed state.

## Architecture

```text
Browser
  -> CloudFront + private S3 bucket for static files
  -> Lambda Function URL API
  -> SQS crop worker queue
  -> private S3 detection data and cached crops
  -> DynamoDB shared progress table
```

The browser never receives AWS credentials. The API requires a shared access key in the `x-prediction-review-key` header. Image URLs are short-lived HMAC-signed API URLs, and Lambda redirects to generated crops using its IAM role.

## Data Layout

Jobs are discovered under:

```text
s3://bird-detection-data/detections/
```

A job is any prefix containing:

```text
detection_results.txt
```

The API writes derived review artifacts under the same job prefix:

```text
detections/<job>/review/index.json
detections/<job>/review/summary.json
detections/<job>/review/crops-yx-v1/<detection_id>.jpg
```

When a job is opened, the API starts a background whole-job crop generator if the current crop cache is missing. The worker groups detections by source image, decodes each source image once, writes all crops for that image, and uses SQS shards so multiple Lambda invocations can process the job in parallel. Image requests only redirect to existing generated crops; they do not crop on demand.

## Detection Result Formats

Supported rows:

```text
source_image,detection_id,category,confidence,y,x,Y,X
source_image,detection_id,category,subcategory,confidence,<geo columns...>,y,x,Y,X
```

The current detection exporter writes the final four bbox fields as `y,x,Y,X` / `row,column,row,column`. The crop code follows that convention.

The source image path may be an `s3://bucket/key` URL or a key in the configured data bucket.

## Configuration

Job type category sets, shortcuts, bird subcategories, and legacy source-label mappings live in:

```text
config.json
```

The API detects the job type while parsing `detection_results.txt`. Rows with bird category labels or a predicted subcategory are treated as `birds`; rows with rubbish class labels are treated as `rubbish`. Rubbish jobs use the configured rubbish classes and do not require subcategory selection. Unknown job categories are still handled dynamically. The configured `Nothing / false detection` category is always available in Phase 2.

## Deployment

The stack template is:

```text
aws/full-stack.yaml
```

It creates:

- private static-site S3 bucket
- CloudFront distribution
- Lambda review API with Function URL
- SQS queue for parallel crop generation
- DynamoDB on-demand progress table
- IAM role for private S3/DynamoDB access

Deployment notes are in:

```text
docs/aws-deployment.md
```

After deployment, `static/config.js` must point at the Lambda Function URL:

```js
window.PREDICTION_REVIEW_API_BASE = "https://<lambda-url-id>.lambda-url.ap-southeast-2.on.aws";
```

## Local Development

There is no local Python server in this version. Edit the static files directly and deploy them to the static bucket, or serve `review_tool/` with any simple static web server if you want to inspect layout only. API-backed testing uses the deployed Lambda URL.

## GitHub Hygiene

This folder is ready to commit as source code and docs. Do not commit local prediction images, detection outputs, deployment zips, access keys, or generated caches. The `.gitignore` files ignore those runtime artifacts, including `review_tool/.deploy/`.
