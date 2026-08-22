# AWS Deployment Notes

This review app is deployed as a new static site, separate from the existing detection-job launcher.

## Observed AWS State

Profile and region:

```text
profile: ben
region: ap-southeast-2
```

Related existing resources:

```text
CloudFront: E3UJ8LF8ZFMUHV
Static bucket: bird-detection-jobs-web-031496224070-ap-southeast-2
Existing URL: https://detections.aeroglobe.com.au
HTTP API: f0m9zzwmha
Lambda: bird-detection-jobs-api-api
DynamoDB: bird-detection-jobs-api-jobs
Data bucket: bird-detection-data
```

Current S3 jobs:

```text
s3://bird-detection-data/detections/DCCEEW-2026-08-10/
s3://bird-detection-data/detections/DCCEEW-2026-08-11/
```

Both current jobs were observed with `exportCrops: false`, so the review API generates and caches crops with a background whole-job worker.

## Deployed Stack

Template:

```text
review_tool/aws/full-stack.yaml
```

Resources:

```text
CloudFront + private S3 bucket: static HTML/CSS/JS
Lambda Function URL: review API
Lambda: job parsing, crop generation, progress writes
DynamoDB on-demand: shared selected/reviewed state
S3: detection results, cached review indexes, cached crops
```

Lambda Function URL is used instead of API Gateway because it is cheaper for this prototype and allows long-running first-load requests when a large job index must be built.

## Security Model

A static website cannot securely access private S3 by itself.

The deployed pattern is:

```text
browser -> Lambda Function URL -> S3/DynamoDB
```

The browser calls the Lambda API with a shared access key. Lambda validates the key, then uses its IAM role to read `bird-detection-data`, write cached review artifacts, and update DynamoDB.

Images are not exposed as public S3 objects. The app receives short-lived signed API URLs. The image endpoint validates the signature and redirects to a short-lived S3 URL for an existing crop. It does not crop on demand. Opening a job starts the background crop worker if the current cache is missing. The current exporter writes bbox values as `y,x,Y,X`, so crop generation treats the final four detection fields as row/column coordinates.

This is acceptable for the current collaborative prototype. For stronger user-level identity later, replace the shared access key with Cognito, IAM Identity Center, or another identity provider.

## Collaboration Model

Progress is shared by job, not by reviewer.

DynamoDB keys:

```text
PK = JOB#<jobId>
SK = SELECTED#<detectionId>

PK = JOB#<jobId>
SK = REVIEW#<detectionId>

PK = JOB#<jobId>
SK = EVENT#<timestamp>#<uuid>
```

Rules:

- Selecting or clearing a detection changes shared job state.
- Reviewing a detection writes one latest correction for that detection.
- Undo restores only the most recent review event for that job.
- Multiple users can work in the same job without separate local progress files.

## API

Current endpoints:

```text
GET  /api/config
GET  /api/jobs
GET  /api/summary?job_id=<job>
GET  /api/progress?job_id=<job>
GET  /api/detections?job_id=<job>&category_id=<id>&subcategory=<filter>&offset=<n>&limit=<n>
GET  /api/detection_ids?job_id=<job>&category_id=<id>&subcategory=<filter>
GET  /api/detection?id=<job/detection-id>
GET  /api/image?job_id=<job>&id=<job/detection-id>&expires=<unix>&sig=<hmac>
POST /api/detections/by-id
POST /api/progress/selection
POST /api/progress/review
POST /api/progress/undo
```

Errors are returned as JSON:

```json
{
  "error": "Source image was not found.",
  "code": "MISSING_SOURCE_IMAGE",
  "detail": "s3://bucket/key"
}
```

## Deployment Commands

Package and upload the Lambda zip:

```powershell
aws --profile ben --region ap-southeast-2 s3 cp .\review_tool\.deploy\prediction-review-api.zip s3://bird-detection-data/artifacts/prediction-review-api/prediction-review-api.zip
```

Deploy the stack:

```powershell
aws --profile ben --region ap-southeast-2 cloudformation deploy `
  --stack-name prediction-review `
  --template-file .\review_tool\aws\full-stack.yaml `
  --capabilities CAPABILITY_IAM `
  --parameter-overrides `
    SiteBucketName=<globally-unique-static-bucket> `
    CodeS3Bucket=bird-detection-data `
    CodeS3Key=artifacts/prediction-review-api/prediction-review-api.zip `
    AccessKey=<shared-access-key> `
    ImageUrlSecret=<image-url-secret>
```

Set the API URL in `static/config.js`, then upload static files to the stack's website bucket and invalidate CloudFront.

## Future Improvements

- Add real authentication with user identity.
- Add optional claim/lock records with TTL to reduce duplicated work.
- Add export endpoints for reviewed detections and unresolved selections.
- Add per-job `review_config.json` in S3 when jobs need custom labels.
