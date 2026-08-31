# Image Processor Lambda

Container-image Lambda (Node 20 + Sharp) triggered by S3 `ObjectCreated` events
on the `uploads/` prefix of the image bucket. It compresses the image and
reports the result (success **or failure**) to the backend, which flips the DB
row to `PROCESSED` / `FAILED` and pushes realtime notifications to browsers.

It never touches the database — no VPC attachment, no `DATABASE_URL`. Its only
permissions are S3 read on `uploads/*`, S3 write on `processed/*`, and (in
queue mode) `sqs:SendMessage` on the events queue.

## What it does

1. Ignores records outside `uploads/` (prevents a reprocessing loop).
2. `GetObject` → `sharp().rotate().resize({width: MAX_WIDTH, withoutEnlargement: true}).jpeg({quality})`.
3. `PutObject` to `processed/{basename}.jpg` (same bucket unless `TARGET_BUCKET`).
4. Reports the result — a single event body
   `{type, originalKey, processedKey?, processedSize?, failureReason?, occurredAt}`,
   delivered by one of two legs selected with `NOTIFY_MODE`:
   - `api` (default): `POST {BACKEND_URL}/internal/images/events` with
     `x-api-key: LAMBDA_API_KEY` (one retry, then throws so S3 event retries
     kick in; 404 = no DB row, tolerated).
   - `sqs`: `SendMessage` to `SQS_QUEUE_URL` (SQS itself provides durability).

**Failure semantics**: a processing error is reported as
`{type:'failed', failureReason}` and the record is skipped — it does *not*
rethrow, so S3 won't retry the same object and spam duplicate failure
notifications. A failure to *report* throws, letting S3 retry the whole
invocation.

## Test (no AWS needed)

```bash
npm install
npm test    # stubbed S3/SQS + fetch: prefix skipping, compression, 404
            # tolerance, sqs-mode publishing, failure reporting
```

## Build & deploy

```bash
docker buildx build --platform linux/amd64 -t image-processor-lambda .
# push to ECR, create the function from the image, add the S3 event
# notification on uploads/ — full checklist in ../docs/DEPLOYMENT.md
```

Local invocation of the handler with a sample event:

```bash
BACKEND_URL=http://localhost:3001 LAMBDA_API_KEY=dev-key \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=eu-west-1 \
node -e '
require("./src/handler").handler({
  Records: [{ eventSource: "aws:s3", s3: { bucket: { name: "my-bucket" }, object: { key: "uploads/test.png" } } }],
}).then(console.log)'
```
