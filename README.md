# Image Processor Lambda

Container-image Lambda (Node 20 + Sharp) triggered by S3 `ObjectCreated` events
on the `uploads/` prefix of the image bucket. It compresses the image and
notifies the backend API, which flips the DB row to `PROCESSED`.

It never touches the database — no VPC attachment, no `DATABASE_URL`. Its only
permissions are S3 read on `uploads/*` and S3 write on `processed/*`.

## What it does

1. Ignores records outside `uploads/` (prevents a reprocessing loop).
2. `GetObject` → `sharp().rotate().resize({width: MAX_WIDTH, withoutEnlargement: true}).jpeg({quality})`.
3. `PutObject` to `processed/{basename}.jpg` (same bucket unless `TARGET_BUCKET`).
4. `POST {BACKEND_URL}/internal/images/processed` with `x-api-key: LAMBDA_API_KEY`
   and `{originalKey, processedKey, processedSize}` (one retry, then throws so
   S3 event retries kick in).

## Test (no AWS needed)

```bash
npm install
npm test    # stubbed S3 + fetch: prefix skipping, compression, 404 tolerance
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
