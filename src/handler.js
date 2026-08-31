// S3-event Lambda (container image, nodejs:20 runtime).
// Flow per record:
//   uploads/{id}.{ext}  ->  sharp compress  ->  processed/{id}.jpg (same bucket)
//   then POST /internal/images/processed to the backend with a shared x-api-key.
//
// AWS credentials are resolved by the SDK default chain (Lambda execution role).
// No VPC attachment required: this function only talks to S3 and the backend URL.

const { GetObjectCommand, PutObjectCommand, S3Client } = require('@aws-sdk/client-s3');
const sharp = require('sharp');

const s3 = new S3Client({});

const UPLOADS_PREFIX = 'uploads/';
const MAX_WIDTH = Number(process.env.MAX_WIDTH || 2000);
const JPEG_QUALITY = Number(process.env.JPEG_QUALITY || 80);
const BACKEND_URL = process.env.BACKEND_URL; // e.g. https://api.example.com
const LAMBDA_API_KEY = process.env.LAMBDA_API_KEY;

function requireEnv() {
  if (!BACKEND_URL) throw new Error('BACKEND_URL env var is required');
  if (!LAMBDA_API_KEY) throw new Error('LAMBDA_API_KEY env var is required');
}

function streamToBuffer(stream) {
  const chunks = [];
  return new Promise((resolve, reject) => {
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks)));
  });
}

async function notifyBackend(payload) {
  const url = `${BACKEND_URL.replace(/\/$/, '')}/internal/images/processed`;
  const body = JSON.stringify(payload);

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-api-key': LAMBDA_API_KEY,
        },
        body,
      });
      if (res.ok) return;
      // 404 = no DB row for this key (e.g. event from a manual upload);
      // not worth retrying.
      if (res.status === 404) {
        console.warn(`Backend has no record for ${payload.originalKey}, skipping`);
        return;
      }
      throw new Error(`Backend responded ${res.status}`);
    } catch (err) {
      if (attempt === 2) throw err;
      console.warn(`Backend callback failed (${err.message}), retrying...`);
    }
  }
}

async function processRecord(record) {
  const bucket = record.s3.bucket.name;
  // S3 event keys are URL-encoded
  const originalKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));

  // Only react to originals; processed/ writes must not trigger reprocessing.
  if (!originalKey.startsWith(UPLOADS_PREFIX)) {
    console.log(`Skipping ${originalKey} (not under ${UPLOADS_PREFIX})`);
    return;
  }

  const targetBucket = process.env.TARGET_BUCKET || bucket;
  const basename = originalKey.slice(UPLOADS_PREFIX.length).replace(/\.[^.]+$/, '');
  const processedKey = `processed/${basename}.jpg`;

  console.log(`Fetching s3://${bucket}/${originalKey}`);
  const getRes = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: originalKey }));
  const input = await streamToBuffer(getRes.Body);

  const output = await sharp(input)
    .rotate() // respect EXIF orientation
    .resize({ width: MAX_WIDTH, withoutEnlargement: true })
    .jpeg({ quality: JPEG_QUALITY })
    .toBuffer();

  await s3.send(
    new PutObjectCommand({
      Bucket: targetBucket,
      Key: processedKey,
      Body: output,
      ContentType: 'image/jpeg',
    }),
  );

  const saved = ((input.length - output.length) / input.length * 100).toFixed(1);
  console.log(
    `Compressed ${originalKey}: ${input.length} -> ${output.length} bytes (${saved}% saved), wrote s3://${targetBucket}/${processedKey}`,
  );

  await notifyBackend({
    originalKey,
    processedKey,
    processedSize: output.length,
  });
}

exports.handler = async (event) => {
  requireEnv();
  console.log(`Event with ${event.Records?.length ?? 0} record(s)`);

  for (const record of event.Records ?? []) {
    if (record.eventSource !== 'aws:s3' && record.eventSource !== 's3') continue;
    await processRecord(record);
  }

  return { processed: event.Records?.length ?? 0 };
};
