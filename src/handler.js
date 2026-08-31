// S3-event Lambda (container image, nodejs:20 runtime).
// Flow per record:
//   uploads/{id}.{ext}  ->  sharp compress  ->  processed/{id}.jpg (same bucket)
//   then reports the result to the backend, either:
//     NOTIFY_MODE=api (default): POST /internal/images/events with x-api-key
//     NOTIFY_MODE=sqs:           SendMessage to SQS_QUEUE_URL
// Event body: {type:'processed'|'failed', originalKey, processedKey?,
//   processedSize?, failureReason?, occurredAt}
//
// On processing failure the result is reported (type:'failed') and the record
// is skipped — no rethrow, so S3 event retries (which would duplicate the
// failure notification) are not triggered. A failure to *report* rethrows so
// the S3 event can retry the whole thing.
//
// AWS credentials are resolved by the SDK default chain (Lambda execution role).
// No VPC attachment required: this function only talks to S3 and the backend/SQS.

const { GetObjectCommand, PutObjectCommand, S3Client } = require('@aws-sdk/client-s3');
const sharp = require('sharp');

const s3 = new S3Client({});

const UPLOADS_PREFIX = 'uploads/';
const MAX_WIDTH = Number(process.env.MAX_WIDTH || 2000);
const JPEG_QUALITY = Number(process.env.JPEG_QUALITY || 80);
const NOTIFY_MODE = process.env.NOTIFY_MODE || 'api';
const BACKEND_URL = process.env.BACKEND_URL; // e.g. https://api.example.com
const LAMBDA_API_KEY = process.env.LAMBDA_API_KEY;
const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL;

// Created lazily so api-mode never loads/pays for the SQS client.
let sqs = null;
function getSqsClient() {
  if (!sqs) {
    const { SQSClient } = require('@aws-sdk/client-sqs');
    sqs = new SQSClient({});
  }
  return sqs;
}

function requireEnv() {
  if (NOTIFY_MODE !== 'api' && NOTIFY_MODE !== 'sqs') {
    throw new Error(`Invalid NOTIFY_MODE '${NOTIFY_MODE}' (expected 'api' or 'sqs')`);
  }
  if (NOTIFY_MODE === 'sqs') {
    if (!SQS_QUEUE_URL) throw new Error('SQS_QUEUE_URL env var is required when NOTIFY_MODE=sqs');
    return;
  }
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

async function sendViaApi(event) {
  const url = `${BACKEND_URL.replace(/\/$/, '')}/internal/images/events`;
  const body = JSON.stringify(event);

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
        console.warn(`Backend has no record for ${event.originalKey}, skipping`);
        return;
      }
      throw new Error(`Backend responded ${res.status}`);
    } catch (err) {
      if (attempt === 2) throw err;
      console.warn(`Backend callback failed (${err.message}), retrying...`);
    }
  }
}

async function sendViaSqs(event) {
  const { SendMessageCommand } = require('@aws-sdk/client-sqs');
  await getSqsClient().send(
    new SendMessageCommand({
      QueueUrl: SQS_QUEUE_URL,
      MessageBody: JSON.stringify(event),
    }),
  );
}

async function reportEvent(event) {
  if (!event.occurredAt) event.occurredAt = new Date().toISOString();
  if (NOTIFY_MODE === 'sqs') {
    await sendViaSqs(event);
  } else {
    await sendViaApi(event);
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

  await reportEvent({
    type: 'processed',
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
    const originalKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
    try {
      await processRecord(record);
    } catch (err) {
      // Report the failure and move on; rethrowing would make S3 retry the
      // same object and spam duplicate failure notifications.
      console.error(`Processing ${originalKey} failed: ${err.message}`);
      try {
        await reportEvent({
          type: 'failed',
          originalKey,
          failureReason: err.message,
        });
      } catch (reportErr) {
        throw reportErr; // reporting itself is broken — let S3 retry
      }
    }
  }

  return { processed: event.Records?.length ?? 0 };
};