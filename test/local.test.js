// Local unit test: runs the handler with a stubbed S3 client and fetch.
// No AWS access needed. Run with: npm test

const test = require('node:test');
const assert = require('node:assert');
const { Readable } = require('node:stream');
const { S3Client } = require('@aws-sdk/client-s3');

process.env.BACKEND_URL = 'http://backend.test';
process.env.LAMBDA_API_KEY = 'test-key';
process.env.MAX_WIDTH = '1000';

const handler = require('../src/handler');

async function makePng() {
  return require('sharp')({
    create: { width: 3000, height: 2000, channels: 3, background: { r: 200, g: 50, b: 100 } },
  })
    .png()
    .toBuffer();
}

function makeEvent(key) {
  return {
    Records: [
      {
        eventSource: 'aws:s3',
        s3: {
          bucket: { name: 'source-bucket' },
          object: { key: Buffer.from(key).toString('base64') === key ? key : encodeURIComponent(key) },
        },
      },
    ],
  };
}

function stubS3({ body, puts }) {
  S3Client.prototype.send = async function (command) {
    const name = command.constructor.name;
    if (name === 'GetObjectCommand') {
      assert.equal(command.input.Bucket, 'source-bucket');
      return { Body: Readable.from([body]) };
    }
    if (name === 'PutObjectCommand') {
      puts.push(command.input);
      return {};
    }
    throw new Error(`unexpected command ${name}`);
  };
}

test('skips objects outside uploads/ prefix', async () => {
  const puts = [];
  let fetched = 0;
  stubS3({ body: await makePng(), puts });
  globalThis.fetch = async () => {
    fetched++;
    return { ok: true, status: 200 };
  };

  const result = await handler.handler(makeEvent('processed/foo.jpg'));
  assert.equal(result.processed, 1);
  assert.equal(puts.length, 0, 'no S3 writes expected');
  assert.equal(fetched, 0, 'no backend call expected');
});

test('compresses, writes processed/, and notifies backend', async () => {
  const png = await makePng();
  const puts = [];
  const calls = [];
  stubS3({ body: png, puts });
  globalThis.fetch = async (url, opts) => {
    calls.push({ url, opts });
    return { ok: true, status: 200 };
  };

  const result = await handler.handler(makeEvent('uploads/abc-123.png'));
  assert.equal(result.processed, 1);

  // S3 write
  assert.equal(puts.length, 1);
  assert.equal(puts[0].Bucket, 'source-bucket');
  assert.equal(puts[0].Key, 'processed/abc-123.jpg');
  assert.equal(puts[0].ContentType, 'image/jpeg');

  // Backend callback
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, 'http://backend.test/internal/images/processed');
  assert.equal(calls[0].opts.headers['x-api-key'], 'test-key');
  const payload = JSON.parse(calls[0].opts.body);
  assert.equal(payload.originalKey, 'uploads/abc-123.png');
  assert.equal(payload.processedKey, 'processed/abc-123.jpg');
  assert.equal(payload.processedSize, puts[0].Body.length);

  // Compression actually happened: output smaller than input, resized to MAX_WIDTH
  const sharp = require('sharp');
  const meta = await sharp(puts[0].Body).metadata();
  assert.equal(meta.width, 1000);
  assert.ok(puts[0].Body.length < png.length, 'compressed output should be smaller');
});

test('backend 404 is tolerated without throwing', async () => {
  const puts = [];
  stubS3({ body: await makePng(), puts });
  globalThis.fetch = async () => ({ ok: false, status: 404 });

  const result = await handler.handler(makeEvent('uploads/orphan.png'));
  assert.equal(result.processed, 1);
  assert.equal(puts.length, 1, 'S3 write still happens');
});
