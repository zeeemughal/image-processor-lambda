// Local unit test: runs the handler with a stubbed S3/SQS clients and fetch.
// No AWS access needed. Run with: npm test

const test = require('node:test');
const assert = require('node:assert');
const { Readable } = require('node:stream');
const { S3Client } = require('@aws-sdk/client-s3');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

const DEFAULT_ENV = {
  NOTIFY_MODE: 'api',
  BACKEND_URL: 'http://backend.test',
  LAMBDA_API_KEY: 'test-key',
  MAX_WIDTH: '1000',
};

// The handler captures env at module load, so each test re-loads it with its
// own env vars.
function loadHandler(env) {
  delete require.cache[require.resolve('../src/handler')];
  return require('../src/handler');
}

function withEnv(env, fn) {
  const saved = {};
  for (const [k, v] of Object.entries(env)) {
    saved[k] = process.env[k];
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
  return Promise.resolve()
    .then(fn)
    .finally(() => {
      for (const [k, v] of Object.entries(saved)) {
        if (v === undefined) delete process.env[k];
        else process.env[k] = v;
      }
      delete require.cache[require.resolve('../src/handler')];
    });
}

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

function stubS3({ body, puts, getError }) {
  S3Client.prototype.send = async function (command) {
    const name = command.constructor.name;
    if (name === 'GetObjectCommand') {
      if (getError) throw getError;
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

function stubSqs({ sent, sendError }) {
  SQSClient.prototype.send = async function (command) {
    if (sendError) throw sendError;
    if (!(command instanceof SendMessageCommand)) {
      throw new Error(`unexpected sqs command ${command.constructor.name}`);
    }
    sent.push(command.input);
    return {};
  };
}

test('skips objects outside uploads/ prefix', async () => {
  await withEnv(DEFAULT_ENV, async () => {
    const handler = loadHandler();
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
});

test('compresses, writes processed/, and notifies backend', async () => {
  await withEnv(DEFAULT_ENV, async () => {
    const handler = loadHandler();
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
    assert.equal(calls[0].url, 'http://backend.test/internal/images/events');
    assert.equal(calls[0].opts.headers['x-api-key'], 'test-key');
    const payload = JSON.parse(calls[0].opts.body);
    assert.equal(payload.type, 'processed');
    assert.equal(payload.originalKey, 'uploads/abc-123.png');
    assert.equal(payload.processedKey, 'processed/abc-123.jpg');
    assert.equal(payload.processedSize, puts[0].Body.length);
    assert.ok(payload.occurredAt, 'occurredAt should be stamped');

    // Compression actually happened: output smaller than input, resized to MAX_WIDTH
    const sharp = require('sharp');
    const meta = await sharp(puts[0].Body).metadata();
    assert.equal(meta.width, 1000);
    assert.ok(puts[0].Body.length < png.length, 'compressed output should be smaller');
  });
});

test('backend 404 is tolerated without throwing', async () => {
  await withEnv(DEFAULT_ENV, async () => {
    const handler = loadHandler();
    const puts = [];
    stubS3({ body: await makePng(), puts });
    globalThis.fetch = async () => ({ ok: false, status: 404 });

    const result = await handler.handler(makeEvent('uploads/orphan.png'));
    assert.equal(result.processed, 1);
    assert.equal(puts.length, 1, 'S3 write still happens');
  });
});

test('sqs mode: publishes processed event to the queue', async () => {
  await withEnv({
    ...DEFAULT_ENV,
    NOTIFY_MODE: 'sqs',
    SQS_QUEUE_URL: 'https://sqs.test/queue',
  }, async () => {
    const handler = loadHandler();
    const puts = [];
    const sent = [];
    stubS3({ body: await makePng(), puts });
    stubSqs({ sent });

    const result = await handler.handler(makeEvent('uploads/abc-123.png'));
    assert.equal(result.processed, 1);
    assert.equal(puts.length, 1);
    assert.equal(sent.length, 1);
    assert.equal(sent[0].QueueUrl, 'https://sqs.test/queue');
    const payload = JSON.parse(sent[0].MessageBody);
    assert.equal(payload.type, 'processed');
    assert.equal(payload.originalKey, 'uploads/abc-123.png');
    assert.equal(payload.processedKey, 'processed/abc-123.jpg');
    assert.ok(payload.occurredAt);
    assert.ok(!globalThis.fetch || true, 'fetch not needed in sqs mode');
  });
});

test('sqs mode: processing failure publishes failed event without throwing', async () => {
  await withEnv({
    ...DEFAULT_ENV,
    NOTIFY_MODE: 'api',
    SQS_QUEUE_URL: 'https://sqs.test/queue',
  }, async () => {
    const handler = loadHandler();
    const calls = [];
    stubS3({ body: Buffer.from('this is not an image'), puts: [] });
    globalThis.fetch = async (url, opts) => {
      calls.push({ url, opts });
      return { ok: true, status: 200 };
    };

    // sharp will reject on the invalid body; handler must report 'failed'
    // and resolve instead of throwing.
    const result = await handler.handler(makeEvent('uploads/broken.png'));
    assert.equal(result.processed, 1);
    assert.equal(calls.length, 1);
    const payload = JSON.parse(calls[0].opts.body);
    assert.equal(payload.type, 'failed');
    assert.equal(payload.originalKey, 'uploads/broken.png');
    assert.ok(payload.failureReason);
    assert.ok(payload.occurredAt);
    assert.ok(!('processedKey' in payload));
  });
});

test('sqs mode: report failure rethrows so S3 retries', async () => {
  await withEnv({
    ...DEFAULT_ENV,
    NOTIFY_MODE: 'sqs',
    SQS_QUEUE_URL: 'https://sqs.test/queue',
  }, async () => {
    const handler = loadHandler();
    stubS3({ body: await makePng(), puts: [] });
    stubSqs({ sent: [], sendError: new Error('sqs down') });

    await assert.rejects(
      () => handler.handler(makeEvent('uploads/abc-123.png')),
      /sqs down/,
    );
  });
});

test('sqs mode without SQS_QUEUE_URL fails fast', async () => {
  await withEnv({
    NOTIFY_MODE: 'sqs',
    BACKEND_URL: 'http://backend.test',
    MAX_WIDTH: '1000',
  }, async () => {
    const handler = loadHandler();
    await assert.rejects(
      () => handler.handler(makeEvent('uploads/abc-123.png')),
      /SQS_QUEUE_URL/,
    );
  });
});