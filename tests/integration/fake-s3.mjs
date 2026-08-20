/**
 * Minimal S3-compatible server with the conditional-write semantics Escapement
 * depends on: create-if-absent (`If-None-Match: *`), compare-and-swap
 * (`If-Match`), GET, DELETE and ListObjectsV2. Nothing else — the point of the
 * suite is the commit protocol, not the AWS SDK.
 */
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';

const etagOf = (buf) => '"' + createHash('md5').update(buf).digest('hex') + '"';
const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;');

export function startFakeS3(port) {
  const store = new Map();
  /**
   * Per-operation request counts, so the suite can assert on what a node costs
   * and not just on what it does. S3 bills LIST at the PUT rate and charges for
   * failed conditional requests too, so an idle follower that LISTs or probes
   * the lease with a PUT is a real recurring bill — see `counts` assertions in
   * failover.test.mjs.
   */
  const counts = { get: 0, put: 0, list: 0, delete: 0 };
  const server = createServer(async (req, res) => {
    const url = new URL(req.url, 'http://x');
    const key = decodeURIComponent(url.pathname.replace(/^\//, '').split('/').slice(1).join('/'));

    if (req.method === 'GET' && url.searchParams.get('list-type') === '2') {
      counts.list++;
      const prefix = url.searchParams.get('prefix') ?? '';
      const keys = [...store.keys()].filter((k) => k.startsWith(prefix)).sort();
      res.writeHead(200, { 'content-type': 'application/xml' });
      return res.end(
        '<?xml version="1.0" encoding="UTF-8"?>' +
          '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
          `<Name>b</Name><KeyCount>${keys.length}</KeyCount><IsTruncated>false</IsTruncated>` +
          keys
            .map(
              (k) =>
                `<Contents><Key>${esc(k)}</Key><Size>${store.get(k).body.length}</Size></Contents>`,
            )
            .join('') +
          '</ListBucketResult>',
      );
    }

    if (req.method === 'PUT') {
      counts.put++;
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const body = Buffer.concat(chunks);
      const existing = store.get(key);
      if (req.headers['if-none-match'] === '*' && existing) {
        res.writeHead(412);
        return res.end('<Error><Code>PreconditionFailed</Code></Error>');
      }
      const ifMatch = req.headers['if-match'];
      if (ifMatch !== undefined && (!existing || existing.etag !== ifMatch)) {
        res.writeHead(412);
        return res.end('<Error><Code>PreconditionFailed</Code></Error>');
      }
      const etag = etagOf(body);
      store.set(key, { body, etag });
      res.writeHead(200, { ETag: etag });
      return res.end();
    }

    if (req.method === 'GET' || req.method === 'HEAD') {
      counts.get++;
      const object = store.get(key);
      if (!object) {
        res.writeHead(404);
        return res.end('<Error><Code>NoSuchKey</Code></Error>');
      }
      res.writeHead(200, { ETag: object.etag, 'content-length': object.body.length });
      return res.end(req.method === 'HEAD' ? undefined : object.body);
    }

    if (req.method === 'DELETE') {
      counts.delete++;
      store.delete(key);
      res.writeHead(204);
      return res.end();
    }

    res.writeHead(400);
    res.end();
  });

  return new Promise((resolve) => server.listen(port, () => resolve({ server, store, counts })));
}
