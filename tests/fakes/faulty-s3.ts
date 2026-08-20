/**
 * An S3 stub with fault injection, for testing how LogStore classifies and
 * survives the error codes S3 actually returns.
 *
 * The integration stub (`tests/integration/fake-s3.mjs`) models a HEALTHY S3 —
 * conditional writes, GET, DELETE, LIST — because the failover suite is about
 * the protocol. This one exists for the opposite question: what happens when a
 * request 500s, a success response is lost, a LIST paginates, or a conditional
 * write loses a race. Faults are declared per method + key substring and burn
 * down a `times` counter, so a test can say "the next PUT to log/000000000002
 * fails with 500 but the write lands anyway".
 */

import { createHash } from 'node:crypto';
import { createServer, type Server } from 'node:http';

export interface Fault {
  method: 'PUT' | 'GET' | 'DELETE' | 'LIST';
  /** Applies when the object key contains this substring. */
  keyIncludes: string;
  /** How many matching requests to fail before the fault is spent. */
  times: number;
  status: number;
  /** S3 error code for the XML body, e.g. 'InternalError', 'SlowDown'. */
  code?: string;
  /**
   * Store the PUT body even though the response is an error — the "write
   * landed but the success response was lost" case that makes commit outcomes
   * ambiguous in the first place.
   */
  storeBody?: boolean;
  /** Runs after the fault fires — lets a test simulate a concurrent winner. */
  then?: () => void;
}

export interface FaultyS3 {
  url: string;
  store: Map<string, { body: Buffer; etag: string }>;
  requests: { get: number; put: number; list: number; delete: number };
  faults: Fault[];
  /** Plant an object directly — another writer's log entry, a lease, a snapshot. */
  setObject: (key: string, value: unknown) => void;
  close: () => Promise<void>;
}

const etagOf = (buf: Buffer): string => `"${createHash('md5').update(buf).digest('hex')}"`;
const esc = (s: string): string => s.replace(/&/g, '&amp;').replace(/</g, '&lt;');
const errorXml = (code: string): string => `<Error><Code>${code}</Code></Error>`;

/** `pageSize` caps LIST pages so pagination handling is actually exercised. */
export async function startFaultyS3(pageSize = 1000): Promise<FaultyS3> {
  const store = new Map<string, { body: Buffer; etag: string }>();
  const requests = { get: 0, put: 0, list: 0, delete: 0 };
  const faults: Fault[] = [];

  function takeFault(method: Fault['method'], key: string): Fault | undefined {
    const fault = faults.find(
      (f) => f.method === method && f.times > 0 && key.includes(f.keyIncludes),
    );
    if (fault) fault.times--;
    return fault;
  }

  const server: Server = createServer((req, res) => {
    void (async () => {
      const url = new URL(req.url!, 'http://x');
      // Path is /<bucket>/<key...> under path-style addressing.
      const key = decodeURIComponent(url.pathname.replace(/^\//, '').split('/').slice(1).join('/'));

      if (req.method === 'GET' && url.searchParams.get('list-type') === '2') {
        requests.list++;
        const fault = takeFault('LIST', url.searchParams.get('prefix') ?? '');
        if (fault) {
          fault.then?.();
          res.writeHead(fault.status, { 'content-type': 'application/xml' });
          return res.end(errorXml(fault.code ?? 'InternalError'));
        }
        const prefix = url.searchParams.get('prefix') ?? '';
        const all = [...store.keys()].filter((k) => k.startsWith(prefix)).sort();
        const from = Number(url.searchParams.get('continuation-token') ?? '0');
        const page = all.slice(from, from + pageSize);
        const truncated = from + pageSize < all.length;
        res.writeHead(200, { 'content-type': 'application/xml' });
        return res.end(
          `<?xml version="1.0" encoding="UTF-8"?>` +
            `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
            `<Name>b</Name><KeyCount>${page.length}</KeyCount>` +
            `<IsTruncated>${truncated}</IsTruncated>${
              truncated ? `<NextContinuationToken>${from + pageSize}</NextContinuationToken>` : ''
            }${page
              .map(
                (k) =>
                  `<Contents><Key>${esc(k)}</Key><Size>${store.get(k)!.body.length}</Size></Contents>`,
              )
              .join('')}</ListBucketResult>`,
        );
      }

      if (req.method === 'PUT') {
        requests.put++;
        const chunks: Buffer[] = [];
        for await (const c of req) chunks.push(c as Buffer);
        const body = Buffer.concat(chunks);

        const fault = takeFault('PUT', key);
        if (fault) {
          if (fault.storeBody) store.set(key, { body, etag: etagOf(body) });
          fault.then?.();
          res.writeHead(fault.status, { 'content-type': 'application/xml' });
          return res.end(errorXml(fault.code ?? 'InternalError'));
        }

        const existing = store.get(key);
        if (req.headers['if-none-match'] === '*' && existing) {
          res.writeHead(412, { 'content-type': 'application/xml' });
          return res.end(errorXml('PreconditionFailed'));
        }
        const ifMatch = req.headers['if-match'];
        if (ifMatch !== undefined && existing?.etag !== ifMatch) {
          res.writeHead(412, { 'content-type': 'application/xml' });
          return res.end(errorXml('PreconditionFailed'));
        }
        const etag = etagOf(body);
        store.set(key, { body, etag });
        res.writeHead(200, { ETag: etag });
        return res.end();
      }

      if (req.method === 'GET' || req.method === 'HEAD') {
        requests.get++;
        const fault = takeFault('GET', key);
        if (fault) {
          fault.then?.();
          res.writeHead(fault.status, { 'content-type': 'application/xml' });
          return res.end(errorXml(fault.code ?? 'InternalError'));
        }
        const object = store.get(key);
        if (!object) {
          res.writeHead(404, { 'content-type': 'application/xml' });
          return res.end(errorXml('NoSuchKey'));
        }
        res.writeHead(200, { ETag: object.etag, 'content-length': object.body.length });
        return res.end(req.method === 'HEAD' ? undefined : object.body);
      }

      if (req.method === 'DELETE') {
        requests.delete++;
        const fault = takeFault('DELETE', key);
        if (fault) {
          fault.then?.();
          res.writeHead(fault.status, { 'content-type': 'application/xml' });
          return res.end(errorXml(fault.code ?? 'InternalError'));
        }
        store.delete(key);
        res.writeHead(204);
        return res.end();
      }

      res.writeHead(400);
      res.end();
    })();
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  const port = typeof address === 'object' && address ? address.port : 0;

  return {
    url: `http://127.0.0.1:${port}`,
    store,
    requests,
    faults,
    setObject: (key, value) => {
      const body = Buffer.from(JSON.stringify(value));
      store.set(key, { body, etag: etagOf(body) });
    },
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}
