/**
 * The only place that talks to S3.
 *
 * Everything here leans on one primitive: a conditional write. `If-None-Match:
 * *` creates a key only if it does not exist; `If-Match: <etag>` replaces one
 * only if it has not changed. Those two headers give the engine its durability
 * point, its fence against a second writer, and its lease hand-off.
 */

import {
  DeleteObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';

import type { Lease, LogEntry, SnapshotFile } from './types';
import { UpstreamUnavailableError } from '../utils/http-error';

/** Zero-padded so a lexicographic LIST is also numeric order. */
const pad = (n: number): string => String(n).padStart(12, '0');

function httpStatus(err: unknown): number | undefined {
  return (err as { $metadata?: { httpStatusCode?: number } } | undefined)?.$metadata
    ?.httpStatusCode;
}
function errName(err: unknown): string {
  return (err as { name?: string } | undefined)?.name ?? '';
}
export const isPreconditionFailed = (err: unknown): boolean =>
  httpStatus(err) === 412 || errName(err) === 'PreconditionFailed';
export const isNotFound = (err: unknown): boolean =>
  httpStatus(err) === 404 || errName(err) === 'NoSuchKey' || errName(err) === 'NotFound';

export interface LogStoreOptions {
  bucket: string;
  prefix: string;
  region: string;
  /** Point at MinIO or a stub. Enables path-style addressing. */
  endpoint?: string | undefined;
}

export class LogStore {
  private readonly s3: S3Client;
  private readonly bucket: string;
  private readonly prefix: string;

  constructor(opts: LogStoreOptions) {
    this.bucket = opts.bucket;
    this.prefix = opts.prefix.replace(/\/+$/, '');
    this.s3 = new S3Client({
      region: opts.region,
      ...(opts.endpoint ? { endpoint: opts.endpoint, forcePathStyle: true } : {}),
    });
  }

  logKey(seq: number): string {
    return `${this.prefix}/log/${pad(seq)}.json`;
  }
  snapshotKey(seq: number): string {
    return `${this.prefix}/snapshot/${pad(seq)}.json`;
  }
  leaseKey(): string {
    return `${this.prefix}/lease.json`;
  }
  /** The seq encoded in a key produced by logKey/snapshotKey. */
  static seqOf(key: string): number {
    return Number(key.slice(key.lastIndexOf('/') + 1).replace('.json', ''));
  }

  private async getJson<T>(key: string): Promise<{ value: T; etag: string | undefined } | null> {
    try {
      const res = await this.s3.send(new GetObjectCommand({ Bucket: this.bucket, Key: key }));
      const body = await res.Body!.transformToString();
      return { value: JSON.parse(body) as T, etag: res.ETag };
    } catch (err) {
      if (isNotFound(err)) return null;
      throw err;
    }
  }

  private async putJson(
    key: string,
    value: unknown,
    opts: { ifNoneMatch?: boolean; ifMatch?: string | undefined } = {},
  ): Promise<void> {
    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: JSON.stringify(value),
        ContentType: 'application/json',
        ...(opts.ifNoneMatch ? { IfNoneMatch: '*' } : {}),
        ...(opts.ifMatch ? { IfMatch: opts.ifMatch } : {}),
      }),
    );
  }

  async listKeys(subPrefix: string): Promise<string[]> {
    const out: string[] = [];
    let token: string | undefined;
    do {
      const res = await this.s3.send(
        new ListObjectsV2Command({
          Bucket: this.bucket,
          Prefix: `${this.prefix}/${subPrefix}`,
          ContinuationToken: token,
        }),
      );
      for (const o of res.Contents ?? []) if (o.Key) out.push(o.Key);
      token = res.NextContinuationToken;
    } while (token);
    return out.sort();
  }

  // -- log ------------------------------------------------------------------

  /**
   * Append one batch.
   *
   * A `412` and a network timeout are handled by the same move — read the key
   * back and compare `batchId` — which collapses "did my write land?" and "have
   * I been fenced?" into one decidable question. Returns `fenced` when the slot
   * belongs to another writer; throws when the outcome is still unknown after
   * `maxAttempts`, which the engine treats as fatal.
   */
  async append(entry: LogEntry, maxAttempts = 6): Promise<'committed' | 'fenced'> {
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        await this.putJson(this.logKey(entry.seq), entry, { ifNoneMatch: true });
        return 'committed';
      } catch (err) {
        if (!isPreconditionFailed(err)) {
          await new Promise((r) => setTimeout(r, 50 * 2 ** attempt));
        }
        let existing: { value: LogEntry } | null;
        try {
          existing = await this.getJson<LogEntry>(this.logKey(entry.seq));
        } catch {
          continue; // could not read back; retry the whole step
        }
        if (existing === null) continue; // never landed — retry the same seq
        return existing.value.batchId === entry.batchId ? 'committed' : 'fenced';
      }
    }
    throw new UpstreamUnavailableError(
      `commit outcome for seq ${entry.seq} is still unknown after ${maxAttempts} attempts`,
    );
  }

  async readLog(seq: number): Promise<LogEntry | null> {
    const res = await this.getJson<LogEntry>(this.logKey(seq));
    return res?.value ?? null;
  }

  // -- snapshots ------------------------------------------------------------

  async readLatestSnapshot(): Promise<SnapshotFile | null> {
    const keys = await this.listKeys('snapshot/');
    const newest = keys[keys.length - 1];
    if (!newest) return null;
    const res = await this.getJson<SnapshotFile>(newest);
    return res?.value ?? null;
  }

  async writeSnapshot(snap: SnapshotFile): Promise<void> {
    await this.putJson(this.snapshotKey(snap.seq), snap);
  }

  /** Drop log entries the snapshot already covers, keeping recovery short. */
  async pruneLog(upToSeq: number): Promise<number> {
    let removed = 0;
    for (const key of await this.listKeys('log/')) {
      if (LogStore.seqOf(key) <= upToSeq) {
        await this.s3.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: key }));
        removed++;
      }
    }
    return removed;
  }

  // -- lease ----------------------------------------------------------------

  /**
   * Try to become leader.
   *
   * The lease is an optimisation, not the safety mechanism: it stops two nodes
   * from wasting effort fighting. Correctness comes from `append`, so a takeover
   * that guesses wrong about liveness still cannot issue anything twice — the
   * loser is fenced on its first commit.
   *
   * Reads before it writes, and that ordering is purely about cost. A follower
   * calls this on every poll for the lifetime of the process, and the answer is
   * almost always "someone else holds it, stay put". Leading with the write made
   * that answer arrive as a 412 on a PUT, which S3 bills at the PUT rate —
   * failed conditional requests are charged like any other ("You are only
   * charged existing rates for the applicable requests, including for failed
   * requests"). A PUT costs 12.5x a GET, so the common path was paying write
   * prices to be told no. Looking first makes the steady state one GET, and the
   * writes happen only when there is actually a lease to take.
   *
   * The conditional headers still do the fencing; they are just no longer how we
   * ask the question. Both races the read-first order opens are covered:
   * a lease created between our GET and our `If-None-Match` PUT fails that PUT,
   * and one replaced between them fails the `If-Match` PUT. Either way we go
   * round again rather than assuming.
   */
  async tryAcquireLease(
    mine: Lease,
    maxAttempts = 3,
  ): Promise<{ won: true } | { won: false; held: Lease }> {
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const current = await this.getJson<Lease>(this.leaseKey());

      if (current === null) {
        // No lease object at all: a cold bucket, or a graceful hand-off that
        // deleted it. Create-only, so a peer racing us here loses.
        try {
          await this.putJson(this.leaseKey(), mine, { ifNoneMatch: true });
          return { won: true };
        } catch (err) {
          if (!isPreconditionFailed(err)) throw err;
          continue; // someone created it first — re-read and reconsider
        }
      }

      if (current.value.expiresAt > Date.now()) {
        return { won: false, held: current.value };
      }

      // Expired. If-Match keeps two waiting followers from both taking over.
      try {
        await this.putJson(this.leaseKey(), mine, { ifMatch: current.etag });
        return { won: true };
      } catch (err) {
        if (!isPreconditionFailed(err)) throw err;
        return { won: false, held: current.value };
      }
    }

    // Contended past the attempt budget. Reporting a loss is the safe answer:
    // the caller stays a follower and tries again on its next poll.
    const last = await this.getJson<Lease>(this.leaseKey());
    return last === null ? { won: false, held: mine } : { won: false, held: last.value };
  }

  async renewLease(mine: Lease): Promise<void> {
    await this.putJson(this.leaseKey(), mine);
  }

  async releaseLease(): Promise<void> {
    await this.s3.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: this.leaseKey() }));
  }
}
