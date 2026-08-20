/**
 * In-memory stand-in for LogStore with the same conditional-write semantics.
 *
 * The point of the engine tests is the commit protocol, not the AWS SDK, so this
 * models exactly what S3 guarantees: create-if-absent, compare-and-swap, and
 * nothing else.
 */

import type { LogStore } from '../../src/engine/log-store';
import type { Lease, LogEntry, SnapshotFile } from '../../src/engine/types';

export class MemoryLogStore {
  readonly log = new Map<number, LogEntry>();
  readonly snapshots: SnapshotFile[] = [];
  lease: Lease | null = null;
  /** Bumped on every lease write, the way S3 mints a new etag per PUT. */
  leaseEtag = 'etag-0';
  private leaseWrites = 0;
  appendCalls = 0;

  /** Set to make the next append behave as though another writer won the slot. */
  fenceNextAppend = false;

  /**
   * Set to make the next append throw — what LogStore.append does when six
   * attempts still cannot determine whether the write landed. The engine must
   * treat that as fatal rather than guess.
   */
  throwNextAppend = false;

  /** Set to make the next readLease throw once — a transient S3 error. */
  throwNextReadLease = false;

  /**
   * Seqs that LIST still reports but GET cannot find — what a reader sees when
   * compaction prunes a key between its LIST and its GET.
   */
  readonly hideFromRead = new Set<number>();

  async append(entry: LogEntry): Promise<'committed' | 'fenced'> {
    this.appendCalls++;
    if (this.throwNextAppend) {
      this.throwNextAppend = false;
      throw new Error(`commit outcome for seq ${entry.seq} is still unknown`);
    }
    if (this.fenceNextAppend) {
      this.fenceNextAppend = false;
      this.log.set(entry.seq, { ...entry, batchId: 'someone-elses-batch', writerId: 'other' });
      return 'fenced';
    }
    if (this.log.has(entry.seq)) {
      return this.log.get(entry.seq)!.batchId === entry.batchId ? 'committed' : 'fenced';
    }
    this.log.set(entry.seq, entry);
    return 'committed';
  }

  async readLog(seq: number): Promise<LogEntry | null> {
    if (this.hideFromRead.has(seq)) return null;
    return this.log.get(seq) ?? null;
  }

  async readLatestSnapshot(): Promise<SnapshotFile | null> {
    return this.snapshots[this.snapshots.length - 1] ?? null;
  }

  async writeSnapshot(snap: SnapshotFile): Promise<void> {
    // Round-trip through JSON so a snapshot that cannot serialize fails here,
    // the way it would against S3.
    this.snapshots.push(JSON.parse(JSON.stringify(snap)) as SnapshotFile);
  }

  async pruneLog(upToSeq: number): Promise<number> {
    let removed = 0;
    for (const seq of [...this.log.keys()]) {
      if (seq <= upToSeq) {
        this.log.delete(seq);
        removed++;
      }
    }
    return removed;
  }

  /** Another node's takeover, for tests: replaces the lease and mints a new etag. */
  takeLease(lease: Lease): void {
    this.lease = lease;
    this.leaseEtag = `etag-${++this.leaseWrites}`;
  }

  async readLease(): Promise<{ lease: Lease; etag: string | undefined } | null> {
    if (this.throwNextReadLease) {
      this.throwNextReadLease = false;
      throw new Error('transient S3 error reading the lease');
    }
    return this.lease === null ? null : { lease: this.lease, etag: this.leaseEtag };
  }

  async tryAcquireLease(
    mine: Lease,
  ): Promise<{ won: true; etag: string | undefined } | { won: false; held: Lease | null }> {
    if (this.lease && this.lease.expiresAt > Date.now()) {
      return { won: false, held: this.lease };
    }
    this.takeLease(mine);
    return { won: true, etag: this.leaseEtag };
  }

  /** Conditional, like S3's If-Match: a renew against a replaced lease fails. */
  async renewLease(mine: Lease, etag: string): Promise<string | undefined> {
    if (this.lease === null || etag !== this.leaseEtag) {
      throw Object.assign(new Error('PreconditionFailed'), { name: 'PreconditionFailed' });
    }
    this.takeLease(mine);
    return this.leaseEtag;
  }

  /** Verified delete, like the real store: only the holder's release removes it. */
  async releaseLease(writerId: string): Promise<void> {
    if (this.lease?.writerId === writerId) this.lease = null;
  }

  asLogStore(): LogStore {
    return this as unknown as LogStore;
  }
}

export const silentLogger = {
  info: (): void => {},
  warn: (): void => {},
  error: (): void => {},
};
