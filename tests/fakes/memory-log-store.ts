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
  appendCalls = 0;

  /** Set to make the next append behave as though another writer won the slot. */
  fenceNextAppend = false;

  /**
   * Seqs that LIST still reports but GET cannot find — what a reader sees when
   * compaction prunes a key between its LIST and its GET.
   */
  readonly hideFromRead = new Set<number>();

  async append(entry: LogEntry): Promise<'committed' | 'fenced'> {
    this.appendCalls++;
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

  async tryAcquireLease(mine: Lease): Promise<{ won: true } | { won: false; held: Lease }> {
    if (this.lease && this.lease.expiresAt > Date.now()) {
      return { won: false, held: this.lease };
    }
    this.lease = mine;
    return { won: true };
  }

  async renewLease(mine: Lease): Promise<void> {
    this.lease = mine;
  }

  async releaseLease(): Promise<void> {
    this.lease = null;
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
