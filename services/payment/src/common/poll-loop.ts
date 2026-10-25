import type { Logger } from '@nestjs/common';
import { setTimeout as sleep } from 'node:timers/promises';

/**
 * Vòng poll nền có backoff, giống Relay.Run của order (Go): gọi `tick` mỗi `intervalMs`; tick lỗi
 * thì chờ tăng dần (2x, 4x... tối đa `maxBackoffMs`) để không dội lỗi khi Postgres/Redpanda sập.
 * Khác @nestjs/schedule `@Interval`: tick sau chỉ bắt đầu khi tick trước xong, không chồng nhau.
 */
export class PollLoop {
  private readonly abort = new AbortController();
  private done?: Promise<void>;

  constructor(
    private readonly name: string,
    private readonly tick: () => Promise<void>,
    private readonly intervalMs: number,
    private readonly log: Logger,
    private readonly maxBackoffMs = 10_000,
  ) {}

  start() {
    this.done ??= this.run();
  }

  /** Dừng vòng và chờ tick đang chạy (nếu có) xong. */
  async stop() {
    this.abort.abort();
    await this.done;
  }

  private async run() {
    let wait = this.intervalMs;
    while (!this.abort.signal.aborted) {
      try {
        await this.tick();
        wait = this.intervalMs;
      } catch (err) {
        if (this.abort.signal.aborted) return;
        wait = Math.min(wait * 2, this.maxBackoffMs);
        this.log.warn({ msg: `${this.name} failed, backing off`, err: String(err), retry_in_ms: wait });
      }
      await sleep(wait, undefined, { signal: this.abort.signal }).catch(() => {});
    }
  }
}
