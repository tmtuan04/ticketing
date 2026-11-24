import { Injectable } from '@nestjs/common';
import type { Metadata } from '@grpc/grpc-js';
import { CatalogClient } from './catalog.client';
import type { Event, Seat } from './catalog.pb';
import { isPublished } from './views';

/** Số khóa tối đa giữ trong bộ nhớ (mỗi event tối đa 2 khóa); quá thì bỏ khóa cũ nhất. */
const MAX_ENTRIES = 1000;

export interface SeatMapData {
  event: Event;
  seats: Seat[];
}

/**
 * Cache dữ liệu catalog của event ĐÃ PUBLISH cho API công khai (chi tiết event, sơ đồ ghế).
 *
 * Vì sao cache được mà không cần TTL hay invalidate: sau khi publish, catalog không còn RPC nào sửa
 * event/hạng vé/ghế (GenerateSeats bị khóa, không có update/unpublish), nên dữ liệu này bất biến.
 * Event draft thì không cache vì admin còn sửa. Trạng thái ghế (available/held/sold) là dữ liệu sống
 * của inventory, KHÔNG nằm ở đây: controller vẫn hỏi inventory mỗi request.
 *
 * Vì sao cần: load test full_flow cho thấy catalog (Node 1 CPU, ~5ms CPU mỗi RPC) bão hòa ở ~100 lượt
 * xem sơ đồ ghế/s, 504 hàng loạt, trong khi phần dữ liệu nó trả về không bao giờ đổi.
 *
 * Lưu Promise thay vì giá trị: các request tới cùng lúc khi cache còn trống (đúng lúc mở bán) dùng
 * chung một lời gọi catalog thay vì cùng ập vào catalog. Lỗi hoặc event chưa publish thì xóa khóa,
 * request sau gọi lại. Mỗi replica gateway có cache riêng: không cần đồng bộ vì dữ liệu bất biến.
 */
@Injectable()
export class PublishedCatalogCache {
  private readonly entries = new Map<string, Promise<unknown>>();

  constructor(private readonly catalog: CatalogClient) {}

  /** Event (kèm venue, hạng vé). Trả cả event draft cho caller tự quyết, chỉ là không cache. */
  getEvent(eventId: string, md: Metadata): Promise<Event> {
    return this.load(
      `event:${eventId}`,
      async () => (await this.catalog.getEvent({ eventId }, md)).event!,
      isPublished,
    );
  }

  /** Event + danh sách ghế (chưa có trạng thái), gọi song song hai RPC khi chưa có trong cache. */
  getSeatMap(eventId: string, md: Metadata): Promise<SeatMapData> {
    return this.load(
      `seats:${eventId}`,
      async () => {
        const [{ event }, { seats }] = await Promise.all([
          this.catalog.getEvent({ eventId }, md),
          this.catalog.listSeats({ eventId }, md),
        ]);
        return { event: event!, seats };
      },
      (v) => isPublished(v.event),
    );
  }

  /** Xóa toàn bộ cache (dùng trong test). */
  clear() {
    this.entries.clear();
  }

  get size() {
    return this.entries.size;
  }

  private load<T>(key: string, fetch: () => Promise<T>, cacheable: (v: T) => boolean): Promise<T> {
    const hit = this.entries.get(key);
    if (hit) return hit as Promise<T>;

    // chỉ xóa nếu khóa vẫn trỏ tới đúng Promise này (có thể đã bị clear/evict rồi nạp lại)
    const forget = () => {
      if (this.entries.get(key) === promise) this.entries.delete(key);
    };
    const promise: Promise<T> = fetch().then(
      (v) => {
        if (!cacheable(v)) forget();
        return v;
      },
      (err: unknown) => {
        forget();
        throw err;
      },
    );
    this.entries.set(key, promise);
    // Map giữ thứ tự chèn: khóa đầu tiên là khóa cũ nhất
    while (this.entries.size > MAX_ENTRIES) this.entries.delete(this.entries.keys().next().value!);
    return promise;
  }
}
