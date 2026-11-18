import { Metadata } from '@grpc/grpc-js';
import { PublishedCatalogCache } from './catalog.cache';
import type { CatalogClient } from './catalog.client';
import { EventStatus, type Event } from './catalog.pb';

const md = new Metadata();

function event(id: string, status = EventStatus.EVENT_STATUS_PUBLISHED) {
  return { id, status } as Event;
}

/** CatalogClient giả: đếm số lần gọi, kết quả do test quyết định. */
function fakeCatalog() {
  const calls = { getEvent: 0, listSeats: 0 };
  let status = EventStatus.EVENT_STATUS_PUBLISHED;
  let fail = false;
  const client = {
    getEvent: async ({ eventId }: { eventId: string }) => {
      calls.getEvent++;
      await Promise.resolve(); // bất đồng bộ như gRPC thật: request đồng thời đều tới trước khi có kết quả
      if (fail) throw new Error('catalog down');
      return { event: event(eventId, status) };
    },
    listSeats: async () => {
      calls.listSeats++;
      return { seats: [] };
    },
  };
  return {
    calls,
    cache: new PublishedCatalogCache(client as unknown as CatalogClient),
    setStatus: (s: EventStatus) => (status = s),
    setFail: (f: boolean) => (fail = f),
  };
}

describe('PublishedCatalogCache', () => {
  it('caches a published event: later calls do not hit catalog', async () => {
    const f = fakeCatalog();
    await f.cache.getEvent('e1', md);
    await f.cache.getEvent('e1', md);
    await f.cache.getSeatMap('e1', md);
    await f.cache.getSeatMap('e1', md);
    expect(f.calls).toEqual({ getEvent: 2, listSeats: 1 }); // 1 cho getEvent, 1 cho getSeatMap
  });

  it('shares one catalog call between concurrent requests on a cold cache', async () => {
    const f = fakeCatalog();
    const results = await Promise.all(Array.from({ length: 50 }, () => f.cache.getSeatMap('e1', md)));
    expect(f.calls).toEqual({ getEvent: 1, listSeats: 1 });
    expect(new Set(results).size).toBe(1);
  });

  it('does not cache draft events', async () => {
    const f = fakeCatalog();
    f.setStatus(EventStatus.EVENT_STATUS_DRAFT);
    expect((await f.cache.getEvent('e1', md)).status).toBe(EventStatus.EVENT_STATUS_DRAFT);
    f.setStatus(EventStatus.EVENT_STATUS_PUBLISHED);
    expect((await f.cache.getEvent('e1', md)).status).toBe(EventStatus.EVENT_STATUS_PUBLISHED);
    expect(f.calls.getEvent).toBe(2);
    expect(f.cache.size).toBe(1);
  });

  it('does not cache errors', async () => {
    const f = fakeCatalog();
    f.setFail(true);
    await expect(f.cache.getEvent('e1', md)).rejects.toThrow('catalog down');
    f.setFail(false);
    await expect(f.cache.getEvent('e1', md)).resolves.toMatchObject({ id: 'e1' });
    expect(f.calls.getEvent).toBe(2);
  });

  it('bounds the number of entries, evicting the oldest', async () => {
    const f = fakeCatalog();
    for (let i = 0; i < 1005; i++) await f.cache.getEvent(`e${i}`, md);
    expect(f.cache.size).toBe(1000);
    await f.cache.getEvent('e1004', md); // mới nhất: còn trong cache
    expect(f.calls.getEvent).toBe(1005);
    await f.cache.getEvent('e0', md); // cũ nhất: đã bị bỏ
    expect(f.calls.getEvent).toBe(1006);
  });
});
