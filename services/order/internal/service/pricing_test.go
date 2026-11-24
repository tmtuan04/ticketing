// Unit test cho cache bảng giá (pricing.go): không cần container, chạy được với -short.
package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	catalogv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/catalog/v1"
	inventoryv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/inventory/v1"
)

// countingCatalog trả một event 2 hạng vé (VIP 2.000.000, STD 800.000) và đếm số lần gọi.
type countingCatalog struct {
	catalogv1.CatalogServiceClient
	status    atomic.Int32
	fail      atomic.Bool
	delay     time.Duration
	getEvents atomic.Int32
	listSeats atomic.Int32
}

func newCountingCatalog() *countingCatalog {
	c := &countingCatalog{}
	c.status.Store(int32(catalogv1.EventStatus_EVENT_STATUS_PUBLISHED))
	return c
}

func (c *countingCatalog) GetEvent(ctx context.Context, in *catalogv1.GetEventRequest, _ ...grpc.CallOption) (*catalogv1.GetEventResponse, error) {
	c.getEvents.Add(1)
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if c.fail.Load() {
		return nil, errors.New("catalog down")
	}
	return &catalogv1.GetEventResponse{Event: &catalogv1.Event{
		Id:     in.GetEventId(),
		Status: catalogv1.EventStatus(c.status.Load()),
		Tiers: []*catalogv1.TicketTier{
			{Id: "vip", PriceAmount: 2_000_000, Currency: "VND"},
			{Id: "std", PriceAmount: 800_000, Currency: "VND"},
		},
	}}, nil
}

func (c *countingCatalog) ListSeats(context.Context, *catalogv1.ListSeatsRequest, ...grpc.CallOption) (*catalogv1.ListSeatsResponse, error) {
	c.listSeats.Add(1)
	return &catalogv1.ListSeatsResponse{Seats: []*catalogv1.Seat{
		{Id: "a1", TierId: "vip"}, {Id: "b1", TierId: "std"}, {Id: "b2", TierId: "std"},
	}}, nil
}

func newPricingService(cat catalogv1.CatalogServiceClient) *Service {
	return &Service{prices: newPriceCache(cat, time.Second)}
}

func hold(event string, seats ...string) *inventoryv1.Hold {
	return &inventoryv1.Hold{EventId: event, SeatIds: seats}
}

func TestPriceSumsTierPrices(t *testing.T) {
	s := newPricingService(newCountingCatalog())
	amount, currency, err := s.price(context.Background(), hold("e1", "a1", "b1", "b2"))
	if err != nil || amount != 3_600_000 || currency != "VND" {
		t.Fatalf("price = %d %s %v, want 3600000 VND", amount, currency, err)
	}
	if _, _, err := s.price(context.Background(), hold("e1", "zz")); err == nil {
		t.Fatal("seat without price: want error")
	}
}

func TestPriceCachesPublishedEvent(t *testing.T) {
	cat := newCountingCatalog()
	s := newPricingService(cat)
	for range 5 {
		if _, _, err := s.price(context.Background(), hold("e1", "a1")); err != nil {
			t.Fatal(err)
		}
	}
	if cat.getEvents.Load() != 1 || cat.listSeats.Load() != 1 {
		t.Fatalf("catalog calls = %d GetEvent, %d ListSeats; want 1, 1", cat.getEvents.Load(), cat.listSeats.Load())
	}
}

func TestPriceDoesNotCacheDraftOrErrors(t *testing.T) {
	cat := newCountingCatalog()
	s := newPricingService(cat)

	cat.status.Store(int32(catalogv1.EventStatus_EVENT_STATUS_DRAFT))
	if _, _, err := s.price(context.Background(), hold("e1", "a1")); err != nil {
		t.Fatal(err)
	}
	cat.status.Store(int32(catalogv1.EventStatus_EVENT_STATUS_PUBLISHED))
	cat.fail.Store(true)
	if _, _, err := s.price(context.Background(), hold("e1", "a1")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("catalog down: err = %v, want ErrUnavailable", err)
	}
	cat.fail.Store(false)
	for range 2 {
		if _, _, err := s.price(context.Background(), hold("e1", "a1")); err != nil {
			t.Fatal(err)
		}
	}
	// draft (1) + lỗi (1) + lần nạp thành công (1); lần cuối lấy từ cache
	if got := cat.getEvents.Load(); got != 3 {
		t.Fatalf("GetEvent calls = %d, want 3", got)
	}
}

// Đúng lúc mở bán: cache trống, nhiều đơn cùng lúc chỉ được tạo ra MỘT lần gọi catalog.
func TestPriceCoalescesConcurrentMisses(t *testing.T) {
	cat := newCountingCatalog()
	cat.delay = 50 * time.Millisecond
	s := newPricingService(cat)

	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := s.price(context.Background(), hold("e1", "a1")); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if cat.getEvents.Load() != 1 {
		t.Fatalf("GetEvent calls = %d, want 1", cat.getEvents.Load())
	}
}

// Request dẫn đầu bị hủy giữa chừng không được làm hỏng request khác đang chờ cùng kết quả.
func TestPriceLeaderCancellationDoesNotFailFollowers(t *testing.T) {
	cat := newCountingCatalog()
	cat.delay = 100 * time.Millisecond
	s := newPricingService(cat)

	leaderCtx, cancel := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, _, err := s.price(leaderCtx, hold("e1", "a1"))
		leaderErr <- err
	}()
	time.Sleep(20 * time.Millisecond) // để leader bắt đầu lời gọi catalog trước
	followerErr := make(chan error, 1)
	go func() {
		_, _, err := s.price(context.Background(), hold("e1", "a1"))
		followerErr <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()

	if err := <-leaderErr; !errors.Is(err, ErrUnavailable) {
		t.Fatalf("leader err = %v, want ErrUnavailable (context canceled)", err)
	}
	if err := <-followerErr; err != nil {
		t.Fatalf("follower err = %v, want nil", err)
	}
	if cat.getEvents.Load() != 1 {
		t.Fatalf("GetEvent calls = %d, want 1", cat.getEvents.Load())
	}
}

func TestPriceCacheEvictsOldest(t *testing.T) {
	cat := newCountingCatalog()
	s := newPricingService(cat)
	for i := range maxCachedEvents + 1 {
		if _, _, err := s.price(context.Background(), hold(fmt.Sprintf("e%d", i), "a1")); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(s.prices.events); n != maxCachedEvents {
		t.Fatalf("cached events = %d, want %d", n, maxCachedEvents)
	}
	before := cat.getEvents.Load()
	_, _, _ = s.price(context.Background(), hold(fmt.Sprintf("e%d", maxCachedEvents), "a1")) // mới nhất: hit
	_, _, _ = s.price(context.Background(), hold("e0", "a1"))                                 // cũ nhất: đã bị bỏ
	if got := cat.getEvents.Load() - before; got != 1 {
		t.Fatalf("GetEvent calls after eviction = %d, want 1", got)
	}
}
