package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	catalogv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/catalog/v1"
	inventoryv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/inventory/v1"
)

// maxCachedEvents: số event giữ bảng giá trong bộ nhớ, quá thì bỏ event nạp sớm nhất. Một event
// vài nghìn ghế tốn cỡ vài trăm KB, 1024 event vẫn nằm trong giới hạn RAM của container.
const maxCachedEvents = 1024

// seatPrice là giá của một ghế (theo hạng vé của ghế đó).
type seatPrice struct {
	amount   int64
	currency string
}

// eventPrices: bảng giá theo ghế của một event, chỉ đọc sau khi tạo.
type eventPrices struct {
	seats     map[string]seatPrice
	published bool
}

// priceCache cache bảng giá của event ĐÃ PUBLISH.
//
// Vì sao cache được mà không cần TTL hay invalidate: sau khi publish, catalog không còn RPC nào sửa
// ghế/hạng vé/giá (GenerateSeats bị khóa, không có update/unpublish), nên bảng giá bất biến. Event
// chưa publish thì không cache (thực tế không có hold nào trên event draft, inventory chỉ được
// khởi tạo lúc publish).
//
// Vì sao cần: load test full_flow cho thấy mỗi CreateOrder gọi catalog GetEvent + ListSeats (cả sơ
// đồ ghế chỉ để lấy giá 1-2 ghế), catalog (Node 1 CPU) bão hòa và 95% đơn bị 504.
//
// singleflight: lúc mở bán cache còn trống mà hàng trăm đơn tới cùng lúc, chỉ một lời gọi catalog
// được thực hiện, các request còn lại chờ chung kết quả đó.
type priceCache struct {
	catalog catalogv1.CatalogServiceClient
	timeout time.Duration

	mu     sync.RWMutex
	events map[string]*eventPrices
	order  []string // thứ tự nạp, để bỏ event cũ nhất khi đầy
	group  singleflight.Group
}

func newPriceCache(catalog catalogv1.CatalogServiceClient, timeout time.Duration) *priceCache {
	return &priceCache{catalog: catalog, timeout: timeout, events: map[string]*eventPrices{}}
}

// get trả bảng giá của event: từ cache nếu có, không thì hỏi catalog (gộp các request trùng).
func (c *priceCache) get(ctx context.Context, eventID string) (*eventPrices, error) {
	c.mu.RLock()
	p := c.events[eventID]
	c.mu.RUnlock()
	if p != nil {
		return p, nil
	}

	ch := c.group.DoChan(eventID, func() (any, error) {
		// Context tách khỏi request dẫn đầu (giữ metadata, bỏ cancel/deadline): request đó bị hủy
		// thì các request đang chờ chung vẫn nhận được kết quả. Deadline riêng là CallTimeout.
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.timeout)
		defer cancel()
		p, err := c.fetch(fctx, eventID)
		if err != nil {
			return nil, err
		}
		if p.published {
			c.store(eventID, p)
		}
		return p, nil
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(*eventPrices), nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: catalog: %v", ErrUnavailable, ctx.Err())
	}
}

func (c *priceCache) fetch(ctx context.Context, eventID string) (*eventPrices, error) {
	var (
		ev    *catalogv1.GetEventResponse
		seats *catalogv1.ListSeatsResponse
		evErr error
		wg    sync.WaitGroup
	)
	// hai RPC độc lập, gọi song song để lần nạp cache không cộng dồn độ trễ
	wg.Add(1)
	go func() {
		defer wg.Done()
		ev, evErr = c.catalog.GetEvent(ctx, &catalogv1.GetEventRequest{EventId: eventID})
	}()
	// Đọc cả sơ đồ ghế của event rồi lập bảng: chỉ một lần mỗi event nhờ cache.
	seats, err := c.catalog.ListSeats(ctx, &catalogv1.ListSeatsRequest{EventId: eventID})
	wg.Wait()
	if evErr != nil {
		return nil, fmt.Errorf("%w: catalog.GetEvent: %v", ErrUnavailable, evErr)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: catalog.ListSeats: %v", ErrUnavailable, err)
	}

	tiers := map[string]*catalogv1.TicketTier{}
	for _, t := range ev.GetEvent().GetTiers() {
		tiers[t.GetId()] = t
	}
	p := &eventPrices{
		seats:     make(map[string]seatPrice, len(seats.GetSeats())),
		published: ev.GetEvent().GetStatus() == catalogv1.EventStatus_EVENT_STATUS_PUBLISHED,
	}
	for _, st := range seats.GetSeats() {
		if t, ok := tiers[st.GetTierId()]; ok {
			p.seats[st.GetId()] = seatPrice{amount: t.GetPriceAmount(), currency: t.GetCurrency()}
		}
	}
	return p, nil
}

func (c *priceCache) store(eventID string, p *eventPrices) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.events[eventID]; ok {
		return
	}
	c.events[eventID] = p
	c.order = append(c.order, eventID)
	for len(c.order) > maxCachedEvents {
		delete(c.events, c.order[0])
		c.order = c.order[1:]
	}
}

// price tính tổng tiền các ghế của hold theo giá hạng vé trong catalog (không tin giá từ client).
// Catalog chỉ cho một loại tiền mỗi event nên mọi hạng vé cùng currency.
func (s *Service) price(ctx context.Context, hold *inventoryv1.Hold) (int64, string, error) {
	prices, err := s.prices.get(ctx, hold.GetEventId())
	if err != nil {
		return 0, "", err
	}
	var (
		amount   int64
		currency string
	)
	for _, id := range hold.GetSeatIds() {
		sp, ok := prices.seats[id]
		if !ok {
			// Ghế đã được giữ ở inventory mà catalog không biết giá: dữ liệu hai bên lệch nhau.
			return 0, "", fmt.Errorf("catalog has no price for seat %s of event %s", id, hold.GetEventId())
		}
		amount += sp.amount
		currency = sp.currency
	}
	return amount, currency, nil
}
