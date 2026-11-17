import type { SeatView, Tier } from '../api';
import { money, seatLabel } from '../lib';

type Props = {
  seats: SeatView[];
  tiers?: Tier[];
  selected?: Set<string>;
  /** ghế thuộc hold/đơn của chính user: viền xanh dương */
  mine?: Set<string>;
  /** ghế vừa bị từ chối (409 unavailableSeatIds): nhấp nháy đỏ */
  rejected?: Set<string>;
  onToggle?: (seat: SeatView) => void;
};

const STATUS_TEXT: Record<string, string> = {
  available: 'Còn trống',
  held: 'Đang giữ',
  sold: 'Đã bán',
  unknown: 'Không rõ',
};

/** Sơ đồ ghế: mỗi section một khối, mỗi hàng một dòng, màu theo trạng thái từ inventory. */
export function SeatMap({ seats, tiers = [], selected, mine, rejected, onToggle }: Props) {
  const tierById = new Map(tiers.map((t) => [t.id, t]));
  const sections = new Map<string, Map<number, SeatView[]>>();
  for (const s of seats) {
    const rows = sections.get(s.section) ?? new Map<number, SeatView[]>();
    const row = rows.get(s.row) ?? [];
    row.push(s);
    rows.set(s.row, row);
    sections.set(s.section, rows);
  }
  const sectionNames = [...sections.keys()].sort();

  const counts = { available: 0, held: 0, sold: 0 } as Record<string, number>;
  for (const s of seats) counts[s.status] = (counts[s.status] ?? 0) + 1;

  return (
    <div className="seatmap">
      <div className="legend">
        <span><i className="seat-dot available" /> Trống <b>{counts.available ?? 0}</b></span>
        <span><i className="seat-dot held" /> Đang giữ <b>{counts.held ?? 0}</b></span>
        <span><i className="seat-dot sold" /> Đã bán <b>{counts.sold ?? 0}</b></span>
        {selected && <span><i className="seat-dot selected" /> Đang chọn</span>}
        {mine && <span><i className="seat-dot mine" /> Của bạn</span>}
      </div>
      <div className="stage">SÂN KHẤU</div>
      {sectionNames.length === 0 && <p className="muted">Event chưa có ghế.</p>}
      {sectionNames.map((name) => {
        const rows = sections.get(name)!;
        const rowNums = [...rows.keys()].sort((a, b) => a - b);
        const tier = tierById.get(rows.get(rowNums[0])![0].tierId);
        return (
          <div className="section" key={name}>
            <div className="section-title">
              Khu {name}
              {tier && (
                <span className="muted"> · {tier.name} · {money(tier.priceAmount, tier.currency)}</span>
              )}
            </div>
            {rowNums.map((r) => (
              <div className="seat-row" key={r}>
                <span className="row-label">{r}</span>
                {rows
                  .get(r)!
                  .sort((a, b) => a.number - b.number)
                  .map((s) => {
                    const cls = [
                      'seat',
                      s.status,
                      selected?.has(s.seatId) ? 'is-selected' : '',
                      mine?.has(s.seatId) ? 'is-mine' : '',
                      rejected?.has(s.seatId) ? 'is-rejected' : '',
                    ].join(' ');
                    const clickable = onToggle && (s.status === 'available' || selected?.has(s.seatId));
                    return (
                      <button
                        key={s.seatId}
                        className={cls}
                        disabled={!clickable}
                        onClick={() => onToggle?.(s)}
                        title={`${seatLabel(s)} · ${tierById.get(s.tierId)?.name ?? ''} · ${STATUS_TEXT[s.status]}`}
                      >
                        {s.number}
                      </button>
                    );
                  })}
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}
