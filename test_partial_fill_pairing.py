"""
Unit tests for FIFO greedy partial-fill aggregation pairing.

This validates the fix for the multi-fill double-count bug where:
- User opens 5 MARA PUT contracts on Mon, 3 on Tue, 2 on Wed (same strike/expiry)
- Tiger consolidates into ONE Expire event with qty=10
- Pairing must consume all 3 fills with that one event
- Total ARGUS realized P&L must equal Tiger's reported pl (no double-count)
"""
from __future__ import annotations

import sys
from types import SimpleNamespace
from datetime import date

# Test harness
class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.failures = []

    def expect(self, cond, name, detail=""):
        if cond:
            self.passed += 1
            print(f"  [PASS] {name}")
        else:
            self.failed += 1
            self.failures.append((name, detail))
            print(f"  [FAIL] {name}: {detail}")

    def report(self):
        print()
        print("=" * 70)
        print(f"RESULT: {self.passed} passed, {self.failed} failed")
        print("=" * 70)
        return self.failed == 0


# Helpers to build synthetic Tiger trades / events
def mk_open(ticker, right, strike, expiry, qty, price, dt, hash_, asset='Option'):
    return SimpleNamespace(
        ticker=ticker, right=right, strike=strike, expiry=expiry,
        quantity=qty, trade_price=price, trade_date=dt, settle_date=dt,
        row_hash=hash_, activity_type='OpenShort' if qty < 0 else 'Open',
        asset_class=asset, realized_pl=None, fee_total=0,
        source_file='test', source_row=0, symbol_raw=f'{ticker}',
        amount=qty * price * 100, currency='USD', notes='',
    )


def mk_close(ticker, right, strike, expiry, qty, price, dt, hash_, pl=None, fee=0):
    return SimpleNamespace(
        ticker=ticker, right=right, strike=strike, expiry=expiry,
        quantity=qty, trade_price=price, trade_date=dt, settle_date=dt,
        row_hash=hash_, activity_type='Close',
        asset_class='Option', realized_pl=pl, fee_total=fee,
        source_file='test', source_row=0, symbol_raw=f'{ticker}',
        amount=qty * price * 100, currency='USD', notes='',
    )


def mk_expire(ticker, right, strike, expiry, qty, dt, hash_, pl, ttype='Option Expire'):
    return SimpleNamespace(
        ticker=ticker, right=right, strike=strike, expiry=expiry,
        quantity=qty, event_date=dt, transaction_type=ttype,
        row_hash=hash_, asset_class='Option', realized_pl=pl,
        source_file='test', source_row=0, symbol_raw=f'{ticker}',
        currency='USD',
    )


# ─────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────
opt_key = lambda t: (t.ticker, t.right, t.strike, t.expiry)


def test_simple_one_to_one(t, _build_partial_pairs):
    """Baseline: 1 open + 1 close, exact qty match."""
    print("\n--- TEST 1: 1 open + 1 close, exact match ---")
    o1 = mk_open('MARA', 'PUT', 11.0, date(2025, 5, 8), -5, 0.40, date(2025, 5, 1), 'o1')
    c1 = mk_close('MARA', 'PUT', 11.0, date(2025, 5, 8), 5, 0.05, date(2025, 5, 6), 'c1', pl=175.0)

    pairs, open_remaining = _build_partial_pairs([o1], [c1], opt_key)
    t.expect(len(pairs) == 1, "1 pair produced")
    t.expect(pairs[0]['qty'] == 5, f"qty=5, got {pairs[0]['qty']}")
    t.expect(abs(pairs[0]['pl_share'] - 175.0) < 0.01, f"pl_share=$175 (got {pairs[0]['pl_share']})")
    t.expect(open_remaining[0] == 0, f"open fully consumed (got {open_remaining[0]})")


def test_multi_open_one_close(t, _build_partial_pairs):
    """The bug case: 3 OpenShort fills + 1 Expire event aggregated."""
    print("\n--- TEST 2: 3 opens + 1 expire (multi-fill aggregation) ---")
    o1 = mk_open('MARA', 'PUT', 11.0, date(2025, 5, 8), -5, 0.40, date(2025, 5, 1), 'o1')
    o2 = mk_open('MARA', 'PUT', 11.0, date(2025, 5, 8), -3, 0.45, date(2025, 5, 2), 'o2')
    o3 = mk_open('MARA', 'PUT', 11.0, date(2025, 5, 8), -2, 0.50, date(2025, 5, 3), 'o3')
    expire = mk_expire('MARA', 'PUT', 11.0, date(2025, 5, 8), 10, date(2025, 5, 8), 'e1', pl=335.0)

    pairs, open_remaining = _build_partial_pairs([o1, o2, o3], [expire], opt_key)

    t.expect(len(pairs) == 3, f"3 pairs (one per open), got {len(pairs)}")
    t.expect(all(q == 0 for q in open_remaining), f"all opens consumed (remaining={open_remaining})")
    if len(pairs) == 3:
        # FIFO order
        t.expect(pairs[0]['open'].row_hash == 'o1', "first pair = o1")
        t.expect(pairs[0]['qty'] == 5, f"o1 consumed=5, got {pairs[0]['qty']}")
        t.expect(pairs[1]['open'].row_hash == 'o2', "second pair = o2")
        t.expect(pairs[1]['qty'] == 3, f"o2 consumed=3, got {pairs[1]['qty']}")
        t.expect(pairs[2]['open'].row_hash == 'o3', "third pair = o3")
        t.expect(pairs[2]['qty'] == 2, f"o3 consumed=2, got {pairs[2]['qty']}")
        # PL must allocate proportionally; total must equal Tiger's reported pl
        total_pl = sum(p['pl_share'] for p in pairs)
        t.expect(abs(total_pl - 335.0) < 0.01, f"sum of pl_shares=$335 (got ${total_pl:.4f})")
        # Proportional allocation: 5/10, 3/10, 2/10 of $335
        t.expect(abs(pairs[0]['pl_share'] - 167.5) < 0.01, f"o1 pl=167.5 (5/10), got {pairs[0]['pl_share']}")
        t.expect(abs(pairs[1]['pl_share'] - 100.5) < 0.01, f"o2 pl=100.5 (3/10), got {pairs[1]['pl_share']}")
        t.expect(abs(pairs[2]['pl_share'] - 67.0) < 0.01, f"o3 pl=67 (2/10), got {pairs[2]['pl_share']}")


def test_one_open_multi_close(t, _build_partial_pairs):
    """Reverse: one big open closed in pieces by 2 closes."""
    print("\n--- TEST 3: 1 open + 2 closes (split close) ---")
    o1 = mk_open('SPY', 'CALL', 600.0, date(2025, 12, 19), -10, 5.00, date(2025, 11, 1), 'o1')
    c1 = mk_close('SPY', 'CALL', 600.0, date(2025, 12, 19), 6, 1.00, date(2025, 12, 1), 'c1', pl=240.0)
    c2 = mk_close('SPY', 'CALL', 600.0, date(2025, 12, 19), 4, 0.50, date(2025, 12, 10), 'c2', pl=180.0)

    pairs, open_remaining = _build_partial_pairs([o1], [c1, c2], opt_key)

    t.expect(len(pairs) == 2, f"2 pairs (one per close), got {len(pairs)}")
    t.expect(open_remaining[0] == 0, f"open fully consumed (got {open_remaining[0]})")
    if len(pairs) == 2:
        t.expect(pairs[0]['qty'] == 6, f"first pair qty=6, got {pairs[0]['qty']}")
        t.expect(abs(pairs[0]['pl_share'] - 240.0) < 0.01, "first pair pl=240")
        t.expect(pairs[1]['qty'] == 4, f"second pair qty=4, got {pairs[1]['qty']}")
        t.expect(abs(pairs[1]['pl_share'] - 180.0) < 0.01, "second pair pl=180")


def test_partial_remainder_stays_open(t, _build_partial_pairs):
    """User opened 10, closed 6, remaining 4 should stay open."""
    print("\n--- TEST 4: partial close, remainder open ---")
    o1 = mk_open('MARA', 'PUT', 10.0, date(2026, 6, 19), -10, 0.30, date(2026, 4, 1), 'o1')
    c1 = mk_close('MARA', 'PUT', 10.0, date(2026, 6, 19), 6, 0.10, date(2026, 4, 15), 'c1', pl=120.0)

    pairs, open_remaining = _build_partial_pairs([o1], [c1], opt_key)
    t.expect(len(pairs) == 1, "1 pair")
    t.expect(pairs[0]['qty'] == 6, "consumed 6")
    t.expect(open_remaining[0] == 4, f"remainder qty=4 on open[0] (got {open_remaining[0]})")


def test_close_unpaired_remainder(t, _build_partial_pairs):
    """Close has more qty than available opens — orphan."""
    print("\n--- TEST 5: close > opens (orphan) ---")
    o1 = mk_open('MARA', 'PUT', 9.0, date(2025, 8, 8), -3, 0.20, date(2025, 7, 1), 'o1')
    c1 = mk_close('MARA', 'PUT', 9.0, date(2025, 8, 8), 5, 0.05, date(2025, 8, 1), 'c1', pl=75.0)

    pairs, open_remaining = _build_partial_pairs([o1], [c1], opt_key)
    # 1 normal pair (consumed=3) + 1 orphan pair (qty=2)
    t.expect(len(pairs) == 2, f"2 pairs (1 normal + 1 orphan), got {len(pairs)}")
    if len(pairs) >= 1:
        t.expect(pairs[0]['qty'] == 3, "first pair qty=3")
    if len(pairs) >= 2:
        t.expect(pairs[1].get('orphan') is True, "second pair flagged orphan")
        t.expect(pairs[1]['qty'] == 2, "orphan qty=2")
    t.expect(open_remaining[0] == 0, "open fully consumed")


def test_chained_passes_carry_remaining_state(t, _build_partial_pairs):
    """Pass 1 (closes) leaves partial open; Pass 2 (exercises) consumes the rest."""
    print("\n--- TEST 6: chained passes (close + exercise) ---")
    o1 = mk_open('MARA', 'PUT', 12.0, date(2025, 6, 13), -8, 0.50, date(2025, 5, 1), 'o1')
    c1 = mk_close('MARA', 'PUT', 12.0, date(2025, 6, 13), 5, 0.10, date(2025, 5, 20), 'c1', pl=200.0)
    expire = mk_expire('MARA', 'PUT', 12.0, date(2025, 6, 13), 3, date(2025, 6, 13), 'e1', pl=120.0)

    # Pass 1: opens × closes
    pairs_close, open_remaining = _build_partial_pairs([o1], [c1], opt_key)
    # Pass 2: opens × exercises (with carryover state)
    pairs_ex, open_remaining = _build_partial_pairs([o1], [expire], opt_key, open_remaining=open_remaining)

    t.expect(len(pairs_close) == 1, "pass 1: 1 close pair")
    t.expect(len(pairs_ex) == 1, "pass 2: 1 expire pair")
    if pairs_close:
        t.expect(pairs_close[0]['qty'] == 5, "close pair qty=5")
        t.expect(abs(pairs_close[0]['pl_share'] - 200.0) < 0.01, "close pair pl=200")
    if pairs_ex:
        t.expect(pairs_ex[0]['qty'] == 3, "expire pair qty=3")
        t.expect(abs(pairs_ex[0]['pl_share'] - 120.0) < 0.01, "expire pair pl=120")
    t.expect(open_remaining[0] == 0, f"open fully consumed (got {open_remaining[0]})")


def test_no_double_count_invariant(t, _build_partial_pairs):
    """The CRITICAL invariant: total PL across all pairs MUST equal sum of close events' realized_pl."""
    print("\n--- TEST 7: no double-count invariant ---")
    # Realistic 3-week scenario
    o1 = mk_open('MARA', 'PUT', 11.0, date(2025, 5, 8), -5, 0.40, date(2025, 5, 1), 'o1')
    o2 = mk_open('MARA', 'PUT', 11.0, date(2025, 5, 8), -3, 0.45, date(2025, 5, 2), 'o2')
    o3 = mk_open('MARA', 'PUT', 11.0, date(2025, 5, 8), -2, 0.50, date(2025, 5, 3), 'o3')
    o4 = mk_open('MARA', 'CALL', 13.0, date(2025, 5, 8), -7, 0.35, date(2025, 5, 1), 'o4')
    expire1 = mk_expire('MARA', 'PUT', 11.0, date(2025, 5, 8), 10, date(2025, 5, 8), 'e1', pl=335.0)
    expire2 = mk_expire('MARA', 'CALL', 13.0, date(2025, 5, 8), 7, date(2025, 5, 8), 'e2', pl=245.0)

    pairs, open_remaining = _build_partial_pairs([o1, o2, o3, o4], [expire1, expire2], opt_key)

    total_pl = sum(p['pl_share'] for p in pairs)
    expected_total = 335.0 + 245.0
    t.expect(abs(total_pl - expected_total) < 0.01,
             f"total PL across all pairs = ${total_pl:.4f}, expected ${expected_total} (Tiger sum)")
    t.expect(all(q == 0 for q in open_remaining), f"all opens consumed (got {open_remaining})")
    t.expect(len(pairs) == 4, f"4 pairs (3 for PUT + 1 for CALL), got {len(pairs)}")


def test_stock_buy_sell_pairing(t, _build_partial_pairs):
    """Stock pairing with same algorithm: buy 100, buy 50, sell 60, sell 90."""
    print("\n--- TEST 8: stock buy/sell partial-fill ---")
    stk_key = lambda t: t.ticker
    b1 = mk_open('MARA', None, None, None, 100, 15.0, date(2025, 4, 1), 'b1', asset='Stock')
    b1.activity_type = 'Open'
    b2 = mk_open('MARA', None, None, None, 50, 16.0, date(2025, 4, 5), 'b2', asset='Stock')
    b2.activity_type = 'Open'
    s1 = mk_close('MARA', None, None, None, -60, 17.0, date(2025, 5, 1), 's1', pl=120.0)
    s1.activity_type = 'Close'
    s1.asset_class = 'Stock'
    s2 = mk_close('MARA', None, None, None, -90, 17.5, date(2025, 5, 10), 's2', pl=187.5)
    s2.activity_type = 'Close'
    s2.asset_class = 'Stock'

    pairs, open_remaining = _build_partial_pairs([b1, b2], [s1, s2], stk_key)

    # s1 (60) consumes 60 from b1 (remainder 40). s2 (90) consumes 40 from b1 + 50 from b2.
    t.expect(len(pairs) == 3, f"3 pairings expected (s1<->b1, s2<->b1 partial, s2<->b2), got {len(pairs)}")
    t.expect(all(q == 0 for q in open_remaining), f"all stock consumed (got {open_remaining})")
    if len(pairs) == 3:
        total_pl = sum(p['pl_share'] for p in pairs)
        t.expect(abs(total_pl - 307.5) < 0.01, f"total stock PL = $307.5 (got ${total_pl:.4f})")


def main():
    # Import the function we're testing (defined in tiger_etl)
    from tiger_etl import _build_partial_pairs

    t = TestRunner()

    test_simple_one_to_one(t, _build_partial_pairs)
    test_multi_open_one_close(t, _build_partial_pairs)
    test_one_open_multi_close(t, _build_partial_pairs)
    test_partial_remainder_stays_open(t, _build_partial_pairs)
    test_close_unpaired_remainder(t, _build_partial_pairs)
    test_chained_passes_carry_remaining_state(t, _build_partial_pairs)
    test_no_double_count_invariant(t, _build_partial_pairs)
    test_stock_buy_sell_pairing(t, _build_partial_pairs)

    success = t.report()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
