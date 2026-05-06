# Tiger API Discovery — 2026-05-05 12:04

Account: `50179929`  License: `TBSG`  Sandbox: `False`


## 1. get_assets() — NAV & cash

**Method:** `get_assets`
**Returned:** 1 item(s)

**Type:** `PortfolioAccount`

### Example 1
```
account                        = '50179929'
market_values                  = defaultdict(<class 'tigeropen.trade.domain.account.MarketValue'>, {})
segments                       = defaultdict(<class 'tigeropen.trade.domain.account.Account'>, {'S': SecuritySegment({'accrued_cash': -1391.46, 'accrued_dividend': 0.0, 'available_funds': 120639.02, 'cash': 38354.64, 'equity_with_loa...
summary                        = Account({'accrued_cash': inf, 'accrued_dividend': inf, 'available_funds': inf, 'buying_power': 482556.07, 'cash': 38354.64, 'currency': 'USD', 'cushion': inf, 'day_trades_remaining': inf, 'equity_with...

  Nested .summary:
    accrued_cash                   = inf
    accrued_dividend               = inf
    available_funds                = inf
    buying_power                   = 482556.07
    cash                           = 38354.64
    currency                       = 'USD'
    cushion                        = inf
    day_trades_remaining           = inf
    equity_with_loan               = inf
    excess_liquidity               = inf
    gross_position_value           = inf
    initial_margin_requirement     = inf
    maintenance_margin_requirement = inf
    net_liquidation                = 330913.69
    realized_pnl                   = 0.0
    regt_equity                    = inf
    regt_margin                    = inf
    sma                            = inf
    timestamp                      = None
    unrealized_pnl                 = -76387.75
```


## 2. get_prime_assets() — Margin / buying power detail

**Method:** `get_prime_assets`
**Returned:** 1 item(s)

**Type:** `PortfolioAccount`

### Example 1
```
account                        = '50179929'
segments                       = {'S': Segment({'currency': 'USD', 'capability': 'RegTMargin', 'category': 'S', 'cash_balance': 38354.64, 'cash_available_for_trade': 120639.02, 'gross_position_value': 293950.51, 'equity_with_loan': 2...
update_timestamp               = 1777953879420
```


## 3. get_aggregate_assets() — Multi-currency totals


[FAIL] code=1010 msg=biz param error(only support institution account)


## 4. get_filled_orders() — Filled orders (last 30 days)

**Method:** `get_filled_orders`
**Returned:** 75 item(s)

**Type:** `Order`

### Example 1
```
account                        = '50179929'
action                         = 'SELL'
active                         = False
adjust_limit                   = None
algo_params                    = None
algo_strategy                  = 'LMT'
attr_desc                      = None
attr_list                      = []
aux_price                      = None
avg_fill_price                 = 0.5
can_modify                     = False
charges                        = None
combo_type                     = None
combo_type_desc                = None
commission                     = 5.54
contract                       = MARA  260605C00014000/OPT/USD
contract_legs                  = None
discount                       = 0
expire_time                    = None
external_id                    = '1777910542.309911'
filled                         = 8
filled_cash_amount             = 400.0
filled_scale                   = 0
gst                            = 0.5
id                             = 43124175490121728
is_open                        = True
latest_price                   = None
limit_price                    = 0.5
liquidation                    = False
order_id                       = 0
order_legs                     = None
order_time                     = 1777910548000
order_type                     = 'LMT'
orders                         = None
outside_rth                    = False
parent_id                      = None
percent_offset                 = None
quantity                       = 8
quantity_scale                 = 0
realized_pnl                   = 0.0
reason                         = ''
refund_cash_amount             = None
remaining                      = 0
secret_key                     = ''
source                         = 'iOS'
status                         = <OrderStatus.FILLED: 'Filled'>
sub_ids                        = None
time_in_force                  = 'DAY'
total_cash_amount              = None
trade_time                     = 1777910549000
trading_session_type           = 'RTH'
trail_stop_price               = None
trailing_percent               = None
update_time                    = 1777910549000
user_mark                      = ''

  Nested .contract:
    categories                     = None
    close_only                     = None
    continuous                     = None
    contract_id                    = None
    contract_month                 = None
    currency                       = 'USD'
    discounted_day_initial_margin  = None
    discounted_day_maintenance_margin = None
    discounted_end_at              = None
    discounted_start_at            = None
    discounted_time_zone_code      = None
    etf_leverage                   = None
    exchange                       = None
    expiry                         = '20260605'
    first_notice_date              = None
    identifier                     = 'MARA  260605C00014000'
    is_etf                         = None
    last_bidding_close_time        = None
    last_trading_date              = None
    local_symbol                   = None
    long_initial_margin            = None
    long_maintenance_margin        = None
    lot_size                       = None
    marginable                     = None
    market                         = 'US'
    min_tick                       = None
    multiplier                     = None
    name                           = None
    origin_symbol                  = None
    primary_exchange               = None
    put_call                       = 'CALL'
    right                          = 'CALL'
    sec_type                       = 'OPT'
    short_fee_rate                 = None
    short_initial_margin           = None
    short_maintenance_margin       = None
    short_margin                   = None
    shortable                      = None
    shortable_count                = None
    status                         = None
    strike                         = '14.0'
    support_fractional_share       = None
    support_overnight_trading      = None
    symbol                         = 'MARA'
    tick_sizes                     = None
    trade                          = None
    trading_class                  = None
    underlying_contract_name       = None
```
### Example 2
```
account                        = '50179929'
action                         = 'SELL'
active                         = False
adjust_limit                   = None
algo_params                    = None
algo_strategy                  = 'LMT'
attr_desc                      = None
attr_list                      = []
aux_price                      = None
avg_fill_price                 = 7.8
can_modify                     = False
charges                        = None
combo_type                     = None
combo_type_desc                = None
commission                     = 0.72
contract                       = CRCL  260605C00135000/OPT/USD
contract_legs                  = None
discount                       = 0
expire_time                    = None
external_id                    = '1777910476.493764'
filled                         = 1
filled_cash_amount             = 780.0
filled_scale                   = 0
gst                            = 0.06
id                             = 43124167064816640
is_open                        = True
latest_price                   = None
limit_price                    = 7.8
liquidation                    = False
order_id                       = 0
order_legs                     = None
order_time                     = 1777910484000
order_type                     = 'LMT'
orders                         = None
outside_rth                    = False
parent_id                      = None
percent_offset                 = None
quantity                       = 1
quantity_scale                 = 0
realized_pnl                   = 0.0
reason                         = ''
refund_cash_amount             = None
remaining                      = 0
secret_key                     = ''
source                         = 'iOS'
status                         = <OrderStatus.FILLED: 'Filled'>
sub_ids                        = None
time_in_force                  = 'DAY'
total_cash_amount              = None
trade_time                     = 1777911453000
trading_session_type           = 'RTH'
trail_stop_price               = None
trailing_percent               = None
update_time                    = 1777911453000
user_mark                      = ''

  Nested .contract:
    categories                     = None
    close_only                     = None
    continuous                     = None
    contract_id                    = None
    contract_month                 = None
    currency                       = 'USD'
    discounted_day_initial_margin  = None
    discounted_day_maintenance_margin = None
    discounted_end_at              = None
    discounted_start_at            = None
    discounted_time_zone_code      = None
    etf_leverage                   = None
    exchange                       = None
    expiry                         = '20260605'
    first_notice_date              = None
    identifier                     = 'CRCL  260605C00135000'
    is_etf                         = None
    last_bidding_close_time        = None
    last_trading_date              = None
    local_symbol                   = None
    long_initial_margin            = None
    long_maintenance_margin        = None
    lot_size                       = None
    marginable                     = None
    market                         = 'US'
    min_tick                       = None
    multiplier                     = None
    name                           = None
    origin_symbol                  = None
    primary_exchange               = None
    put_call                       = 'CALL'
    right                          = 'CALL'
    sec_type                       = 'OPT'
    short_fee_rate                 = None
    short_initial_margin           = None
    short_maintenance_margin       = None
    short_margin                   = None
    shortable                      = None
    shortable_count                = None
    status                         = None
    strike                         = '135.0'
    support_fractional_share       = None
    support_overnight_trading      = None
    symbol                         = 'CRCL'
    tick_sizes                     = None
    trade                          = None
    trading_class                  = None
    underlying_contract_name       = None
```


## 5. get_orders() — All orders, last 30 days

**Method:** `get_orders`
**Returned:** 10 item(s)

**Type:** `Order`

### Example 1
```
account                        = '50179929'
action                         = 'SELL'
active                         = False
adjust_limit                   = None
algo_params                    = None
algo_strategy                  = 'LMT'
attr_desc                      = None
attr_list                      = []
aux_price                      = None
avg_fill_price                 = 0.5
can_modify                     = False
charges                        = None
combo_type                     = None
combo_type_desc                = None
commission                     = 5.54
contract                       = MARA  260605C00014000/OPT/USD
contract_legs                  = None
discount                       = 0
expire_time                    = None
external_id                    = '1777910542.309911'
filled                         = 8
filled_cash_amount             = 400.0
filled_scale                   = 0
gst                            = 0.5
id                             = 43124175490121728
is_open                        = True
latest_price                   = None
limit_price                    = 0.5
liquidation                    = False
order_id                       = 0
order_legs                     = None
order_time                     = 1777910548000
order_type                     = 'LMT'
orders                         = None
outside_rth                    = False
parent_id                      = None
percent_offset                 = None
quantity                       = 8
quantity_scale                 = 0
realized_pnl                   = 0.0
reason                         = ''
refund_cash_amount             = None
remaining                      = 0
secret_key                     = ''
source                         = 'iOS'
status                         = <OrderStatus.FILLED: 'Filled'>
sub_ids                        = None
time_in_force                  = 'DAY'
total_cash_amount              = None
trade_time                     = 1777910549000
trading_session_type           = 'RTH'
trail_stop_price               = None
trailing_percent               = None
update_time                    = 1777910549000
user_mark                      = ''

  Nested .contract:
    categories                     = None
    close_only                     = None
    continuous                     = None
    contract_id                    = None
    contract_month                 = None
    currency                       = 'USD'
    discounted_day_initial_margin  = None
    discounted_day_maintenance_margin = None
    discounted_end_at              = None
    discounted_start_at            = None
    discounted_time_zone_code      = None
    etf_leverage                   = None
    exchange                       = None
    expiry                         = '20260605'
    first_notice_date              = None
    identifier                     = 'MARA  260605C00014000'
    is_etf                         = None
    last_bidding_close_time        = None
    last_trading_date              = None
    local_symbol                   = None
    long_initial_margin            = None
    long_maintenance_margin        = None
    lot_size                       = None
    marginable                     = None
    market                         = 'US'
    min_tick                       = None
    multiplier                     = None
    name                           = None
    origin_symbol                  = None
    primary_exchange               = None
    put_call                       = 'CALL'
    right                          = 'CALL'
    sec_type                       = 'OPT'
    short_fee_rate                 = None
    short_initial_margin           = None
    short_maintenance_margin       = None
    short_margin                   = None
    shortable                      = None
    shortable_count                = None
    status                         = None
    strike                         = '14.0'
    support_fractional_share       = None
    support_overnight_trading      = None
    symbol                         = 'MARA'
    tick_sizes                     = None
    trade                          = None
    trading_class                  = None
    underlying_contract_name       = None
```
### Example 2
```
account                        = '50179929'
action                         = 'SELL'
active                         = False
adjust_limit                   = None
algo_params                    = None
algo_strategy                  = 'LMT'
attr_desc                      = None
attr_list                      = []
aux_price                      = None
avg_fill_price                 = 7.8
can_modify                     = False
charges                        = None
combo_type                     = None
combo_type_desc                = None
commission                     = 0.72
contract                       = CRCL  260605C00135000/OPT/USD
contract_legs                  = None
discount                       = 0
expire_time                    = None
external_id                    = '1777910476.493764'
filled                         = 1
filled_cash_amount             = 780.0
filled_scale                   = 0
gst                            = 0.06
id                             = 43124167064816640
is_open                        = True
latest_price                   = None
limit_price                    = 7.8
liquidation                    = False
order_id                       = 0
order_legs                     = None
order_time                     = 1777910484000
order_type                     = 'LMT'
orders                         = None
outside_rth                    = False
parent_id                      = None
percent_offset                 = None
quantity                       = 1
quantity_scale                 = 0
realized_pnl                   = 0.0
reason                         = ''
refund_cash_amount             = None
remaining                      = 0
secret_key                     = ''
source                         = 'iOS'
status                         = <OrderStatus.FILLED: 'Filled'>
sub_ids                        = None
time_in_force                  = 'DAY'
total_cash_amount              = None
trade_time                     = 1777911453000
trading_session_type           = 'RTH'
trail_stop_price               = None
trailing_percent               = None
update_time                    = 1777911453000
user_mark                      = ''

  Nested .contract:
    categories                     = None
    close_only                     = None
    continuous                     = None
    contract_id                    = None
    contract_month                 = None
    currency                       = 'USD'
    discounted_day_initial_margin  = None
    discounted_day_maintenance_margin = None
    discounted_end_at              = None
    discounted_start_at            = None
    discounted_time_zone_code      = None
    etf_leverage                   = None
    exchange                       = None
    expiry                         = '20260605'
    first_notice_date              = None
    identifier                     = 'CRCL  260605C00135000'
    is_etf                         = None
    last_bidding_close_time        = None
    last_trading_date              = None
    local_symbol                   = None
    long_initial_margin            = None
    long_maintenance_margin        = None
    lot_size                       = None
    marginable                     = None
    market                         = 'US'
    min_tick                       = None
    multiplier                     = None
    name                           = None
    origin_symbol                  = None
    primary_exchange               = None
    put_call                       = 'CALL'
    right                          = 'CALL'
    sec_type                       = 'OPT'
    short_fee_rate                 = None
    short_initial_margin           = None
    short_maintenance_margin       = None
    short_margin                   = None
    shortable                      = None
    shortable_count                = None
    status                         = None
    strike                         = '135.0'
    support_fractional_share       = None
    support_overnight_trading      = None
    symbol                         = 'CRCL'
    tick_sizes                     = None
    trade                          = None
    trading_class                  = None
    underlying_contract_name       = None
```


## 6. get_open_orders() — Currently working orders

**Method:** `get_open_orders`
**Returned:** 0 item(s)

_(empty — endpoint may be unavailable for this license/account)_


## 7. get_transactions() — Per-fill executions with FEES ⭐


[FAIL] code=1010 msg=biz param error(field 'symbol' cannot be empty)


## 8. get_funding_history() — Deposits / withdrawals


[FAIL] The truth value of a DataFrame is ambiguous. Use a.empty, a.bool(), a.item(), a.any() or a.all().


## 9. get_segment_fund_history() — Securities ↔ Futures transfers

**Method:** `get_segment_fund_history`
**Returned:** 3 item(s)

**Type:** `SegmentFundItem`

### Example 1
```
amount                         = 250566.0
created_at                     = 1775004930000
currency                       = 'SGD'
from_segment                   = 'FUND'
id                             = 42743330341388288
message                        = None
settled_at                     = 1775004931000
status                         = 'SUCC'
status_desc                    = 'SUCCESS'
to_segment                     = 'SEC'
updated_at                     = 1775004930000
```
### Example 2
```
amount                         = 25.32
created_at                     = 1773103614000
currency                       = 'SGD'
from_segment                   = 'FUND'
id                             = 42494121004892160
message                        = None
settled_at                     = 1773103614000
status                         = 'SUCC'
status_desc                    = 'SUCCESS'
to_segment                     = 'SEC'
updated_at                     = 1773103614000
```


## 10. get_analytics_asset() — Historical NAV curve

**Method:** `get_analytics_asset`
**Returned:** 1 item(s)

**Type:** `dict`

### Example 1
```
(no public attrs — dict)
```