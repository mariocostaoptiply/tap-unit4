# tap-unit4

Read-only Singer tap for the **Unit4 Wholesale REST API v1**, built with `hotglue_singer_sdk`.
It replaces the Unit4 side of the Nestinox Alumio integration (`Nestinox_Alumio.ndjson`);
endpoints, filters and payloads mirror that export.

## Config

| Key | Required | Default | Notes |
|---|---|---|---|
| `base_url` | yes | | Alumio `UNIT4_BASE_URI` |
| `environment` | yes | | Alumio `UNIT4_ENVIRONMENT` (path segment) |
| `administration` | no | `01` | Company code in the path |
| `username` / `password` | yes | | HTTP Basic auth |
| `start_date` | no | today | First sell-order day when there is no bookmark |
| `sell_order_lookback_days` | no | `1` | Days before the bookmark day re-read each run |
| `sell_order_statuses` | no | `A,Z,O` | `salesOrders` `status` filter |
| `purchase_order_statuses` | no | `O,B` | `purchaseOrders` `OrderStatus` filter |
| `stock_skip_inactive` | no | `false` | Skip stock calls for inactive products |
| `stock_batch_size` | no | `100` | Products per `requestProductStock` call |
| `unit4_product_stock_request_workers` | no | `10` | Concurrent `requestProductStock` requests (`1`–`32`) |
| `page_size` / `max_pages` / `request_timeout` | no | `100` / `10000` / `300` | |

URL: `{base_url}/{environment}/rest/api/v1/{administration}/{resource}`.

## Streams

| Stream | Endpoint | Key | Replication |
|---|---|---|---|
| `products` | `GET /products?select=productClassification` | `productCode`, `unitCode` | FULL_TABLE |
| `product_stock` | `POST /requestProductStock`, up to `stock_batch_size` products per call | `productCode`, `unitCode`, `resultIndex` | FULL_TABLE |
| `sell_orders` | `GET /salesOrders?filter=orderDate gt <day> and status in (...)&select=lines` | `id` | INCREMENTAL on `orderDate` |
| `purchase_orders` | `GET /purchaseOrders?filter=OrderStatus in (...)&select=lines,supplierInfo`, then `GET /purchaseOrders/{id}?select=lines,supplierInfo` for missing tracked orders | `id` | FULL_TABLE plus tracked-order follow-up |

Optiply `skuCode` is `<productCode>-<unitCode>`; sell-order lines join to products on
`lines[].productIdentifier.productCode` + `lines[].unitCode`. Alumio divided `costPrice` by 100
and read stock from `resultData[0].availableStockForSales`.

Business rules Alumio applied in transformers are **not** in the tap and belong in the ETL:
accepted product groups (`10A`, `20B`, `30C`, `40D`, `50E`), `manualServiceLevel = 0`
for groups `30C`/`40D`, minimum purchase and lot-size floors of `1`, price conversion,
status/discontinued mapping, target upserts, supplier products, and sell-order line resolution.
See the complete [Nestinox ETL migration reference](ETL_REFERENCE.md).

## Behaviour

- **Auth:** Basic. 401 raises `InvalidCredentialsError` without retry; 403 and other 4xx are fatal.
- **Retries:** 408, 429 and 5xx, 5 tries with exponential backoff; `Retry-After` honoured up to 120s.
- **Pagination:** list endpoints use one-based `offset` values (`1`, `1 + limit`, ...); stops
  on a short, empty or 404 page. A repeated page or `max_pages` aborts the sync instead of looping.
- **Stock:** products are optionally filtered by `stock_skip_inactive`, then requested in
  batches of `stock_batch_size`, with at most `unit4_product_stock_request_workers` HTTP
  requests in flight. Unit4 returns `productCode` inside `productIdentifier`; the tap
  flattens it, then validates each response identity before emitting records. At most one
  omitted identity can be inferred from the sole unmatched request; ambiguous responses
  abort. Only HTTP execution runs in worker threads; parsing, validation and Singer emission
  remain on the main thread.
- **State:** `sell_orders` is unsorted, so the bookmark (max `orderDate`) is committed only when
  the stream completes. Each run re-reads from bookmark day minus lookback, so expect
  duplicates on `id`.
  Orders dated `9999-12-31` (including timestamps on that day) are emitted unchanged
  but excluded from bookmark advancement. A sentinel-only run keeps the existing
  bookmark, or creates no bookmark on a first run. A saved sentinel bookmark or
  sentinel `start_date` aborts before HTTP requests. To recover poisoned state,
  restore the last-good sales bookmark, or clear only `sell_orders` state and set
  a deliberate historical `start_date`; without either, the tap starts from today.
- **Purchase orders:** finish the entire filtered, paginated scan first. Track observed
  nonterminal order IDs, then fetch previously tracked IDs absent from that scan by ID,
  without a status filter or pagination. Both phases emit the same `purchase_orders`
  records with native IDs, `orderDate`, header status, supplier info and nested lines.
  The tap does not derive receipts, change delivered quantities or close Optiply orders.
- **Purchase state:** `bookmarks.purchase_orders` stores `tracked_order_ids` (strings)
  and `source_scope` (the normalized Unit4 REST base URL, including environment and
  administration). Only header `orderStatus` values `T` and `Z` retire tracking; terminal
  line statuses alone do not. Other header statuses remain tracked. Tracking changes
  are published only after every page, lookup and record emission succeeds.
  Missing, malformed or mismatched detail responses fail the stream without retiring
  IDs; a detail 404 is never interpreted as completion or end of pagination.
  State from another source, or nonempty tracking without a source scope, fails before
  requests. An empty initial state tracks currently observed orders only, not historical
  closed orders. This flow relies on HotGlue promoting state only after end-to-end job
  success; a failed ETL must resume from the previous successful job state.
- **Deletes:** not exposed by the Alumio contract.

## Unverified against a live tenant

There is no public Unit4 Wholesale API spec; these remain unverified:

- the `orderDate` value format and whether results are ordered;
- meanings of status codes `A/Z/O` and `O/B`;
- whether `resultData` can contain more than one row per product (e.g. per warehouse);
- field types beyond those Alumio mapped (schemas allow additional properties).

## Development

```bash
uv venv -p 3.10 && uv pip install -e '.[test]'
.venv/bin/python -m pytest -q tests
.venv/bin/tap-unit4 --config config.json --discover
```
