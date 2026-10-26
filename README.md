# tap-unit4

Read-only Singer tap for the **Unit4 Wholesale REST API v1**, built with `hotglue_singer_sdk`.
It replaces the Unit4 side of the Nestinox Alumio integration (`Nestinox_Alumio.ndjson`);
endpoints and payloads follow that export, except purchase orders use a rolling date window.

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
| `stock_skip_inactive` | no | `false` | Skip stock calls for inactive products |
| `stock_batch_size` | no | `100` | Products per `requestProductStock` call |
| `unit4_product_stock_request_workers` | no | `10` | Concurrent `requestProductStock` requests (`1`–`32`) |
| `unit4_purchase_orders_request_workers` | no | `1` | Concurrent purchase-order page requests (`1`–`10`); omitted means sequential extraction |
| `page_size` / `max_pages` / `request_timeout` | no | `100` / `10000` / `300` | |

URL: `{base_url}/{environment}/rest/api/v1/{administration}/{resource}`.

## Streams

| Stream | Endpoint | Key | Replication |
|---|---|---|---|
| `products` | `GET /products?select=productClassification` | `productCode`, `unitCode` | FULL_TABLE |
| `product_stock` | `POST /requestProductStock`, up to `stock_batch_size` products per call | `productCode`, `unitCode`, `resultIndex` | FULL_TABLE |
| `sell_orders` | `GET /salesOrders?filter=orderDate gt <day> and status in (...)&select=lines` | `id` | INCREMENTAL on `orderDate` |
| `purchase_orders` | `GET /purchaseOrders?select=lines,supplierInfo`, plus `orderDate ge <day>` after bootstrap | `id` | FULL_TABLE transport with a custom earliest-unfinished-date bookmark |

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
- **Purchase orders:** the first run reads every page without a status or date
  filter, including historical and completed orders. Subsequent runs fetch all
  statuses with `orderDate ge <open_order_date>`. The boundary is the
  earliest calendar date among orders whose native `orderStatus` is `O` or `B`,
  not the maximum date seen. An order that just completed is emitted before the
  boundary advances; another unfinished order on the same day keeps it there.
  Empty or completed-only runs retain the previous boundary. Without a previous
  boundary and without unfinished orders, the next run is still a full scan.
  The custom bookmark is committed only after the record iterator is exhausted.
  Interim STATE messages retain the previous boundary; required-page failures,
  record-output failures and partial/test reads cannot advance it.
  Missing/blank header status or an invalid unfinished-order date aborts the scan.
  `9999-12-31` orders are emitted but never used as the boundary; a saved sentinel
  boundary aborts before HTTP requests.
  Singer discovery intentionally retains FULL_TABLE and no ordinary replication
  key: the SDK's maximum-date algorithm would skip older unfinished orders.
  Existing scalar, maximum-date and tracked-ID bookmarks do not seed the new
  boundary. Keep native IDs, dates, statuses, supplier info and lines unchanged.
  Every run starts at offset `1`; no detail lookups, status filters or additional
  requests are needed to calculate the boundary.
  This business scope follows unfinished orders through their final completion
  update, not edits to already-completed orders outside the window. Newly entered
  backdated orders before the boundary are outside this window too.
  The tap does not derive receipts, change delivered quantities or close Optiply orders.
  Fetch the first page alone, then prefetch at most the configured number of pages.
  Consume responses in offset order; parsing, pagination guards and Singer emission
  stay on the main thread. Up to `workers - 1` speculative pages beyond the end may
  already be requested; their records are never emitted.
  Missing/invalid IDs, duplicate IDs within or across pages, oversized pages and
  failures on required pages abort the scan rather than silently drop or deduplicate
  data. IDs are compared as strings and retained for the scan; buffered payloads are
  bounded by the worker count. Failed scans restart at offset `1` with the last successful boundary.
  Offset pagination still requires stable source ordering: duplicate detection cannot
  prove that no unseen orders were skipped if Unit4 changes the dataset during a run.
- **Deletes:** not exposed by the Alumio contract.

## Unverified against a live tenant

There is no public Unit4 Wholesale API spec; these remain unverified:

- the `orderDate` value format and whether results are ordered;
- meanings of status codes `A/Z/O` and `O/B`;
- whether `resultData` can contain more than one row per product (e.g. per warehouse);
- field availability and types outside the captured samples;
- the filtered purchase-order endpoint on v1 and Hotglue's end-to-end state
  acknowledgement after ETL/target success. Live v3 probes accepted date-only
  `orderDate ge YYYY-MM-DD`, including an order exactly on the boundary, and
  rejected datetime literals. Supplied v3 samples retained `orderDate` when it
  was omitted from `select`. This tap remains on v1 and requests only
  `select=lines,supplierInfo`; no API-version migration is included.

## Discovery and catalog fields

`--discover` renders the schemas declared in `tap_unit4/streams.py`; it does not
sample the API or infer new fields from responses. Purchase-order discovery declares
the 26 observed header attributes, plus nested supplier, line and delivery-line
attributes. The response field is `orderStatus`, not `OrderStatus`; the API's
filter-field spelling does not define the JSON response's casing.

The installed `hotglue_singer_sdk` 1.0.51 removes undeclared top-level attributes
before emitting Singer records, even when `additionalProperties` is true.
New response attributes therefore need explicit schema declarations.
Nested objects and arrays remain nested values, not separate top-level CSV columns.
The element schemas of the observed-empty `notes` and `priceAdjustments` arrays
remain open because their contents are not established by the sample.

Regenerate the catalog after upgrading the tap, then reapply stream/field selections:

```bash
.venv/bin/tap-unit4 --config /path/to/unit4-config.json --discover
```

## Development

```bash
uv venv -p 3.10 && uv pip install -e '.[test]'
.venv/bin/python3 -B -m pytest -q -p no:cacheprovider tests
.venv/bin/tap-unit4 --config config.json --discover
```

### Testing purchase-order state across syncs

Set `unit4_purchase_orders_request_workers` to `10` for threaded extraction and
select `purchase_orders` in the Singer catalog. No new configuration is needed.

1. Run with the normal saved state. If `open_order_date` is absent from
   the purchase-order bookmark, this run reads all purchase orders, irrespective
   of the sales-only `start_date`.
2. After a successful sync, inspect the final Singer STATE:

   ```json
   {
     "bookmarks": {
       "purchase_orders": {
         "open_order_date": "2026-09-10"
       }
     }
   }
   ```

   This synthetic example is a state fragment, not a replacement for other streams' state.
3. Pass the complete saved STATE to the next run (`--state` for local CLI runs).
   Every page should use `filter=orderDate ge 2026-09-10`. Keep the previous
   successful state if the overall ETL/target sync fails.
4. When all unfinished orders on the boundary date complete, their final updates
   must appear before the final STATE moves to the next unfinished date. If none
   remain, the boundary stays unchanged.

To deliberately rebuild purchase history, remove only the
`open_order_date` key from the input purchase-order bookmark; retain
other streams' state. Do not seed it with the latest order date.

The credential-free suite covers progression, date ties, failed-page resume,
failed record emission, partial reads and malformed/sentinel dates. A three-run
offline CLI smoke exercised HTTP pagination, saved Singer STATE and completion
updates against a synthetic local v1 endpoint. These do not prove live v1 or
Hotglue/ETL/target compatibility; those still need the approved sync test.

### Timing purchase-order extraction

Use a Singer catalog with stream-root metadata `selected: true` only for
`purchase_orders` and `selected: false` for the other streams. Generate a catalog with
`--discover` if needed. Time the extraction alone, without an ETL or target:

```bash
/usr/bin/time -p .venv/bin/tap-unit4 \
  --config /path/to/unit4-config.json \
  --catalog /path/to/purchase-orders.catalog.json > /dev/null
```

Do not use `--test`, which limits extraction and cannot commit a new boundary.
Omit `--state` for a full-history timing run; pass the same saved state for each
comparison of rolling-window extraction.
`select=lines,supplierInfo` remains enabled so timing includes the normal nested
payload. Existing `page_size`, `max_pages`, retry and timeout settings still apply;
a `max_pages` error means the scan did not finish. A live scan needs approval
for the source account and page budget before running it.

Set `unit4_purchase_orders_request_workers` to `1` and then `10` to compare serial
and concurrent extraction with the same catalog, saved state and `page_size`.
Repeated windows intentionally re-emit IDs; downstream writes still need
idempotent upserts. A successful tap scan alone does not prove target idempotency.
