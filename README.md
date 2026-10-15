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
| `page_size` / `max_pages` / `request_timeout` | no | `100` / `10000` / `300` | |

URL: `{base_url}/{environment}/rest/api/v1/{administration}/{resource}`.

## Streams

| Stream | Endpoint | Key | Replication |
|---|---|---|---|
| `products` | `GET /products?select=productClassification` | `productCode`, `unitCode` | FULL_TABLE |
| `product_stock` | `POST /requestProductStock`, one call per product | `productCode`, `unitCode`, `resultIndex` | FULL_TABLE |
| `sell_orders` | `GET /salesOrders?filter=orderDate gt <day> and status in (...)&select=lines` | `id` | INCREMENTAL on `orderDate` |
| `purchase_orders` | `GET /purchaseOrders?filter=OrderStatus in (...)&select=lines,supplierInfo` | `id` | FULL_TABLE (open-order snapshot) |

Optiply `skuCode` is `<productCode>-<unitCode>`; sell-order lines join to products on
`lines[].productIdentifier.productCode` + `lines[].unitCode`. Alumio divided `costPrice` by 100
and read stock from `resultData[0].availableStockForSales`.

Business rules Alumio applied in transformers are **not** in the tap and belong in the ETL:
product-group filter (`10A`–`50E`), not-being-bought from groups `30C`/`40D`, minimum purchase
quantity floor of 1, and status/discontinued mapping.

## Behaviour

- **Auth:** Basic. 401 raises `InvalidCredentialsError` without retry; 403 and other 4xx are fatal.
- **Retries:** 408, 429 and 5xx, 5 tries with exponential backoff; `Retry-After` honoured up to 120s.
- **Pagination:** list endpoints use one-based `offset` values (`1`, `1 + limit`, ...); stops
  on a short, empty or 404 page. A repeated page or `max_pages` aborts the sync instead of looping.
- **State:** `sell_orders` is unsorted, so the bookmark (max `orderDate`) is committed only when
  the stream completes. Each run re-reads from bookmark day minus lookback, so expect
  duplicates on `id`.
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
