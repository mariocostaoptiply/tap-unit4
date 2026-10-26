# Nestinox ETL migration reference

This document records the behavior that must be considered when replacing the supplied
Nestinox Alumio configuration with a HotGlue ETL. It is derived from the 48-record Alumio
export and its environment-variable inventory. Credential values and raw customer payloads
are intentionally excluded.

The Singer tap only extracts Unit4 records. Target writes, transformations, schedules, and
Alumio storage behavior belong in the ETL or deployment configuration.

## Source coverage

The export contains four Unit4 reads. All four have corresponding tap streams; no other
Unit4 source endpoint appears in the export.

| Data | Alumio source | Tap stream | Alumio schedule |
|---|---|---|---|
| Products | `GET /products?select=productClassification` | `products` | Nightly at `03:00` |
| Stock | `POST /requestProductStock` inside the product mapper | `product_stock` | Intended standalone schedule every two hours from `07:17` through `17:17`, `Europe/Amsterdam`; exported route incomplete |
| Sell orders | `GET /salesOrders`, statuses `A/Z/O`, with `lines` | `sell_orders` | Hourly at minutes `13` and `42` |
| Purchase orders | `GET /purchaseOrders`, statuses `O/B`, with `lines,supplierInfo` | `purchase_orders` | Every 20 minutes at `:04`, `:24`, and `:44` |

Only the stock schedule declares a timezone. The effective timezone of the other Alumio
schedules is not present in the export.

The current tap uses configurable paging rather than Alumio's fixed limits. Stock requests
use `stock_batch_size`, defaulting to `100`, and up to
`unit4_product_stock_request_workers` concurrent HTTP requests, defaulting to `10`. A live
sequential probe returned one result per requested product with `productCode` nested under
`productIdentifier` and `unitCode` at the top level. The tap flattens that nested product
code and validates the complete identity set before emitting records. At most one missing
identity may be inferred from the sole unmatched request; ambiguous responses abort. The
probe returned `100` results for `100` products in `0.705` seconds.

## Product eligibility and mapping

### Eligibility

Keep a Unit4 product only when at least one `productClassification` entry has a
`productGroupCode` in:

- `10A`
- `20B`
- `30C`
- `40D`
- `50E`

Alumio discarded products whose filtered classification list was empty.

### Product fields

| Unit4 input | Optiply output | Rule |
|---|---|---|
| `productCode`, `unitCode` | `skuCode` | Join with `-` |
| `description` | `name` | Direct mapping |
| `eanCode` | `eanCode` | Direct mapping |
| `minimalPurchase` | `minimumPurchaseQuantity`, `lotSize` | Both must be at least `1` |
| `inactive` | `status` | `false` → `enabled`; `true` → `disabled` |
| `discontinued` | `notBeingBought` | Direct mapping |
| `costPrice` | `price` | Divide by `100`, then format to two decimals |
| `minimumStock` | `minimumStock` | Direct mapping |
| `maximumStock` | `maximumStock` | Include only when greater than `0`; otherwise `null` |
| Unit4 stock result | `stockLevel` | `resultData[0].availableStockForSales`; default to `0` in the outgoing payload |

Alumio also set:

- `accountId` from configuration;
- `unlimitedStock = false`;
- `manualServiceLevel = null` by default;
- `manualServiceLevel = 0` when classification group `30C` or `40D` is present;
- `articleCode` from the `productGroupCode` of a classification whose
  `productCategoryCode` is `INK`.

Important correction: groups `30C` and `40D` control `manualServiceLevel`; they do not set
`notBeingBought`. `notBeingBought` comes from Unit4 `discontinued`.

### Product upsert and supplier products

Alumio used stored `skuCode` → Optiply product-ID data, falling back to an Optiply product
lookup by `skuCode` when the ID was absent. It then selected:

- `POST /products` for a new product;
- `PATCH /products/{id}` for an existing product.

After the product write, Alumio:

1. Retrieved supplier products by `skuCode` and patched their `articleCode`.
2. Retrieved every Optiply supplier.
3. Looked up the product/supplier association.
4. Created a missing supplier product with name, price, status, EAN, lot size, SKU,
   account, product ID, supplier ID, article code, minimum purchase quantity, and defaults
   for preferred, delivery time, and not-being-bought.
5. Patched an existing supplier product's price and availability.

A transformer named for `WitWay` is nevertheless an active dependency of the Nestinox
product route. It enforces the minimum purchase quantity and lot-size floors and formats
price. Reimplement its behavior, not its customer-specific name.

## Sell-order mapping

Alumio filtered sell orders through a consumed-order storage keyed by `orderCode`, then
mapped:

| Unit4 input | Optiply output | Rule |
|---|---|---|
| `id` | internal `unit4_id` | Used for result storage |
| `orderDate` | `placed`, `completed` | Format as UTC timestamp |
| `orderAmount` | `totalValue` | Direct mapping |
| `lines[].productIdentifier.productCode` + `lines[].unitCode` | `orderLines[].productId` | Join with `-`, then resolve through the SKU-to-Optiply-ID storage |
| `lines[].qtyOrdered` | `orderLines[].quantity` | Direct mapping |
| `lines[].nettNettLineAmount` | `orderLines[].subtotalValue` | Direct mapping |

Before posting to `/sellOrders`, Alumio removed lines that did not resolve to a numeric
Optiply product ID and skipped orders left with no lines.

The replacement should use replay-safe target upserts or equivalent state rather than copy
Alumio's consumed-record storage blindly. Confirm the target's natural key before deciding
how `orderCode`, Unit4 `id`, and `orderDate` participate in deduplication.

## Stock flow

The Unit4 stock request is fully visible in the product mapper:

- `quantity = 1`;
- one or more `{unitCode, productIdentifier: {productCode}}` entries;
- `propertiesToInclude = "*"`.

The supplied export does not contain a complete standalone stock route. Its stock scheduler
invokes an incoming configuration that only reads Optiply `/products`; no exported route
then calls Unit4 and patches stock back to Optiply. Treat that workflow as incomplete until
runtime evidence or another export identifies the missing route.

Alumio's standalone stock source started from Optiply products. The tap starts from Unit4
products. The ETL must therefore join or filter stock records against accepted Optiply
products so excluded Unit4 products are not updated downstream.

## Purchase orders

Alumio read open purchase orders with `lines` and `supplierInfo` and identified each entity
by `id`. The supplied export contains no route or transformer that maps those records to an
Optiply API write.

A scheduled reader exists for a purchase-order-line mapping storage, but the export contains
no writer for that storage. Before implementing purchase-order ETL behavior, establish:

- whether purchase orders were only retained in Alumio or consumed by an omitted workflow;
- which order and line fields Optiply needs;
- the target key and close/delete semantics when an order leaves statuses `O/B`;
- who writes and reads the Unit4-line-ID → Optiply-buy-order-ID mapping.

This branch reads all purchase orders in a paginated full-table scan with
`select=lines,supplierInfo` and no status or date filter. It starts from the first
page on every run, ignores previous tracking state and performs no per-order
follow-up lookups. Native IDs and nested payloads are preserved.
Buy-order completion and cumulative receipts from native line IDs and
`quantityDelivered` remain ETL responsibilities, not tap transformations.
For timing, run purchase-order extraction alone; do not feed a full historical
scan into the ETL or target as part of the benchmark.

## Optiply-only maintenance flow

Alumio also ran a job every 15 minutes that:

1. Read incomplete, manually created Optiply buy orders older than 30 minutes.
2. Patched each order's `completed` timestamp to the current time.

This is not a Unit4 tap responsibility. Decide explicitly whether the replacement ETL or
another owned service must retain it.

## Alumio operational behavior

Additional schedules and storage behavior from the export:

- Sell-order route drain: every two minutes from `:01` through `:59`.
- Product route drain: every two minutes from `:10` through `:49`.
- Product-route prune job: hourly at `:10`, removing finished/skipped route data older than
  one minute.
- Purchase-order-line storage reader: every two hours at `:49`; its writer is absent.
- Consumed-product and consumed-sales-order storages prevented repeat processing.
- A SKU storage held the Unit4 SKU, Optiply product ID, and status used by product and
  sell-order transformations.

HotGlue scheduling and replay-safe writes should replace these queue/storage mechanics;
they do not need to reproduce Alumio's implementation structure.

## Export ambiguities to resolve

Do not silently choose behavior for these points:

1. The product mapper builds `skuCode` from `productCode` and `unitCode`, but also maps
   `externalProductCode` to the same target before joining. Confirm the actual Alumio output
   or the intended canonical SKU.
2. The existing-product branch contains a key filter for `stockLevel`; confirm whether it
   removed or retained stock during product patches.
3. The standalone stock route is absent.
4. The purchase-order target route and purchase-order-line storage writer are absent.
5. The environment inventory contains dummy-supplier and supplier-suffix variables that are
   not referenced by the supplied export.
6. Status codes `A/Z/O` and `O/B` are configured but their business meanings are not defined.
7. Non-stock schedules inherit an Alumio timezone that is not included in the export.

## ETL acceptance checklist

### Products and stock

- [ ] Filter products by the five accepted classification groups.
- [ ] Confirm the canonical SKU when `externalProductCode` is present.
- [ ] Apply all product mappings and numeric defaults above.
- [ ] Keep `discontinued` and classification-driven `manualServiceLevel` separate.
- [ ] Upsert products replay-safely.
- [ ] Update stock only for accepted/mapped products.
- [ ] Define whether product upserts include stock or stock runs own that field exclusively.
- [ ] Recreate supplier-product article-code and all-supplier association behavior if still
      required.

### Sell orders

- [ ] Resolve each line to an Optiply product ID.
- [ ] Drop unresolved lines and empty orders with observable counts.
- [ ] Preserve quantity, subtotal, total, placed, and completed semantics.
- [ ] Define a replay-safe order key and verify a second run does not duplicate orders.

### Purchase orders

- [ ] Obtain the missing target contract or confirm that extraction alone is sufficient.
- [ ] Define order-close behavior when records disappear from the open-order snapshot.
- [ ] Resolve ownership of the purchase-order-line mapping.

### Operations

- [ ] Configure HotGlue schedules with explicit timezones.
- [ ] Validate a full default-size stock run and its final partial batch.
- [ ] Verify sell-order state and lookback with two consecutive runs.
- [ ] Confirm the status-code meanings with Nestinox or Unit4 documentation.
- [ ] Exercise product, stock, sell-order, and purchase-order transforms with sanitized
      captured fixtures and captured target writes.
- [ ] Decide whether to retain the manual-buy-order closure job.

## Remaining source-contract risks

The Alumio export does not establish:

- the exact `orderDate` format or ordering guarantee;
- whether every tenant/version echoes stock identities;
- whether a product can return multiple stock rows, for example by warehouse;
- field types beyond the mapped fields;
- an upper stock batch limit above the probed size of `200`;
- Unit4 rate limits.

Keep these as explicit validation items rather than encoding assumptions in the ETL.
