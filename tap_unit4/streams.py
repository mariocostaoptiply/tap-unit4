"""Unit4 Wholesale stream definitions.

Endpoints and filters mirror the Nestinox Alumio integration this tap replaces:

  products         GET  /products?select=productClassification          FULL_TABLE
  product_stock    POST /requestProductStock (batched products)         FULL_TABLE
  sell_orders      GET  /salesOrders?filter=orderDate gt ... &select=lines  INCREMENTAL (orderDate)
  purchase_orders  GET  /purchaseOrders?filter=OrderStatus in (...)&select=lines,supplierInfo  FULL_TABLE
"""

from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
import re
import typing as t
from datetime import date, datetime, timedelta, timezone
from itertools import islice

from hotglue_singer_sdk.exceptions import FatalAPIError

from tap_unit4.client import Unit4Stream

STATUS_CODE = re.compile(r"^[A-Za-z0-9]{1,10}$")
MAX_PRODUCT_STOCK_REQUEST_WORKERS = 32


def nullable(*type_names: str) -> list[str]:
    """Return a nullable JSON schema type list."""
    return [*type_names, "null"]


NUMBER = {"type": nullable("number")}
STRING = {"type": nullable("string")}
BOOLEAN = {"type": nullable("boolean")}


def obj(properties: dict | None = None) -> dict:
    """Nullable open object schema; Unit4 payloads carry many undocumented fields."""
    return {
        "type": nullable("object"),
        "additionalProperties": True,
        "properties": properties or {},
    }


def array_of(item_schema: dict) -> dict:
    """Nullable array schema."""
    return {"type": nullable("array"), "items": item_schema}


def status_filter(field: str, raw_codes: t.Any, default: str) -> str:
    """Build `<field> in ('A','B')` from a comma-separated config value."""
    if isinstance(raw_codes, list):
        codes = [str(c).strip() for c in raw_codes]
    else:
        codes = [c.strip() for c in str(raw_codes or default).split(",")]
    codes = [c for c in codes if c]
    invalid = [c for c in codes if not STATUS_CODE.match(c)]
    if invalid or not codes:
        raise FatalAPIError(f"Invalid status codes for {field}: {invalid or raw_codes!r}")
    quoted = ",".join(f"'{c}'" for c in codes)
    return f"{field} in ({quoted})"


class ProductsStream(Unit4Stream):
    """Unit4 products. Identity is productCode + unitCode (Optiply skuCode = '<productCode>-<unitCode>')."""

    name = "products"
    path = "/products"
    primary_keys = ["productCode", "unitCode"]
    replication_key = None

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "productCode": {"type": "string"},
            "unitCode": {"type": "string"},
            "description": STRING,
            "externalProductCode": STRING,
            "eanCode": STRING,
            "minimalPurchase": NUMBER,
            "inactive": BOOLEAN,
            "discontinued": BOOLEAN,
            # Alumio divided costPrice by 100 before sending it to Optiply.
            "costPrice": NUMBER,
            "minimumStock": NUMBER,
            "maximumStock": NUMBER,
            "productClassification": array_of(
                obj(
                    {
                        "productGroupCode": STRING,
                        "productCategoryCode": STRING,
                    }
                )
            ),
        },
        "required": ["productCode", "unitCode"],
    }

    def extra_url_params(self, context) -> dict[str, t.Any]:
        return {"select": "productClassification"}

    def post_process(self, row: dict, context=None) -> dict | None:
        """Drop rows without a stable identity instead of emitting broken keys."""
        if row.get("productCode") in (None, "") or row.get("unitCode") in (None, ""):
            self.logger.warning(
                "Skipping Unit4 product without productCode/unitCode: %s",
                {k: row.get(k) for k in ("productCode", "unitCode", "description")},
            )
            return None
        row["productCode"] = str(row["productCode"])
        row["unitCode"] = str(row["unitCode"])
        return row


class ProductStockStream(Unit4Stream):
    """Available stock per product via POST /requestProductStock.

    Products are requested in configurable batches through a bounded HTTP request
    pool so stock can run more often than the full product sync.
    """

    name = "product_stock"
    path = "/requestProductStock"
    rest_method = "POST"
    paginated = False
    primary_keys = ["productCode", "unitCode", "resultIndex"]
    replication_key = None

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "productCode": {"type": "string"},
            "unitCode": {"type": "string"},
            "resultIndex": {"type": "integer"},
            "availableStockForSales": NUMBER,
        },
        "required": ["productCode", "unitCode", "resultIndex"],
    }

    @property
    def stock_batch_size(self) -> int:
        """Return the configured number of products per stock request."""
        raw_size = self.config.get("stock_batch_size", 100)
        if isinstance(raw_size, bool):
            raise FatalAPIError("stock_batch_size must be a positive integer.")
        try:
            size = int(raw_size)
        except (TypeError, ValueError) as exc:
            raise FatalAPIError(
                "stock_batch_size must be a positive integer."
            ) from exc
        if size < 1:
            raise FatalAPIError("stock_batch_size must be a positive integer.")
        return size

    @property
    def stock_request_workers(self) -> int:
        """Return the bounded number of concurrent Unit4 stock requests."""
        config_key = "unit4_product_stock_request_workers"
        raw_workers = self.config.get(config_key, 5)
        error = (
            f"{config_key} must be an integer between 1 and "
            f"{MAX_PRODUCT_STOCK_REQUEST_WORKERS}."
        )
        if isinstance(raw_workers, bool):
            raise FatalAPIError(error)
        try:
            workers = int(raw_workers)
        except (TypeError, ValueError) as exc:
            raise FatalAPIError(error) from exc
        if not 1 <= workers <= MAX_PRODUCT_STOCK_REQUEST_WORKERS:
            raise FatalAPIError(error)
        return workers

    def prepare_request_payload(self, context, next_page_token) -> dict:
        return {
            "quantity": 1,
            "productIdentifiers": [
                {
                    "unitCode": product["unitCode"],
                    "productIdentifier": {"productCode": product["productCode"]},
                }
                for product in context["products"]
            ],
            "propertiesToInclude": "*",
        }

    def parse_response(self, response) -> t.Iterable[dict]:
        payload = self._json(response)
        if isinstance(payload, list) and not payload:
            return
        if not isinstance(payload, dict):
            raise FatalAPIError(
                f"Unit4 {self.path} returned {type(payload).__name__}, expected an object."
            )
        results = payload.get("resultData") or []
        if not isinstance(results, list):
            raise FatalAPIError(f"Unit4 {self.path} resultData is not a list.")
        if any(not isinstance(result, dict) for result in results):
            raise FatalAPIError(
                f"Unit4 {self.path} resultData contains a non-object item."
            )
        yield from results

    def _validate_batch_results(
        self, products: list[dict], results: list[dict]
    ) -> t.Iterable[dict]:
        """Require exactly one correlated stock row per requested product."""
        requested = [
            (product["productCode"], product["unitCode"]) for product in products
        ]
        if len(set(requested)) != len(requested):
            raise FatalAPIError(
                f"Unit4 {self.path} request batch contains duplicate product identities."
            )

        returned = []
        for result in results:
            product_code = result.get("productCode")
            unit_code = result.get("unitCode")
            if (
                not isinstance(product_code, str)
                or not product_code
                or not isinstance(unit_code, str)
                or not unit_code
            ):
                raise FatalAPIError(
                    f"Unit4 {self.path} result is missing string productCode/unitCode."
                )
            returned.append((product_code, unit_code))

        if (
            len(results) != len(products)
            or len(set(returned)) != len(returned)
            or set(returned) != set(requested)
        ):
            raise FatalAPIError(
                f"Unit4 {self.path} result identities do not match the requested "
                f"batch (requested={len(products)}, returned={len(results)}, "
                f"unique={len(set(returned))})."
            )

        for result in results:
            yield {**result, "resultIndex": 0}

    def _iter_products(self) -> t.Iterable[dict]:
        products = ProductsStream(tap=self._tap)
        for row in products.request_records(context=None):
            product = products.post_process(row)
            if product is not None:
                yield product

    def get_records(self, context) -> t.Iterable[dict]:
        skip_inactive = bool(self.config.get("stock_skip_inactive", False))
        batch_size = self.stock_batch_size
        workers = self.stock_request_workers
        products = (
            {
                "productCode": product["productCode"],
                "unitCode": product["unitCode"],
            }
            for product in self._iter_products()
            if not (skip_inactive and product.get("inactive") is True)
        )
        decorated_request = self.request_decorator(self._request)
        pending = deque()
        products_exhausted = False

        with ThreadPoolExecutor(max_workers=workers) as executor:
            try:
                while pending or not products_exhausted:
                    while not products_exhausted and len(pending) < workers:
                        batch = list(islice(products, batch_size))
                        if not batch:
                            products_exhausted = True
                            break
                        request_context = {"products": batch}
                        prepared_request = self.prepare_request(request_context, None)
                        future = executor.submit(
                            decorated_request, prepared_request, None
                        )
                        pending.append((batch, prepared_request, future))

                    if not pending:
                        break

                    batch, prepared_request, future = pending.popleft()
                    response = future.result()
                    self.update_sync_costs(prepared_request, response, None)
                    results = list(self.parse_response(response))
                    yield from self._validate_batch_results(batch, results)
            finally:
                for _, _, future in pending:
                    future.cancel()


class SellOrdersStream(Unit4Stream):
    """Unit4 sales orders with nested lines, incremental on orderDate.

    The filter is `orderDate gt <date>`, day-granular. Each run re-reads from the
    bookmark day minus `sell_order_lookback_days`, so records may repeat across runs;
    downstream upserts on `id`.
    """

    name = "sell_orders"
    path = "/salesOrders"
    primary_keys = ["id"]
    replication_key = "orderDate"

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "id": {"type": ["string", "integer"]},
            "orderCode": STRING,
            "orderDate": {"type": "string"},
            "status": STRING,
            "orderAmount": NUMBER,
            "lines": array_of(
                obj(
                    {
                        "productIdentifier": obj({"productCode": STRING}),
                        "unitCode": STRING,
                        "qtyOrdered": NUMBER,
                        "nettNettLineAmount": NUMBER,
                    }
                )
            ),
        },
        "required": ["id", "orderDate"],
    }

    def post_process(self, row: dict, context=None) -> dict:
        """Reject replication markers that cannot be resumed on the next run."""
        marker = row.get("orderDate")
        if not isinstance(marker, str) or not marker.strip():
            raise FatalAPIError("Unit4 sell order is missing a valid orderDate.")
        try:
            _parse_day(marker)
        except FatalAPIError as exc:
            raise FatalAPIError("Unit4 sell order has an invalid orderDate.") from exc
        return row

    @property
    def lookback_days(self) -> int:
        try:
            return max(int(self.config.get("sell_order_lookback_days", 1)), 0)
        except (TypeError, ValueError):
            return 1

    def _from_date(self, context) -> date:
        """Day to filter strictly after: (bookmark or start_date) minus lookback."""
        marker = self.get_starting_replication_key_value(context)
        if marker is None:
            state = self.get_context_state(context)
            if state.get("replication_key") == self.replication_key:
                marker = state.get("replication_key_value")
        marker = marker or self.config.get("start_date")
        if marker:
            start = _parse_day(marker)
        else:
            start = datetime.now(timezone.utc).date()
        # `gt` excludes the given day, so subtract one more to include the start day.
        return start - timedelta(days=self.lookback_days + 1)

    def extra_url_params(self, context) -> dict[str, t.Any]:
        statuses = status_filter(
            "status", self.config.get("sell_order_statuses"), "A,Z,O"
        )
        from_day = self._from_date(context).isoformat()
        return {
            "filter": f"orderDate gt {from_day} and {statuses}",
            "select": "lines",
        }


class PurchaseOrdersStream(Unit4Stream):
    """Unit4 open purchase orders with lines and supplier info. FULL_TABLE: the
    Alumio contract has no change-date filter, and closed orders drop out of the
    status filter, so each run is a snapshot of open orders."""

    name = "purchase_orders"
    path = "/purchaseOrders"
    primary_keys = ["id"]
    replication_key = None

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "id": {"type": ["string", "integer"]},
            "orderStatus": STRING,
            "OrderStatus": STRING,
            "lines": array_of(obj()),
            "supplierInfo": obj(),
        },
        "required": ["id"],
    }

    def extra_url_params(self, context) -> dict[str, t.Any]:
        return {
            "filter": status_filter(
                "OrderStatus", self.config.get("purchase_order_statuses"), "O,B"
            ),
            "select": "lines,supplierInfo",
        }


def _parse_day(value: t.Any) -> date:
    """Parse a Singer bookmark or config date into a calendar day."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise FatalAPIError(f"Cannot parse date from bookmark/start_date {text!r}") from exc
