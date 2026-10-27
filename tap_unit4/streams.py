"""Unit4 Wholesale stream definitions.

Endpoints and filters follow the Nestinox Alumio integration, except purchase orders:

  products         GET  /products?select=productClassification          FULL_TABLE
  product_stock    POST /requestProductStock (batched products)         FULL_TABLE
  sell_orders      GET  /salesOrders?filter=orderDate gt ... &select=lines  INCREMENTAL (orderDate)
  purchase_orders  GET  /purchaseOrders?select=lines,supplierInfo        CUSTOM (oldest O/B date)
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
MAX_PRODUCT_REQUEST_WORKERS = 10
MAX_PURCHASE_ORDER_REQUEST_WORKERS = 10


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
    _stock_products: deque[dict] | None = None

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "id": STRING,
            "adminCode": STRING,
            "countryCodeOrigin": STRING,
            "countryCodeProvenance": STRING,
            "productCode": {"type": "string"},
            "unitCode": {"type": "string"},
            "description": STRING,
            "externalProductCode": STRING,
            "eanCodeGenerationKey": {},
            "eanCode": STRING,
            "eanCodeAsText": STRING,
            "length": NUMBER,
            "width": NUMBER,
            "heigth": NUMBER,
            "weight": NUMBER,
            "productGroupCodeExternalProduct": STRING,
            "guarantee": NUMBER,
            "locationCode": STRING,
            "packingUnit": STRING,
            "packingRatio": NUMBER,
            "referenceJournalEntryCode": STRING,
            "searchName": STRING,
            "searchKeys": STRING,
            "vatGroup": STRING,
            "warehouseCode": STRING,
            "stockRegistration": BOOLEAN,
            "stockKeeping": BOOLEAN,
            "purchaseOrderRecommendation": BOOLEAN,
            "blocked": BOOLEAN,
            "batchRegistration": BOOLEAN,
            "oneOff": BOOLEAN,
            "minimalPurchase": NUMBER,
            "inactive": BOOLEAN,
            "discontinued": BOOLEAN,
            # Alumio divided costPrice by 100 before sending it to Optiply.
            "costPrice": NUMBER,
            "minimumStock": NUMBER,
            "maximumStock": NUMBER,
            "compositionType": STRING,
            "storeUnit": STRING,
            "storeRatio": NUMBER,
            "bulkUnit": STRING,
            "bulkRatio": NUMBER,
            "statistics": STRING,
            "useOfSerialNumbers": STRING,
            "productClassification": array_of(
                obj(
                    {
                        "id": STRING,
                        "productIdentifier": obj(
                            {
                                "productCode": STRING,
                                "externalProductCode": STRING,
                                "productGroupCodeExternalProduct": STRING,
                            }
                        ),
                        "productGroupCode": STRING,
                        "productCategoryCode": STRING,
                    }
                )
            ),
            "printStickers": NUMBER,
            "imagePath": STRING,
            "abcCode": STRING,
            "isAssembly": BOOLEAN,
            "assemblyRecommendation": BOOLEAN,
            "percentageLoss": NUMBER,
            "assemblyReferenceJournalEntryCode": STRING,
            "assemblyCosts": NUMBER,
            "assemblyWarehouseCode": STRING,
            "assemblyLocationCode": STRING,
            "surplusLocationCode": STRING,
            "numberOfUnitsPerWorkingDay": NUMBER,
            "numberOfDaysPerUnit": NUMBER,
            "numberOfSafetyDays": NUMBER,
            "assemblyInstruction": STRING,
            "assemblySubContractedWork": BOOLEAN,
            "assemblyOnlyThroughOutsourcedWork": BOOLEAN,
            "assemblyForecastNumber": NUMBER,
            "usePartialRelocation": STRING,
            "useComponentPartial": STRING,
            "partialRelocationMinimum": NUMBER,
            "applySubAssemblyEndProduct": BOOLEAN,
            "applySubAssemblyComponent": BOOLEAN,
            "subAssemblyApplicationType": STRING,
            "alternativeUnitCode": STRING,
            "packingRatioAlternativeUnit": NUMBER,
            "externalStatus": STRING,
            "stockCosts": NUMBER,
            "stockCostPercentage": NUMBER,
            "salesPriceBasedOnComponents": BOOLEAN,
            "factMeasure": NUMBER,
            "percentagePriceControl": NUMBER,
            "propertiesToInclude": STRING,
        },
        "required": ["productCode", "unitCode"],
    }

    def extra_url_params(self, context) -> dict[str, t.Any]:
        return {"select": "productClassification"}

    @property
    def product_request_workers(self) -> int:
        """Return the bounded number of concurrent catalogue page requests."""
        workers = self.config.get("unit4_products_request_workers", 1)
        if (
            isinstance(workers, bool)
            or not isinstance(workers, int)
            or not 1 <= workers <= MAX_PRODUCT_REQUEST_WORKERS
        ):
            raise FatalAPIError(
                "unit4_products_request_workers must be an integer between "
                f"1 and {MAX_PRODUCT_REQUEST_WORKERS}."
            )
        return workers

    def request_records(self, context) -> t.Iterable[dict]:
        seen: set[tuple[str, str]] = set()
        for page in self._request_pages(context, self.product_request_workers):
            for row in page:
                if any(row.get(key) in (None, "") for key in self.primary_keys):
                    continue
                identity = tuple(str(row[key]) for key in self.primary_keys)
                if identity in seen:
                    raise FatalAPIError(
                        "Duplicate Unit4 product identity; "
                        "pagination may have changed during extraction."
                    )
                seen.add(identity)
            yield from page

    def get_records(self, context) -> t.Iterable[dict]:
        if not self.has_selected_descendents:
            yield from super().get_records(context)
            return
        self._stock_products = deque()
        try:
            products = deque(super().get_records(context))
            while products:
                yield products.popleft()
            super()._sync_children({})
        finally:
            self._stock_products.clear()
            self._stock_products = None

    def get_child_context(self, record, context) -> dict:
        return {"product": record}

    def _sync_children(self, child_context) -> None:
        # SDK 1.0.51 calls children per accepted row; drain only after the parent iterator completes.
        if self._stock_products is not None:
            # SDK field selection removes keys in place before emitting the parent row.
            self._stock_products.append(child_context["product"].copy())

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
        if isinstance(row.get("eanCode"), int) and not isinstance(
            row["eanCode"], bool
        ):
            row["eanCode"] = None if row["eanCode"] == 0 else str(row["eanCode"])
        return row


class ProductStockStream(Unit4Stream):
    """Full product rows enriched by batched stock requests after catalogue pagination."""

    name = "product_stock"
    path = "/requestProductStock"
    rest_method = "POST"
    paginated = False
    primary_keys = ["productCode", "unitCode", "resultIndex"]
    replication_key = None
    parent_stream_type = ProductsStream
    state_partitioning_keys = []

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            **ProductsStream.schema["properties"],
            "productIdentifier": obj({"productCode": STRING}),
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
        raw_workers = self.config.get(config_key, 10)
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
        unidentified = []
        for index, result in enumerate(results):
            top_level_product_code = result.get("productCode")
            product_identifier = result.get("productIdentifier")
            if product_identifier is not None and not isinstance(
                product_identifier, dict
            ):
                raise FatalAPIError(
                    f"Unit4 {self.path} result productIdentifier is not an object."
                )
            nested_product_code = (
                product_identifier.get("productCode")
                if isinstance(product_identifier, dict)
                else None
            )
            for product_code in (top_level_product_code, nested_product_code):
                if product_code not in (None, "") and not isinstance(
                    product_code, str
                ):
                    raise FatalAPIError(
                        f"Unit4 {self.path} result has a non-string productCode."
                    )
            if (
                top_level_product_code
                and nested_product_code
                and top_level_product_code != nested_product_code
            ):
                raise FatalAPIError(
                    f"Unit4 {self.path} result has conflicting product identities."
                )

            product_code = top_level_product_code or nested_product_code
            unit_code = result.get("unitCode")
            if unit_code not in (None, "") and not isinstance(unit_code, str):
                raise FatalAPIError(
                    f"Unit4 {self.path} result has a non-string unitCode."
                )
            if product_code and unit_code:
                results[index] = {**result, "productCode": product_code}
                returned.append((product_code, unit_code))
            else:
                unidentified.append((index, product_code, unit_code))

        requested_set = set(requested)
        returned_set = set(returned)
        missing = requested_set - returned_set
        if (
            len(results) != len(products)
            or len(returned_set) != len(returned)
            or not returned_set.issubset(requested_set)
            or len(unidentified) != len(missing)
            or len(unidentified) > 1
        ):
            raise FatalAPIError(
                f"Unit4 {self.path} result identities do not match the requested "
                f"batch (requested={len(products)}, returned={len(results)}, "
                f"identified={len(returned_set)}, unidentified={len(unidentified)})."
            )

        if unidentified:
            index, product_code, unit_code = unidentified[0]
            missing_product_code, missing_unit_code = missing.pop()
            if (
                product_code not in (None, "", missing_product_code)
                or unit_code not in (None, "", missing_unit_code)
            ):
                raise FatalAPIError(
                    f"Unit4 {self.path} unidentified result does not match the "
                    "only missing request identity."
                )
            results[index] = {
                **results[index],
                "productCode": missing_product_code,
                "unitCode": missing_unit_code,
            }

        for result in results:
            yield {**result, "resultIndex": 0}

    def _iter_products(self) -> t.Iterable[dict]:
        products = self._tap.streams["products"]._stock_products
        if products is None:
            raise FatalAPIError("product_stock must be synced through its products parent.")
        while products:
            yield products.popleft()

    def get_records(self, context) -> t.Iterable[dict]:
        skip_inactive = bool(self.config.get("stock_skip_inactive", False))
        batch_size = self.stock_batch_size
        workers = self.stock_request_workers
        products = (
            product
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
                    by_identity = {
                        (product["productCode"], product["unitCode"]): product
                        for product in batch
                    }
                    for result in self._validate_batch_results(batch, results):
                        product = by_identity[(result["productCode"], result["unitCode"])]
                        yield {**product, **result}
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

    def _increment_stream_state(self, latest_record: dict, *, context=None) -> None:
        """Emit sentinel-dated orders without using them as replication markers."""
        if _parse_day(latest_record["orderDate"]) == date.max:
            return
        super()._increment_stream_state(latest_record, context=context)

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
            if start == date.max:
                raise FatalAPIError(
                    "Unit4 sell_orders bookmark/start_date is the 9999-12-31 sentinel. "
                    "Restore a last-good sales bookmark or clear only sell_orders state "
                    "and configure a historical start_date."
                )
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
    """Purchase orders from the earliest unfinished date, including final updates."""

    name = "purchase_orders"
    path = "/purchaseOrders"
    primary_keys = ["id"]
    replication_key = None

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "id": {"type": ["string", "integer"]},
            "adminCode": STRING,
            "orderCode": STRING,
            "automaticDiscountCalculation": BOOLEAN,
            "branchCode": STRING,
            "currencyCode": STRING,
            "contactPersonCode": STRING,
            "deliveryConditionCode": STRING,
            "deliveryConditionDescription": STRING,
            "discountAmount": NUMBER,
            "discountPercentage": NUMBER,
            "orderDate": STRING,
            "orderStatus": STRING,
            "orderConfirmationText": STRING,
            "textLinesAtAnOrder": STRING,
            "purchaseOrderInstructionsText": STRING,
            "orderStatusDescription": STRING,
            "orderType": STRING,
            "paymentConditionCode": STRING,
            "reference": STRING,
            "supplierCode": STRING,
            "warehouseCode": STRING,
            "canCollect": BOOLEAN,
            "propertiesToInclude": STRING,
            "lines": array_of(
                obj(
                    {
                        "id": STRING,
                        "adminCode": STRING,
                        "orderCode": STRING,
                        "lineNumber": NUMBER,
                        "deliveryAfter": STRING,
                        "deliveryBefore": STRING,
                        "deliveryLines": array_of(
                            obj(
                                {
                                    "id": STRING,
                                    "adminCode": STRING,
                                    "orderCode": STRING,
                                    "orderLineNumber": NUMBER,
                                    "deliveryLineNumber": NUMBER,
                                    "confirmationDate": STRING,
                                    "deliveryAfter": STRING,
                                    "deliveryBefore": STRING,
                                    "quantityDelivered": NUMBER,
                                    "quantityOrdered": NUMBER,
                                    "remainderExpired": BOOLEAN,
                                    "statusCode": STRING,
                                    "statusDescription": STRING,
                                    "warehouseCode": STRING,
                                    "instructionAtDelivery": STRING,
                                    "splitDeliveryLine": BOOLEAN,
                                    "adjustShipment": BOOLEAN,
                                }
                            )
                        ),
                        "discountAmount": NUMBER,
                        "discountPercentage": NUMBER,
                        "discountPercentageExtra": NUMBER,
                        "discountProductGroupPercentage": NUMBER,
                        "discountProductGroupPercentageExtra": NUMBER,
                        "price": NUMBER,
                        "productIdentifier": obj(
                            {
                                "productCode": STRING,
                                "externalProductCode": STRING,
                                "productGroupCodeExternalProduct": STRING,
                            }
                        ),
                        "qualityCode": NUMBER,
                        "quantityDelivered": NUMBER,
                        "quantityOrdered": NUMBER,
                        "statusCode": STRING,
                        "statusDescription": STRING,
                        "unitCode": STRING,
                        "warehouseCode": STRING,
                        "purchaseContractCode": STRING,
                        "pickupOrganisation": STRING,
                        "pickupAddressOneOff": BOOLEAN,
                        "textLinesAtOrderLine": STRING,
                        "purchaseOrderLineInstructionText": STRING,
                        "productDescriptionOrderLineText": STRING,
                        # Empty sample arrays do not establish their element types.
                        "notes": array_of({}),
                        "priceAdjustments": array_of({}),
                    }
                )
            ),
            "supplierInfo": obj(
                {
                    "id": STRING,
                    "organisationInfo": obj({"id": STRING, "name": STRING}),
                }
            ),
        },
        "required": ["id"],
    }

    def extra_url_params(self, context) -> dict[str, t.Any]:
        params = {"select": "lines,supplierInfo"}
        marker = self.get_context_state(context).get("open_order_date")
        if marker is not None:
            try:
                lower_bound = date.fromisoformat(marker)
            except (TypeError, ValueError) as exc:
                raise FatalAPIError(
                    "Unit4 purchase_orders open_order_date must be YYYY-MM-DD."
                ) from exc
            if lower_bound == date.max:
                raise FatalAPIError(
                    "Unit4 purchase_orders open_order_date cannot be 9999-12-31. "
                    "Restore the last-good bookmark or clear only purchase_orders state."
                )
            params["filter"] = f"orderDate ge {lower_bound.isoformat()}"
        return params

    def get_records(self, context) -> t.Iterable[dict]:
        earliest_open_date = None
        for row in super().get_records(context):
            status = row.get("orderStatus")
            if not isinstance(status, str) or not status.strip():
                raise FatalAPIError("Unit4 purchase order is missing a valid orderStatus.")
            if status in ("O", "B"):
                marker = row.get("orderDate")
                if not isinstance(marker, str):
                    raise FatalAPIError("Unit4 unfinished purchase order is missing orderDate.")
                try:
                    order_day = datetime.fromisoformat(marker.replace("Z", "+00:00")).date()
                except ValueError as exc:
                    raise FatalAPIError(
                        "Unit4 unfinished purchase order has an invalid orderDate."
                    ) from exc
                if order_day != date.max:
                    earliest_open_date = (
                        order_day if earliest_open_date is None
                        else min(earliest_open_date, order_day)
                    )
            yield row

        # Exhaustion follows emission of every record; failures and partial reads never commit.
        if self.selected and earliest_open_date is not None:
            self.get_context_state(context)["open_order_date"] = (
                earliest_open_date.isoformat()
            )

    @property
    def purchase_order_request_workers(self) -> int:
        """Return the bounded number of concurrent purchase-order requests."""
        workers = self.config.get("unit4_purchase_orders_request_workers", 1)
        if (
            isinstance(workers, bool)
            or not isinstance(workers, int)
            or not 1 <= workers <= MAX_PURCHASE_ORDER_REQUEST_WORKERS
        ):
            raise FatalAPIError(
                "unit4_purchase_orders_request_workers must be an integer between "
                f"1 and {MAX_PURCHASE_ORDER_REQUEST_WORKERS}."
            )
        return workers

    def request_records(self, context) -> t.Iterable[dict]:
        seen_ids: set[str] = set()
        for page in self._request_pages(context, self.purchase_order_request_workers):
            for record in page:
                value = record.get("id")
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (str, int))
                    or not str(value).strip()
                ):
                    raise FatalAPIError("Unit4 purchase order has a missing or invalid id.")
                order_id = str(value)
                if order_id in seen_ids:
                    raise FatalAPIError(
                        f"Duplicate Unit4 purchase order {order_id!r}; "
                        "pagination may have changed during extraction."
                    )
                seen_ids.add(order_id)
            yield from page


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
