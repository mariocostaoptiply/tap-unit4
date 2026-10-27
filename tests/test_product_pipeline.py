"""Credential-free regressions for concurrent products and the deferred stock child."""

import io
import json
import threading
import unittest
from collections import Counter
from contextlib import redirect_stdout
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import requests
import requests_mock
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.tap_base import InvalidCredentialsError

from tap_unit4.tap import TapUnit4


BASE = "https://unit4.example.com/acme/rest/api/v1/01"


def config(**overrides):
    return {
        "base_url": "https://unit4.example.com",
        "environment": "acme",
        "username": "synthetic-user",
        "password": "synthetic-password",
        "page_size": 2,
        "stock_batch_size": 2,
        "unit4_products_request_workers": 4,
        "unit4_product_stock_request_workers": 2,
        **overrides,
    }


def product(index, unit="ST"):
    return {
        "id": f"product-{index}-{unit}",
        "adminCode": "01",
        "countryCodeOrigin": "NL",
        "countryCodeProvenance": "BE",
        "eanCodeGenerationKey": None,
        "eanCode": f"8710000{index:06d}",
        "eanCodeAsText": f"8710000{index:06d}",
        "length": 3.5,
        "width": 1.5,
        "weight": 0.5,
        "externalProductCode": f"EXT-{index}",
        "productGroupCodeExternalProduct": "EXT-G",
        "guarantee": 12,
        "locationCode": "A-01",
        "packingUnit": "BOX",
        "packingRatio": 10,
        "referenceJournalEntryCode": "J",
        "searchName": f"SEARCH-{index}",
        "searchKeys": f"KEY-{index}",
        "vatGroup": "HIGH",
        "warehouseCode": "W",
        "stockRegistration": True,
        "discontinued": False,
        "purchaseOrderRecommendation": True,
        "blocked": False,
        "batchRegistration": False,
        "oneOff": False,
        "minimumStock": 3,
        "maximumStock": 30,
        "minimalPurchase": 2,
        "compositionType": "N",
        "storeUnit": "ST",
        "storeRatio": 1,
        "bulkUnit": "BOX",
        "bulkRatio": 10,
        "statistics": "Y",
        "useOfSerialNumbers": "N",
        "printStickers": 1,
        "imagePath": "/synthetic/image.png",
        "abcCode": "A",
        "isAssembly": False,
        "assemblyRecommendation": False,
        "percentageLoss": 1.5,
        "assemblyReferenceJournalEntryCode": "ASSEMBLY-J",
        "assemblyCosts": 2.25,
        "assemblyWarehouseCode": "AW",
        "assemblyLocationCode": "AL",
        "surplusLocationCode": "SL",
        "numberOfUnitsPerWorkingDay": 8,
        "numberOfDaysPerUnit": 0.5,
        "numberOfSafetyDays": 3,
        "assemblySubContractedWork": False,
        "assemblyOnlyThroughOutsourcedWork": False,
        "assemblyForecastNumber": 4,
        "usePartialRelocation": "N",
        "useComponentPartial": "N",
        "partialRelocationMinimum": 2,
        "applySubAssemblyEndProduct": False,
        "applySubAssemblyComponent": False,
        "subAssemblyApplicationType": "N",
        "alternativeUnitCode": "BOX",
        "packingRatioAlternativeUnit": 10,
        "externalStatus": "ACTIVE",
        "stockCosts": 1.25,
        "stockCostPercentage": 0.5,
        "salesPriceBasedOnComponents": False,
        "factMeasure": 1,
        "percentagePriceControl": 2.5,
        "propertiesToInclude": "*",
        "productCode": f"P{index}",
        "unitCode": unit,
        "description": f"Synthetic product {index}",
        "costPrice": index + 1.5,
        "inactive": False,
        "heigth": 2.5,
        "stockKeeping": True,
        "assemblyInstruction": "Synthetic assembly instruction",
        "productClassification": [{
            "id": f"classification-{index}",
            "productIdentifier": {"productCode": f"P{index}"},
            "productGroupCode": "G",
            "productCategoryCode": "C",
        }],
    }


def offset(request):
    return int(parse_qs(urlparse(request.url).query)["offset"][0])


def pipeline_tap(selected=("products", "product_stock"), deselect=(), **overrides):
    cfg = config(**overrides)
    catalog = TapUnit4(config=cfg).catalog_dict
    for entry in catalog["streams"]:
        for meta in entry["metadata"]:
            if meta["breadcrumb"] == []:
                meta["metadata"]["selected"] = entry["tap_stream_id"] in selected
            if (entry["tap_stream_id"], tuple(meta["breadcrumb"])) in deselect:
                meta["metadata"]["selected"] = False
    return TapUnit4(config=cfg, catalog=catalog)


def sync(tap):
    output = io.StringIO()
    with redirect_stdout(output):
        tap.sync_all()
    return [json.loads(line) for line in output.getvalue().splitlines()]


def emitted(messages, stream):
    return [m["record"] for m in messages if m["type"] == "RECORD" and m["stream"] == stream]


class ConcurrentProductTests(unittest.TestCase):
    def test_output_and_termination_match_for_short_full_and_empty_pages(self):
        for workers in (1, 4, 10):
            for count in (0, 1, 2, 3, 4, 9, 10):
                for ending in (200, 204, 404):
                    if count == 0 and ending == 404:
                        continue
                    with self.subTest(workers=workers, count=count, ending=ending):
                        source = [product(i) for i in range(count)]
                        stream = pipeline_tap(
                            selected=("products",), unit4_products_request_workers=workers,
                        ).streams["products"]

                        def page(request, context):
                            start = offset(request) - 1
                            context.status_code = 200 if start < count else ending
                            return source[start:start + 2]

                        with requests_mock.Mocker() as mock:
                            mock.get(f"{BASE}/products", json=page)
                            self.assertEqual(list(stream.get_records(None)), source)
                            requested = [offset(r) for r in mock.request_history]
                            self.assertEqual(len(requested), len(set(requested)))

    def test_concurrent_completion_preserves_offset_order_and_main_thread_parsing(self):
        stream = pipeline_tap(
            selected=("products",), unit4_products_request_workers=4,
        ).streams["products"]
        source = [product(i) for i in range(11)]
        barrier = threading.Barrier(4, timeout=3)
        later_page_done = threading.Event()
        lock = threading.Lock()
        active = peak = 0
        completed = []
        parse_threads = []
        main_thread = threading.get_ident()
        original_parse = stream.parse_response

        def send(prepared, **_kwargs):
            nonlocal active, peak
            start = offset(prepared)
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                if start in (3, 5, 7, 9):
                    barrier.wait()
                if start == 3:
                    self.assertTrue(later_page_done.wait(timeout=3))
                response = requests.Response()
                response.status_code = 200
                response._content = json.dumps(source[start - 1:start + 1]).encode()
                response.request = prepared
                with lock:
                    completed.append(start)
                if start == 5:
                    later_page_done.set()
                return response
            finally:
                with lock:
                    active -= 1

        def parse(response):
            parse_threads.append(threading.get_ident())
            yield from original_parse(response)

        with patch.object(stream.requests_session, "send", side_effect=send):
            with patch.object(stream, "parse_response", side_effect=parse):
                self.assertEqual(list(stream.get_records(None)), source)
        self.assertEqual(peak, 4)
        self.assertLess(completed.index(5), completed.index(3))
        self.assertEqual(set(parse_threads), {main_thread})

    def test_invalid_workers_fail_before_http(self):
        for workers in (0, 11):
            with self.subTest(workers=workers), requests_mock.Mocker() as mock:
                stream = pipeline_tap(unit4_products_request_workers=workers).streams["products"]
                with self.assertRaises(FatalAPIError):
                    list(stream.get_records(None))
                self.assertEqual(mock.request_history, [])

    def test_duplicate_normalized_identity_aborts_before_emitting_bad_page(self):
        pages = (
            [[product(0), product(1)], [product(0)]],
            [[product(0), product(1)], [product(2), product(3)], [product(0), product(1)]],
            [[{"productCode": 1, "unitCode": "ST"}, {"productCode": "1", "unitCode": "ST"}]],
        )
        for source_pages in pages:
            with self.subTest(pages=source_pages), requests_mock.Mocker() as mock:
                stream = pipeline_tap(selected=("products",)).streams["products"]

                def page(request, _context):
                    index = (offset(request) - 1) // 2
                    return source_pages[index] if index < len(source_pages) else []

                mock.get(f"{BASE}/products", json=page)
                rows = []
                with self.assertRaises(FatalAPIError):
                    for row in stream.get_records(None):
                        rows.append(row)
                identities = [(str(r["productCode"]), r["unitCode"]) for r in rows]
                self.assertEqual(len(identities), len(set(identities)))

    def test_page_budget_and_oversized_pages_abort(self):
        for limit, source in ((2, [product(i) for i in range(9)]), (100, [product(i) for i in range(3)])):
            with self.subTest(limit=limit), requests_mock.Mocker() as mock:
                stream = pipeline_tap(selected=("products",), max_pages=limit).streams["products"]
                mock.get(
                    f"{BASE}/products",
                    json=lambda r, _c: source[offset(r) - 1:offset(r) + 1] if limit == 2 else source,
                )
                with self.assertRaises(FatalAPIError):
                    list(stream.get_records(None))
                self.assertTrue(all(offset(r) <= (3 if limit == 2 else 1) for r in mock.request_history))


class DeferredStockTests(unittest.TestCase):
    @staticmethod
    def register(mock, source, calls, bad_stock=False):
        def page(request, _context):
            calls.append(("products", offset(request)))
            start = offset(request) - 1
            return source[start:start + 2]

        def stock(request, _context):
            calls.append(("stock", None))
            identifiers = request.json()["productIdentifiers"]
            results = [
                {
                    "productIdentifier": entry["productIdentifier"],
                    "unitCode": entry["unitCode"],
                    "availableStockForSales": int(entry["productIdentifier"]["productCode"][1:]) + 7,
                }
                for entry in reversed(identifiers)
            ]
            if bad_stock:
                results[0]["productIdentifier"] = {"productCode": "UNREQUESTED"}
            return {"resultData": results}

        mock.get(f"{BASE}/products", json=page)
        mock.post(f"{BASE}/requestProductStock", json=stock)

    def test_single_scan_then_stock_emits_full_rows_joined_by_composite_identity(self):
        source = [product(i) for i in range(9)] + [product(0, unit="BOX")]
        calls = []
        tap = pipeline_tap()
        with requests_mock.Mocker() as mock:
            self.register(mock, source, calls)
            messages = sync(tap)
        self.assertEqual(emitted(messages, "products"), source)
        actual = {(r["productCode"], r["unitCode"]): r for r in emitted(messages, "product_stock")}
        expected = {
            (r["productCode"], r["unitCode"]): {
                **r,
                "productIdentifier": {"productCode": r["productCode"]},
                "availableStockForSales": int(r["productCode"][1:]) + 7,
                "resultIndex": 0,
            }
            for r in source
        }
        self.assertEqual(actual, expected)
        self.assertEqual(len(emitted(messages, "product_stock")), len(source))
        first_stock = next(i for i, call in enumerate(calls) if call[0] == "stock")
        self.assertTrue(all(kind == "products" for kind, _ in calls[:first_stock]))
        self.assertTrue(all(kind == "stock" for kind, _ in calls[first_stock:]))
        gets = [start for kind, start in calls if kind == "products"]
        self.assertEqual(len(gets), len(set(gets)))
        self.assertLessEqual(len(gets), 9)
        schemas = [m for m in messages if m["type"] == "SCHEMA" and m["stream"] == "product_stock"]
        self.assertEqual(len(schemas), 1)
        self.assertIsNone(tap.streams["products"]._stock_products)

    def test_stream_and_field_selections_do_not_strip_queued_product_data(self):
        for selected in (("products",), ("product_stock",), ("products", "product_stock")):
            with self.subTest(selected=selected), requests_mock.Mocker() as mock:
                source = [product(i) for i in range(3)]
                calls = []
                self.register(mock, source, calls)
                tap = pipeline_tap(
                    selected=selected,
                    deselect=(("products", ("properties", "description")),
                              ("products", ("properties", "productClassification"))),
                )
                messages = sync(tap)
                counts = Counter(m["stream"] for m in messages if m["type"] == "RECORD")
                self.assertEqual(counts, Counter({name: 3 for name in selected}))
                for row in emitted(messages, "products"):
                    self.assertNotIn("description", row)
                    self.assertNotIn("productClassification", row)
                for row in emitted(messages, "product_stock"):
                    original = source[int(row["productCode"][1:])]
                    self.assertEqual(row["description"], original["description"])
                    self.assertEqual(row["productClassification"], original["productClassification"])
                self.assertEqual(any(kind == "stock" for kind, _ in calls), "product_stock" in selected)
                self.assertIsNone(tap.streams["products"]._stock_products)

    def test_parent_filter_can_exclude_the_last_or_every_product_without_skipping_child_completion(self):
        for expression, codes in (("productCode != 'P2'", ["P0", "P1"]), ("False", [])):
            with self.subTest(expression=expression), requests_mock.Mocker() as mock:
                calls = []
                self.register(mock, [product(i) for i in range(3)], calls)
                messages = sync(pipeline_tap(stream_maps={"products": {"__filter__": expression}}))
                self.assertEqual([r["productCode"] for r in emitted(messages, "products")], codes)
                self.assertEqual(sorted(r["productCode"] for r in emitted(messages, "product_stock")), codes)
                self.assertTrue(any(m["type"] == "SCHEMA" and m["stream"] == "product_stock" for m in messages))

    @patch("time.sleep", lambda *_: None)
    def test_product_failure_never_starts_stock_and_next_run_starts_from_first_page(self):
        for status, error in ((401, InvalidCredentialsError), (403, FatalAPIError), (500, RetriableAPIError)):
            with self.subTest(status=status):
                tap = pipeline_tap()
                with requests_mock.Mocker() as mock:
                    def page(request, context):
                        context.status_code = status if offset(request) == 3 else 200
                        return [product(i) for i in range(offset(request) - 1, offset(request) + 1)]

                    mock.get(f"{BASE}/products", json=page)
                    with redirect_stdout(io.StringIO()), self.assertRaises(error):
                        tap.sync_all()
                    self.assertFalse(any(r.method == "POST" for r in mock.request_history))
                self.assertIsNone(tap.streams["products"]._stock_products)
                calls = []
                with requests_mock.Mocker() as mock:
                    self.register(mock, [product(i) for i in range(3)], calls)
                    messages = sync(tap)
                self.assertEqual(calls[0], ("products", 1))
                self.assertEqual(len(emitted(messages, "product_stock")), 3)

    def test_stock_mismatch_and_failed_parent_emission_clear_the_queue(self):
        for failure in ("stock", "parent-output"):
            with self.subTest(failure=failure), requests_mock.Mocker() as mock:
                calls = []
                self.register(mock, [product(i) for i in range(10)], calls, bad_stock=failure == "stock")
                tap = pipeline_tap()
                parent = tap.streams["products"]
                if failure == "stock":
                    retained_queues = []
                    stock = tap.streams["product_stock"]
                    original_parse = stock.parse_response

                    def parse(response):
                        retained_queues.append(parent._stock_products)
                        self.assertGreater(len(parent._stock_products), 0)
                        yield from original_parse(response)

                    with patch.object(stock, "parse_response", side_effect=parse):
                        with redirect_stdout(io.StringIO()), self.assertRaises(FatalAPIError):
                            tap.sync_all()
                    self.assertEqual(list(retained_queues[0]), [])
                else:
                    with patch.object(parent, "_write_record_message", side_effect=RuntimeError("output failed")):
                        with redirect_stdout(io.StringIO()), self.assertRaises(RuntimeError):
                            tap.sync_all()
                    self.assertFalse(any(kind == "stock" for kind, _ in calls))
                self.assertIsNone(parent._stock_products)

    def test_empty_and_all_inactive_catalogues_emit_no_stock_records(self):
        for source in ([], [{**product(0), "inactive": True}]):
            with self.subTest(source=source), requests_mock.Mocker() as mock:
                calls = []
                self.register(mock, source, calls)
                tap = pipeline_tap(stock_skip_inactive=True)
                messages = sync(tap)
                self.assertEqual(emitted(messages, "product_stock"), [])
                self.assertFalse(any(kind == "stock" for kind, _ in calls))
                self.assertTrue(any(m["type"] == "SCHEMA" and m["stream"] == "product_stock" for m in messages))
                self.assertIsNone(tap.streams["products"]._stock_products)

    def test_partial_parent_read_does_not_start_stock(self):
        calls = []
        tap = pipeline_tap()
        with requests_mock.Mocker() as mock:
            self.register(mock, [product(i) for i in range(3)], calls)
            records = tap.streams["products"].get_records(None)
            self.assertEqual(next(records), product(0))
            records.close()
        self.assertFalse(any(kind == "stock" for kind, _ in calls))
        self.assertIsNone(tap.streams["products"]._stock_products)

    def test_transient_product_page_errors_do_not_duplicate_stock_requests(self):
        source = [product(i) for i in range(3)]
        calls = []
        attempts = 0
        with requests_mock.Mocker() as mock, patch("time.sleep", lambda *_: None):
            self.register(mock, source, calls)

            def page(request, context):
                nonlocal attempts
                start = offset(request) - 1
                if start == 2:
                    attempts += 1
                    if attempts < 3:
                        context.status_code = 429 if attempts == 1 else 503
                        return []
                return source[start:start + 2]

            mock.get(f"{BASE}/products", json=page)
            messages = sync(pipeline_tap())
            posts = [r for r in mock.request_history if r.method == "POST"]
            requested = [
                (p["productIdentifier"]["productCode"], p["unitCode"])
                for request in posts for p in request.json()["productIdentifiers"]
            ]
            self.assertEqual(Counter(requested), Counter((r["productCode"], r["unitCode"]) for r in source))
            self.assertEqual(len(emitted(messages, "product_stock")), 3)
            self.assertEqual(attempts, 3)
