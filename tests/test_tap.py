"""Credential-free tests for tap-unit4 (HTTP mocked with requests-mock)."""

import base64
import json
import threading
import unittest
from copy import deepcopy
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import requests
import requests_mock
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.tap_base import InvalidCredentialsError

from tap_unit4.tap import TapUnit4

BASE = "https://unit4.example.com/acme/rest/api/v1/01"


def config(**overrides):
    cfg = {
        "base_url": "https://unit4.example.com/",
        "environment": "acme",
        "username": "user",
        "password": "secret",
        "page_size": 2,
        "unit4_product_stock_request_workers": 1,
        "start_date": "2026-09-10T00:00:00Z",
    }
    cfg.update(overrides)
    return cfg


def records(stream, context=None):
    return list(stream.get_records(context))


def query(request):
    return {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}


class DiscoveryTests(unittest.TestCase):
    def test_streams_keys_and_replication(self):
        tap = TapUnit4(config=config())
        streams = tap.streams
        self.assertEqual(
            sorted(streams), ["product_stock", "products", "purchase_orders", "sell_orders"]
        )
        self.assertEqual(streams["products"].primary_keys, ["productCode", "unitCode"])
        self.assertEqual(streams["sell_orders"].replication_key, "orderDate")
        self.assertIsNone(streams["purchase_orders"].replication_key)
        catalog = tap.catalog_dict
        json.dumps(catalog)
        self.assertEqual(len(catalog["streams"]), 4)

    def test_purchase_order_catalog_exposes_native_attributes(self):
        catalog = TapUnit4(config=config()).catalog_dict
        purchase_orders = next(
            stream for stream in catalog["streams"]
            if stream["tap_stream_id"] == "purchase_orders"
        )
        expected_fields = set("""
            id adminCode orderCode automaticDiscountCalculation branchCode
            currencyCode contactPersonCode deliveryConditionCode
            deliveryConditionDescription discountAmount discountPercentage lines
            orderConfirmationText textLinesAtAnOrder purchaseOrderInstructionsText
            orderDate orderStatus orderStatusDescription orderType
            paymentConditionCode reference supplierCode supplierInfo warehouseCode
            canCollect propertiesToInclude
        """.split())
        properties = purchase_orders["schema"]["properties"]
        self.assertEqual(set(properties), expected_fields)
        metadata_fields = {
            item["breadcrumb"][1]
            for item in purchase_orders["metadata"]
            if len(item["breadcrumb"]) == 2 and item["breadcrumb"][0] == "properties"
        }
        self.assertEqual(metadata_fields, expected_fields)
        line_properties = properties["lines"]["items"]["properties"]
        self.assertTrue({
            "id", "productIdentifier", "unitCode", "quantityOrdered",
            "quantityDelivered", "price", "deliveryAfter", "deliveryBefore",
            "deliveryLines", "statusCode",
        }.issubset(line_properties))
        self.assertEqual(
            set(properties["supplierInfo"]["properties"]),
            {"id", "organisationInfo"},
        )

    def test_password_is_secret(self):
        from hotglue_singer_sdk.helpers._secrets import is_common_secret_key

        self.assertTrue(is_common_secret_key("password"))


class AuthAndErrorTests(unittest.TestCase):
    def setUp(self):
        self.stream = TapUnit4(config=config()).streams["purchase_orders"]

    def test_basic_auth_header_and_url(self):
        with requests_mock.Mocker() as m:
            order = {"id": 1, "orderStatus": "Z"}
            m.get(f"{BASE}/purchaseOrders", json=[order])
            self.assertEqual(records(self.stream), [order])
            sent = m.request_history[0]
            expected = base64.b64encode(b"user:secret").decode()
            self.assertEqual(sent.headers["Authorization"], f"Basic {expected}")
            self.assertEqual(
                query(sent), {"limit": "2", "offset": "1", "select": "lines,supplierInfo"}
            )

    def test_401_is_invalid_credentials_without_retry(self):
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/purchaseOrders", status_code=401)
            with self.assertRaises(InvalidCredentialsError):
                records(self.stream)
            self.assertEqual(m.call_count, 1)

    def test_403_and_400_are_fatal(self):
        for status in (403, 400):
            with requests_mock.Mocker() as m:
                m.get(f"{BASE}/purchaseOrders", status_code=status, text="nope")
                with self.assertRaises(FatalAPIError):
                    records(self.stream)
                self.assertEqual(m.call_count, 1)

    def test_first_page_404_is_fatal(self):
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/purchaseOrders", status_code=404)
            with self.assertRaises(FatalAPIError):
                records(self.stream)

    def test_non_paginated_404_is_fatal(self):
        stream = TapUnit4(config=config()).streams["product_stock"]
        with requests_mock.Mocker() as m:
            m.post(f"{BASE}/requestProductStock", status_code=404)
            with self.assertRaises(FatalAPIError):
                list(
                    stream.request_records(
                        {
                            "products": [
                                {"productCode": "P1", "unitCode": "ST"}
                            ]
                        }
                    )
                )

    def test_empty_200_is_fatal(self):
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/purchaseOrders", status_code=200, content=b"")
            with self.assertRaises(FatalAPIError):
                records(self.stream)

    def test_malformed_json_is_fatal(self):
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/purchaseOrders", text="<html>")
            with self.assertRaises(FatalAPIError):
                records(self.stream)

    def test_object_instead_of_list_is_fatal(self):
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/purchaseOrders", json={"error": "x"})
            with self.assertRaises(FatalAPIError):
                records(self.stream)

    def test_non_object_list_item_is_fatal(self):
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/purchaseOrders", json=[{"id": 1}, 2])
            with self.assertRaises(FatalAPIError):
                records(self.stream)

    @patch("time.sleep", lambda *_: None)
    def test_5xx_retries_then_succeeds(self):
        with requests_mock.Mocker() as m:
            order = {"id": 9, "orderStatus": "Z"}
            m.get(
                f"{BASE}/purchaseOrders",
                [{"status_code": 503}, {"json": [order]}],
            )
            self.assertEqual(records(self.stream), [order])
            self.assertEqual(m.call_count, 2)

    @patch("time.sleep", lambda *_: None)
    def test_5xx_retries_are_bounded(self):
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/purchaseOrders", status_code=500)
            with self.assertRaises(RetriableAPIError):
                records(self.stream)
            self.assertEqual(m.call_count, 5)

    def test_retry_after_forms_are_bounded(self):
        resp = requests.Response()
        resp.status_code = 429
        for header, expected in (
            ("9999", 120.0),
            ("3", 3.0),
            ("Wed, 21 Oct 2015 07:28:00 GMT", 0.0),
            ("garbage", 0.0),
        ):
            resp.headers["Retry-After"] = header
            self.assertEqual(self.stream._retry_after_seconds(resp), expected)


class PaginationTests(unittest.TestCase):
    def setUp(self):
        self.stream = TapUnit4(config=config()).streams["products"]

    def _product(self, code):
        return {"productCode": code, "unitCode": "ST"}

    def test_one_based_offset_pages_until_short_page(self):
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/products",
                [
                    {"json": [self._product("A"), self._product("B")]},
                    {"json": [self._product("C")]},
                ],
            )
            codes = [r["productCode"] for r in records(self.stream)]
            self.assertEqual(codes, ["A", "B", "C"])
            offsets = [query(r)["offset"] for r in m.request_history]
            self.assertEqual(offsets, ["1", "3"])
            self.assertEqual(query(m.request_history[0])["select"], "productClassification")

    def test_full_last_page_then_empty(self):
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/products",
                [
                    {"json": [self._product("A"), self._product("B")]},
                    {"json": []},
                ],
            )
            self.assertEqual(len(records(self.stream)), 2)
            self.assertEqual(m.call_count, 2)

    def test_full_last_page_then_404(self):
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/products",
                [
                    {"json": [self._product("A"), self._product("B")]},
                    {"status_code": 404},
                ],
            )
            self.assertEqual(len(records(self.stream)), 2)

    def test_repeated_page_is_fatal(self):
        page = [self._product("A"), self._product("B")]
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/products", json=page)
            with self.assertRaises(FatalAPIError):
                records(self.stream)
            self.assertEqual(m.call_count, 2)

    def test_max_pages_guard(self):
        stream = TapUnit4(config=config(max_pages=1)).streams["products"]
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/products",
                [
                    {"json": [self._product("A"), self._product("B")]},
                    {"json": [self._product("C"), self._product("D")]},
                ],
            )
            with self.assertRaises(FatalAPIError):
                records(stream)

    def test_products_without_identity_are_skipped(self):
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/products",
                json=[{"productCode": "A"}],
            )
            self.assertEqual(records(self.stream), [])

    def test_numeric_ean_codes_are_normalized(self):
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/products",
                [
                    {
                        "json": [
                            {
                                "productCode": "A",
                                "unitCode": "ST",
                                "eanCode": 0,
                            },
                            {
                                "productCode": "B",
                                "unitCode": "ST",
                                "eanCode": 8712345678901,
                            },
                        ]
                    },
                    {"json": []},
                ],
            )

            rows = records(self.stream)
            self.assertIsNone(rows[0]["eanCode"])
            self.assertEqual(rows[1]["eanCode"], "8712345678901")


class ProductStockTests(unittest.TestCase):
    @staticmethod
    def _product(code, inactive=False):
        return {"productCode": code, "unitCode": "ST", "inactive": inactive}

    @staticmethod
    def _stock(code, quantity=1):
        return {
            "productCode": code,
            "unitCode": "ST",
            "availableStockForSales": quantity,
        }

    @staticmethod
    def _nested_stock(code, quantity=1):
        return {
            "productIdentifier": {"productCode": code},
            "unitCode": "ST",
            "availableStockForSales": quantity,
        }

    def test_batches_products_with_unit4_payload(self):
        stream = TapUnit4(
            config=config(page_size=100, stock_batch_size=2)
        ).streams["product_stock"]
        products = [self._product("P1"), self._product("P2")]
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/products", json=products)
            m.post(
                f"{BASE}/requestProductStock",
                json={
                    "resultData": [
                        self._nested_stock("P1", 7),
                        self._nested_stock("P2", 8),
                    ]
                },
            )
            rows = records(stream)

            self.assertEqual(
                rows,
                [
                    {
                        **self._nested_stock("P1", 7),
                        "productCode": "P1",
                        "resultIndex": 0,
                    },
                    {
                        **self._nested_stock("P2", 8),
                        "productCode": "P2",
                        "resultIndex": 0,
                    },
                ],
            )
            posts = [request for request in m.request_history if request.method == "POST"]
            self.assertEqual(len(posts), 1)
            self.assertEqual(
                posts[0].json(),
                {
                    "quantity": 1,
                    "productIdentifiers": [
                        {"unitCode": "ST", "productIdentifier": {"productCode": "P1"}},
                        {"unitCode": "ST", "productIdentifier": {"productCode": "P2"}},
                    ],
                    "propertiesToInclude": "*",
                },
            )
            self.assertNotIn("offset", posts[0].url)

    def test_default_batch_size_is_100_and_flattens_nested_identities(self):
        stream = TapUnit4(config=config(page_size=200)).streams["product_stock"]
        products = [self._product(f"P{index}") for index in range(101)]
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/products", json=products)
            m.post(
                f"{BASE}/requestProductStock",
                [
                    {
                        "json": {
                            "resultData": [
                                self._nested_stock(f"P{index}")
                                for index in range(100)
                            ]
                        }
                    },
                    {"json": {"resultData": [self._nested_stock("P100")]}},
                ],
            )

            rows = records(stream)
            self.assertEqual(
                [row["productCode"] for row in rows],
                [f"P{index}" for index in range(101)],
            )
            posts = [request for request in m.request_history if request.method == "POST"]
            self.assertEqual(
                [len(request.json()["productIdentifiers"]) for request in posts],
                [100, 1],
            )

    def test_default_stock_request_workers_is_ten(self):
        cfg = config()
        cfg.pop("unit4_product_stock_request_workers")
        stream = TapUnit4(config=cfg).streams["product_stock"]
        self.assertEqual(stream.stock_request_workers, 10)

    def test_stock_request_workers_must_be_between_one_and_thirty_two(self):
        for value in (0, 33):
            with self.subTest(value=value):
                stream = TapUnit4(
                    config=config(unit4_product_stock_request_workers=value)
                ).streams["product_stock"]
                with self.assertRaises(FatalAPIError):
                    _ = stream.stock_request_workers

    def test_stock_requests_are_concurrent_but_parsed_on_main_thread(self):
        stream = TapUnit4(
            config=config(
                stock_batch_size=1,
                unit4_product_stock_request_workers=2,
            )
        ).streams["product_stock"]
        products = [self._product("P1"), self._product("P2")]
        main_thread = threading.get_ident()
        second_request_started = threading.Event()
        request_threads = set()
        request_threads_lock = threading.Lock()
        parse_threads = []

        def send_request(prepared_request, _context):
            with request_threads_lock:
                request_threads.add(threading.get_ident())
            payload = json.loads(prepared_request.body)
            product_code = payload["productIdentifiers"][0]["productIdentifier"][
                "productCode"
            ]
            if product_code == "P1":
                self.assertTrue(second_request_started.wait(timeout=2))
            else:
                second_request_started.set()

            response = requests.Response()
            response.status_code = 200
            response._content = json.dumps(
                {"resultData": [{"availableStockForSales": 1}]}
            ).encode()
            response.request = prepared_request
            return response

        original_parse_response = stream.parse_response

        def parse_response(response):
            parse_threads.append(threading.get_ident())
            yield from original_parse_response(response)

        with patch.object(
            stream, "_iter_products", return_value=iter(products)
        ):
            with patch.object(stream, "_request", side_effect=send_request):
                with patch.object(
                    stream, "parse_response", side_effect=parse_response
                ):
                    rows = records(stream)

        self.assertEqual([row["productCode"] for row in rows], ["P1", "P2"])
        self.assertEqual(len(request_threads), 2)
        self.assertNotIn(main_thread, request_threads)
        self.assertEqual(set(parse_threads), {main_thread})

    def test_configurable_batch_size(self):
        stream = TapUnit4(
            config=config(page_size=100, stock_batch_size=2)
        ).streams["product_stock"]
        products = [self._product("P1"), self._product("P2"), self._product("P3")]
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/products", json=products)
            m.post(
                f"{BASE}/requestProductStock",
                [
                    {"json": {"resultData": [self._stock("P1"), self._stock("P2")]}},
                    {"json": {"resultData": [self._stock("P3")]}},
                ],
            )

            self.assertEqual(len(records(stream)), 3)
            posts = [request for request in m.request_history if request.method == "POST"]
            self.assertEqual(
                [len(request.json()["productIdentifiers"]) for request in posts],
                [2, 1],
            )

    def test_invalid_batch_size_is_fatal(self):
        for value in (0, -1):
            with self.subTest(value=value):
                stream = TapUnit4(
                    config=config(stock_batch_size=value)
                ).streams["product_stock"]
                with self.assertRaises(FatalAPIError):
                    records(stream)

    def test_single_unidentified_stock_result_uses_only_missing_identity(self):
        products = [self._product("P1"), self._product("P2")]
        unidentified_results = (
            {"availableStockForSales": 7},
            {"productCode": "P1", "availableStockForSales": 7},
            {"unitCode": "ST", "availableStockForSales": 7},
        )
        for unidentified in unidentified_results:
            with self.subTest(unidentified=unidentified):
                stream = TapUnit4(
                    config=config(page_size=100, stock_batch_size=2)
                ).streams["product_stock"]
                with requests_mock.Mocker() as m:
                    m.get(f"{BASE}/products", json=products)
                    m.post(
                        f"{BASE}/requestProductStock",
                        json={
                            "resultData": [
                                unidentified,
                                self._stock("P2", 8),
                            ]
                        },
                    )

                    self.assertEqual(
                        records(stream),
                        [
                            {**self._stock("P1", 7), "resultIndex": 0},
                            {**self._stock("P2", 8), "resultIndex": 0},
                        ],
                    )

    def test_batch_identity_mismatch_is_fatal(self):
        products = [self._product("P1"), self._product("P2")]
        responses = (
            [self._stock("P1")],
            [self._stock("P1"), self._stock("P1")],
            [self._stock("P1"), self._stock("P3")],
            [{"availableStockForSales": 1}, {"availableStockForSales": 2}],
            [
                {"productCode": "P3", "availableStockForSales": 1},
                self._stock("P2"),
            ],
            [
                {"unitCode": "EA", "availableStockForSales": 1},
                self._stock("P2"),
            ],
            [
                {"productCode": 1, "unitCode": "ST", "availableStockForSales": 1},
                self._stock("P2"),
            ],
            [
                {"productCode": "P1", "unitCode": 1, "availableStockForSales": 1},
                self._stock("P2"),
            ],
            [
                {
                    **self._nested_stock("P1"),
                    "productCode": "P3",
                },
                self._stock("P2"),
            ],
            [
                {
                    "productIdentifier": "P1",
                    "unitCode": "ST",
                    "availableStockForSales": 1,
                },
                self._stock("P2"),
            ],
            [
                {
                    "productIdentifier": {"productCode": 1},
                    "unitCode": "ST",
                    "availableStockForSales": 1,
                },
                self._stock("P2"),
            ],
        )
        for result_data in responses:
            with self.subTest(result_data=result_data):
                stream = TapUnit4(
                    config=config(page_size=100, stock_batch_size=2)
                ).streams["product_stock"]
                with requests_mock.Mocker() as m:
                    m.get(f"{BASE}/products", json=products)
                    m.post(
                        f"{BASE}/requestProductStock",
                        json={"resultData": result_data},
                    )
                    with self.assertRaises(FatalAPIError):
                        records(stream)

    def test_skip_inactive_and_empty_result(self):
        stream = TapUnit4(
            config=config(stock_skip_inactive=True)
        ).streams["product_stock"]
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/products", json=[self._product("P1", inactive=True)])
            m.post(f"{BASE}/requestProductStock", json={"resultData": []})
            self.assertEqual(records(stream), [])
            self.assertFalse(any(r.method == "POST" for r in m.request_history))

    def test_unexpected_stock_shape_is_fatal(self):
        stream = TapUnit4(config=config()).streams["product_stock"]
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/products", json=[self._product("P1")])
            m.post(f"{BASE}/requestProductStock", json={"resultData": {"x": 1}})
            with self.assertRaises(FatalAPIError):
                records(stream)

    def test_non_object_stock_result_is_fatal(self):
        stream = TapUnit4(config=config()).streams["product_stock"]
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/products", json=[self._product("P1")])
            m.post(
                f"{BASE}/requestProductStock",
                json={"resultData": [self._stock("P1"), 2]},
            )
            with self.assertRaises(FatalAPIError):
                records(stream)


class PurchaseOrderSyncTests(unittest.TestCase):
    @staticmethod
    def _order(order_id="PO-1", status="O", delivered=2):
        return {
            "id": order_id,
            "orderDate": "2026-09-10T00:00:00",
            "orderStatus": status,
            "supplierInfo": {"id": "S1"},
            "lines": [
                {
                    "id": f"{order_id},1",
                    "statusCode": "Z",
                    "productIdentifier": {"productCode": "P1"},
                    "unitCode": "ST",
                    "quantity": 10,
                    "quantityDelivered": delivered,
                }
            ],
        }

    def test_all_statuses_are_emitted_across_unfiltered_pages(self):
        orders = [
            self._order("PO-1", "O"),
            self._order("PO-2", "B"),
            self._order("PO-3", "T"),
            self._order("PO-4", "Z"),
            self._order(37, "X"),
        ]
        stream = TapUnit4(config=config()).streams["purchase_orders"]
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/purchaseOrders",
                [{"json": orders[:2]}, {"json": orders[2:4]}, {"json": orders[4:]}],
            )
            self.assertEqual(records(stream), orders)
            self.assertEqual(
                [query(request) for request in m.request_history],
                [
                    {"limit": "2", "offset": str(offset), "select": "lines,supplierInfo"}
                    for offset in (1, 3, 5)
                ],
            )

    def test_completed_orders_emit_before_the_boundary_advances(self):
        orders = [
            {**self._order("HISTORY", "Z"), "orderDate": "2026-09-01"},
            {**self._order("NEWER", "B"), "orderDate": "2026-09-12"},
            {**self._order("OLDEST", "O"), "orderDate": "2026-09-10"},
            {**self._order("TIED", "B"), "orderDate": "2026-09-10T18:00:00"},
            {**self._order("RECENT-CLOSED", "Z"), "orderDate": "2026-09-30"},
        ]
        state = {}
        previous_boundary = None
        for completed_ids, expected_boundary in (
            ((), "2026-09-10"),
            (("OLDEST",), "2026-09-10"),
            (("OLDEST", "TIED"), "2026-09-12"),
            (("OLDEST", "TIED", "NEWER"), "2026-09-12"),
        ):
            with self.subTest(completed=completed_ids):
                source = [
                    {**row, "orderStatus": "Z"} if row["id"] in completed_ids else row
                    for row in orders
                    if previous_boundary is None or row["orderDate"] >= previous_boundary
                ]
                tap = TapUnit4(
                    config=config(unit4_purchase_orders_request_workers=5), state=state
                )
                stream = tap.streams["purchase_orders"]
                stream.STATE_MSG_FREQUENCY = 1
                messages = []

                def page(request, _context):
                    offset = int(query(request)["offset"]) - 1
                    return source[offset:offset + 2]

                with requests_mock.Mocker() as m, patch(
                    "singer.write_message",
                    side_effect=lambda message: messages.append(deepcopy(message)),
                ):
                    m.get(f"{BASE}/purchaseOrders", json=page)
                    stream.sync()
                self.assertEqual(
                    [message.record for message in messages if hasattr(message, "record")],
                    source,
                )
                for request in m.request_history:
                    params = query(request)
                    self.assertEqual(
                        params.get("filter"),
                        f"orderDate ge {previous_boundary}" if previous_boundary else None,
                    )
                    self.assertEqual(params["select"], "lines,supplierInfo")
                states = [message.value for message in messages if hasattr(message, "value")]
                for interim in states[:-1]:
                    self.assertEqual(
                        interim["bookmarks"]["purchase_orders"].get("open_order_date"),
                        previous_boundary,
                    )
                state = states[-1]
                self.assertEqual(
                    state["bookmarks"]["purchase_orders"]["open_order_date"],
                    expected_boundary,
                )
                previous_boundary = expected_boundary

    def test_empty_or_completed_only_scan_preserves_the_boundary(self):
        for boundary in (None, "2026-09-10"):
            for orders in ([], [{"id": "CLOSED", "orderStatus": "Z"}]):
                with self.subTest(boundary=boundary, orders=orders):
                    state = {"bookmarks": {"purchase_orders": {}}}
                    if boundary:
                        state["bookmarks"]["purchase_orders"]["open_order_date"] = boundary
                    tap = TapUnit4(config=config(), state=state)
                    with requests_mock.Mocker() as m, patch("singer.write_message") as output:
                        m.get(f"{BASE}/purchaseOrders", json=orders)
                        tap.streams["purchase_orders"].sync()
                    final_state = output.call_args_list[-1].args[0].value
                    self.assertEqual(
                        final_state["bookmarks"]["purchase_orders"].get("open_order_date"),
                        boundary,
                    )

    def test_failed_resumed_scan_keeps_the_last_successful_boundary(self):
        for boundary in (None, "2026-09-01"):
            for workers in (1, 5):
                with self.subTest(boundary=boundary, workers=workers):
                    bookmark = {"open_order_date": boundary} if boundary else {}
                    tap = TapUnit4(
                        config=config(unit4_purchase_orders_request_workers=workers),
                        state={"bookmarks": {"purchase_orders": bookmark}},
                    )
                    stream = tap.streams["purchase_orders"]
                    stream.STATE_MSG_FREQUENCY = 1
                    messages = []

                    def page(request, context):
                        if query(request)["offset"] == "1":
                            return [self._order("PO-1"), self._order("PO-2")]
                        context.status_code = 400
                        return {"error": "synthetic required-page failure"}

                    with requests_mock.Mocker() as m, patch(
                        "singer.write_message",
                        side_effect=lambda message: messages.append(deepcopy(message)),
                    ):
                        m.get(f"{BASE}/purchaseOrders", json=page)
                        with self.assertRaises(FatalAPIError):
                            stream.sync()
                    self.assertEqual(
                        tap.state["bookmarks"]["purchase_orders"].get("open_order_date"),
                        boundary,
                    )
                    states = [message.value for message in messages if hasattr(message, "value")]
                    self.assertEqual(len(states), 2)
                    for state in states:
                        self.assertEqual(
                            state["bookmarks"]["purchase_orders"].get("open_order_date"),
                            boundary,
                        )
                    resumed = TapUnit4(config=config(), state=states[-1])
                    with requests_mock.Mocker() as m:
                        m.get(f"{BASE}/purchaseOrders", json=[])
                        records(resumed.streams["purchase_orders"])
                        params = query(m.request_history[0])
                        self.assertEqual(params["offset"], "1")
                        self.assertEqual(
                            params.get("filter"),
                            f"orderDate ge {boundary}" if boundary else None,
                        )

    def test_record_output_failure_does_not_commit_the_candidate(self):
        boundary = "2026-09-01"
        tap = TapUnit4(
            config=config(page_size=100),
            state={"bookmarks": {"purchase_orders": {"open_order_date": boundary}}},
        )

        def write(message):
            if hasattr(message, "record") and message.record["id"] == "PO-2":
                raise BrokenPipeError("synthetic Singer output failure")

        with requests_mock.Mocker() as m, patch("singer.write_message", side_effect=write):
            m.get(
                f"{BASE}/purchaseOrders",
                json=[self._order("PO-1"), self._order("PO-2")],
            )
            with self.assertRaises(BrokenPipeError):
                tap.streams["purchase_orders"].sync()
        self.assertEqual(
            tap.state["bookmarks"]["purchase_orders"]["open_order_date"], boundary
        )

    def test_partial_sync_does_not_save_a_candidate_boundary(self):
        tap = TapUnit4(config=config())
        stream = tap.streams["purchase_orders"]
        stream._MAX_RECORDS_LIMIT = 1
        stream.STATE_MSG_FREQUENCY = 1
        with requests_mock.Mocker() as m, patch("singer.write_message"):
            m.get(
                f"{BASE}/purchaseOrders",
                json=[self._order("PO-1"), self._order("PO-2")],
            )
            stream.sync()
        self.assertNotIn(
            "open_order_date", tap.state["bookmarks"]["purchase_orders"]
        )

    def test_invalid_active_markers_or_missing_status_abort_without_progress(self):
        invalid_orders = [
            {"id": "BAD"},
            {**self._order("BAD"), "orderStatus": None},
            {**self._order("BAD"), "orderStatus": ""},
            {key: value for key, value in self._order("BAD").items() if key != "orderDate"},
            *[
                {**self._order("BAD"), "orderDate": value}
                for value in (None, "", "not-a-date", "2026-09-10trailing", 123)
            ],
        ]
        for invalid in invalid_orders:
            with self.subTest(order=invalid):
                tap = TapUnit4(config=config())
                with requests_mock.Mocker() as m, patch("singer.write_message") as output:
                    m.get(
                        f"{BASE}/purchaseOrders",
                        json=[self._order("GOOD"), invalid],
                    )
                    with self.assertRaises(FatalAPIError):
                        tap.streams["purchase_orders"].sync()
                self.assertEqual(
                    [call.args[0].record["id"] for call in output.call_args_list
                     if hasattr(call.args[0], "record")],
                    ["GOOD"],
                )
                self.assertNotIn(
                    "open_order_date", tap.state["bookmarks"]["purchase_orders"]
                )

    def test_sentinel_dates_emit_without_poisoning_the_boundary(self):
        for boundary in (None, "2026-09-01"):
            for include_normal in (False, True):
                with self.subTest(boundary=boundary, include_normal=include_normal):
                    bookmark = {"open_order_date": boundary} if boundary else {}
                    tap = TapUnit4(
                        config=config(), state={"bookmarks": {"purchase_orders": bookmark}}
                    )
                    orders = [{**self._order("SENTINEL"), "orderDate": "9999-12-31"}]
                    if include_normal:
                        orders.append(self._order("NORMAL", "B"))
                    with requests_mock.Mocker() as m, patch("singer.write_message") as output:
                        m.get(
                            f"{BASE}/purchaseOrders", [{"json": orders}, {"json": []}]
                        )
                        tap.streams["purchase_orders"].sync()
                    self.assertEqual(
                        [call.args[0].record for call in output.call_args_list
                         if hasattr(call.args[0], "record")],
                        orders,
                    )
                    self.assertEqual(
                        tap.state["bookmarks"]["purchase_orders"].get("open_order_date"),
                        "2026-09-10" if include_normal else boundary,
                    )

    def test_invalid_saved_boundary_fails_before_requests(self):
        for boundary in ("", "not-a-date", "2026-09-10trailing", 123, "9999-12-31"):
            with self.subTest(boundary=boundary):
                tap = TapUnit4(
                    config=config(),
                    state={"bookmarks": {"purchase_orders": {"open_order_date": boundary}}},
                )
                with requests_mock.Mocker() as m, patch("singer.write_message"):
                    with self.assertRaises(FatalAPIError):
                        tap.streams["purchase_orders"].sync()
                    self.assertEqual(m.request_history, [])

    def test_legacy_tracking_does_not_trigger_detail_lookups(self):
        state = {
            "bookmarks": {
                "purchase_orders": {
                    "source_scope": "https://old.example.com",
                    "replication_key": "orderDate",
                    "replication_key_value": "2035-01-01",
                    "tracked_order_ids": ["MISSING"],
                }
            }
        }
        stream = TapUnit4(config=config(), state=state).streams["purchase_orders"]
        orders = [self._order("PO-1", "Z")]
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/purchaseOrders", json=orders)
            self.assertEqual(records(stream), orders)
            self.assertEqual(m.call_count, 1)
            self.assertNotIn("filter", query(m.request_history[0]))

    def test_later_page_failure_is_fatal(self):
        stream = TapUnit4(config=config()).streams["purchase_orders"]
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/purchaseOrders",
                [
                    {"json": [self._order("PO-1"), self._order("PO-2")]},
                    {"status_code": 400},
                ],
            )
            with self.assertRaises(FatalAPIError):
                records(stream)
            self.assertEqual(m.call_count, 2)

    def test_concurrent_pages_preserve_order_and_payloads(self):
        stream = TapUnit4(
            config=config(unit4_purchase_orders_request_workers=10)
        ).streams["purchase_orders"]
        orders = [self._order(f"PO-{i}", "Z" if i % 2 else "O", i) for i in range(21)]
        barrier = threading.Barrier(10, timeout=3)
        later_page_done = threading.Event()
        lock = threading.Lock()
        active = peak = 0
        completed = []
        parse_threads = []
        main_thread = threading.get_ident()
        original_parse = stream.parse_response

        def send(prepared, **_kwargs):
            nonlocal active, peak
            offset = int(query(prepared)["offset"])
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                if offset in range(3, 23, 2):
                    barrier.wait()
                if offset == 3:
                    self.assertTrue(later_page_done.wait(timeout=3))
                response = requests.Response()
                response.status_code = 200
                response._content = json.dumps(orders[offset - 1:offset + 1]).encode()
                response.request = prepared
                with lock:
                    completed.append(offset)
                if offset == 5:
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
                self.assertEqual(records(stream), orders)
        self.assertEqual(peak, 10)
        self.assertLess(completed.index(5), completed.index(3))
        self.assertEqual(set(parse_threads), {main_thread})

    def test_serial_and_concurrent_scans_match_at_page_boundaries(self):
        for workers in (1, 5, 10):
            for count in (0, 1, 2, 3, 4, 10, 11, 12, 20, 21, 22):
                for ending in (200, 204, 404):
                    if count == 0 and ending == 404:
                        continue
                    with self.subTest(workers=workers, count=count, ending=ending):
                        orders = [self._order(f"PO-{i}") for i in range(count)]
                        stream = TapUnit4(
                            config=config(unit4_purchase_orders_request_workers=workers)
                        ).streams["purchase_orders"]

                        def page(request, context):
                            offset = int(query(request)["offset"]) - 1
                            context.status_code = 200 if offset < count else ending
                            return orders[offset:offset + 2]

                        with requests_mock.Mocker() as m:
                            m.get(f"{BASE}/purchaseOrders", json=page)
                            self.assertEqual(records(stream), orders)
                            offsets = [query(request)["offset"] for request in m.request_history]
                            self.assertEqual(len(offsets), len(set(offsets)))
                            self.assertTrue(all("filter" not in query(r) for r in m.request_history))

    @patch("time.sleep", lambda *_: None)
    def test_concurrent_retries_do_not_repeat_or_lose_records(self):
        stream = TapUnit4(
            config=config(unit4_purchase_orders_request_workers=5)
        ).streams["purchase_orders"]
        orders = [self._order(f"PO-{i}") for i in range(7)]
        attempts = 0

        def page(request, context):
            nonlocal attempts
            offset = int(query(request)["offset"]) - 1
            context.status_code = 200
            if offset == 2:
                attempts += 1
                if attempts < 3:
                    context.status_code = 429 if attempts == 1 else 503
                    context.headers["Retry-After"] = "0"
                    return []
            return orders[offset:offset + 2]

        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/purchaseOrders", json=page)
            self.assertEqual(records(stream), orders)
        self.assertEqual(attempts, 3)

    def test_duplicate_id_aborts_before_emitting_the_bad_page(self):
        pages = (
            [[self._order("PO-1"), self._order("PO-1")]],
            [[self._order("PO-1"), self._order("PO-2")], [self._order("PO-1")]],
            [
                [self._order("PO-1"), self._order("PO-2")],
                [self._order("PO-3"), self._order("PO-4")],
                [self._order("PO-1"), self._order("PO-2")],
            ],
            [[self._order(37), self._order("37")]],
        )
        for source_pages in pages:
            with self.subTest(pages=source_pages):
                stream = TapUnit4(
                    config=config(unit4_purchase_orders_request_workers=5)
                ).streams["purchase_orders"]
                emitted = []

                def page(request, _context):
                    index = (int(query(request)["offset"]) - 1) // 2
                    return source_pages[index] if index < len(source_pages) else []

                with requests_mock.Mocker() as m:
                    m.get(f"{BASE}/purchaseOrders", json=page)
                    with self.assertRaises(FatalAPIError):
                        for row in stream.get_records(None):
                            emitted.append(row)
                ids = [str(row["id"]) for row in emitted]
                self.assertEqual(len(ids), len(set(ids)))

    def test_concurrent_failure_replays_from_first_page(self):
        for status in (400, 401, 403, 500):
            with self.subTest(status=status):
                orders = [self._order(f"PO-{i}") for i in range(5)]
                tap = TapUnit4(config=config(unit4_purchase_orders_request_workers=5))
                messages = []
                stream = tap.streams["purchase_orders"]
                stream.STATE_MSG_FREQUENCY = 1

                def page(request, context):
                    offset = int(query(request)["offset"]) - 1
                    context.status_code = status if offset == 2 else 200
                    return orders[offset:offset + 2]

                error = InvalidCredentialsError if status == 401 else (
                    RetriableAPIError if status == 500 else FatalAPIError
                )
                with requests_mock.Mocker() as m, patch("time.sleep", lambda *_: None):
                    m.get(f"{BASE}/purchaseOrders", json=page)
                    with patch(
                        "singer.write_message",
                        side_effect=lambda message: messages.append(deepcopy(message)),
                    ):
                        with self.assertRaises(error):
                            stream.sync()
                states = [message.value for message in messages if hasattr(message, "value")]
                resumed = TapUnit4(config=config(), state=states[-1])
                with requests_mock.Mocker() as m:
                    m.get(
                        f"{BASE}/purchaseOrders",
                        json=lambda request, _context: orders[
                            int(query(request)["offset"]) - 1:int(query(request)["offset"]) + 1
                        ],
                    )
                    self.assertEqual(records(resumed.streams["purchase_orders"]), orders)
                    self.assertEqual(query(m.request_history[0])["offset"], "1")

    def test_prefetch_does_not_exceed_max_pages(self):
        stream = TapUnit4(
            config=config(unit4_purchase_orders_request_workers=5, max_pages=2)
        ).streams["purchase_orders"]
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/purchaseOrders",
                json=lambda request, _context: [
                    self._order(query(request)["offset"] + suffix) for suffix in ("a", "b")
                ],
            )
            with self.assertRaises(FatalAPIError):
                records(stream)
            self.assertEqual([query(r)["offset"] for r in m.request_history], ["1", "3"])

    def test_invalid_workers_fail_before_requests(self):
        for workers in (0, 11):
            with self.subTest(workers=workers):
                stream = TapUnit4(
                    config=config(unit4_purchase_orders_request_workers=workers)
                ).streams["purchase_orders"]
                with requests_mock.Mocker() as m:
                    with self.assertRaises(FatalAPIError):
                        records(stream)
                    self.assertEqual(m.request_history, [])

    def test_oversized_page_is_not_silently_overlapped(self):
        stream = TapUnit4(
            config=config(unit4_purchase_orders_request_workers=5)
        ).streams["purchase_orders"]
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/purchaseOrders", json=[self._order(f"PO-{i}") for i in range(3)])
            with self.assertRaises(FatalAPIError):
                records(stream)
            self.assertEqual(m.call_count, 1)


class SellOrderReplicationTests(unittest.TestCase):
    def _filter(self, tap):
        stream = tap.streams["sell_orders"]
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/salesOrders", json=[])
            records(stream)
            return query(m.request_history[0])

    def test_first_run_uses_start_date_minus_lookback(self):
        params = self._filter(TapUnit4(config=config()))
        self.assertEqual(
            params["filter"],
            "orderDate gt 2026-09-08 and status in ('A','Z','O')",
        )
        self.assertEqual(params["select"], "lines")

    def test_resume_from_bookmark(self):
        state = {
            "bookmarks": {
                "sell_orders": {
                    "replication_key": "orderDate",
                    "replication_key_value": "2026-09-20T00:00:00",
                }
            }
        }
        params = self._filter(
            TapUnit4(config=config(sell_order_lookback_days=0), state=state)
        )
        self.assertTrue(params["filter"].startswith("orderDate gt 2026-09-19 "))

    def test_legacy_scalar_bookmark_is_coerced(self):
        tap = TapUnit4(config=config(), state={"bookmarks": {"sell_orders": "2026-09-20"}})
        self.assertEqual(
            tap.state["bookmarks"]["sell_orders"]["replication_key_value"], "2026-09-20"
        )

    def test_custom_statuses_and_injection_guard(self):
        params = self._filter(TapUnit4(config=config(sell_order_statuses="A, Z")))
        self.assertTrue(params["filter"].endswith("status in ('A','Z')"))
        with self.assertRaises(FatalAPIError):
            self._filter(TapUnit4(config=config(sell_order_statuses="A') or ('1")))

    def test_sync_emits_bookmark_from_max_order_date(self):
        tap = TapUnit4(config=config())
        stream = tap.streams["sell_orders"]
        orders = [
            {"id": 2, "orderDate": "2026-09-15T00:00:00", "lines": []},
            {"id": 1, "orderDate": "2026-09-12T00:00:00", "lines": []},
        ]
        with requests_mock.Mocker() as m, patch("singer.write_message"):
            m.get(f"{BASE}/salesOrders", [{"json": orders}, {"json": []}])
            stream.sync()
        bookmark = tap.state["bookmarks"]["sell_orders"]
        self.assertEqual(bookmark["replication_key_value"], "2026-09-15T00:00:00")

    def test_sentinel_records_emit_without_poisoning_bookmark(self):
        normal = {"id": 1, "orderDate": "2026-09-15T00:00:00", "lines": []}
        for marker, sentinel_first in (
            ("9999-12-31", False),
            ("9999-12-31", True),
            ("9999-12-31T00:00:00Z", True),
        ):
            with self.subTest(marker=marker, sentinel_first=sentinel_first):
                sentinel = {"id": 2, "orderDate": marker, "lines": []}
                orders = [sentinel, normal] if sentinel_first else [normal, sentinel]
                tap = TapUnit4(config=config())
                with requests_mock.Mocker() as m, patch("singer.write_message") as output:
                    m.get(f"{BASE}/salesOrders", [{"json": orders}, {"json": []}])
                    tap.streams["sell_orders"].sync()
                emitted = [
                    call.args[0].record
                    for call in output.call_args_list
                    if getattr(call.args[0], "record", None) is not None
                ]
                self.assertEqual(emitted, orders)
                bookmark = tap.state["bookmarks"]["sell_orders"]
                self.assertEqual(bookmark["replication_key_value"], normal["orderDate"])
                resumed = TapUnit4(config=config(), state=tap.state)
                with requests_mock.Mocker() as m, patch("singer.write_message"):
                    m.get(f"{BASE}/salesOrders", json=[])
                    resumed.streams["sell_orders"].sync()
                    self.assertTrue(
                        query(m.request_history[0])["filter"].startswith(
                            "orderDate gt 2026-09-13 "
                        )
                    )

    def test_sentinel_only_sync_preserves_bookmark(self):
        for existing in (None, "2026-09-12"):
            with self.subTest(existing=existing):
                bookmark = {"replication_key": "orderDate"}
                if existing is not None:
                    bookmark["replication_key_value"] = existing
                tap = TapUnit4(
                    config=config(), state={"bookmarks": {"sell_orders": bookmark}}
                )
                order = {"id": 1, "orderDate": "9999-12-31", "lines": []}
                with requests_mock.Mocker() as m, patch("singer.write_message") as output:
                    m.get(f"{BASE}/salesOrders", json=[order])
                    tap.streams["sell_orders"].sync()
                emitted = [
                    call.args[0].record
                    for call in output.call_args_list
                    if getattr(call.args[0], "record", None) is not None
                ]
                self.assertEqual(emitted, [order])
                self.assertEqual(
                    tap.state["bookmarks"]["sell_orders"].get("replication_key_value"),
                    existing,
                )

    def test_poisoned_bookmark_fails_before_request(self):
        for bookmark in (
            "9999-12-31",
            {"replication_key": "orderDate", "replication_key_value": "9999-12-31"},
            {
                "replication_key": "orderDate",
                "replication_key_value": "9999-12-31T00:00:00Z",
            },
        ):
            with self.subTest(bookmark=bookmark):
                tap = TapUnit4(
                    config=config(), state={"bookmarks": {"sell_orders": bookmark}}
                )
                with requests_mock.Mocker() as m, patch("singer.write_message"):
                    with self.assertRaises(FatalAPIError):
                        tap.streams["sell_orders"].sync()
                    self.assertEqual(m.request_history, [])
                self.assertTrue(
                    tap.state["bookmarks"]["sell_orders"]["replication_key_value"]
                    .startswith("9999-12-31")
                )

    def test_sentinel_start_date_fails_before_request(self):
        tap = TapUnit4(config=config(start_date="9999-12-31"))
        with requests_mock.Mocker() as m, patch("singer.write_message"):
            with self.assertRaises(FatalAPIError):
                tap.streams["sell_orders"].sync()
            self.assertEqual(m.request_history, [])

    def test_missing_or_invalid_order_date_is_fatal_without_bookmark(self):
        for order in (
            {"id": 1},
            {"id": 1, "orderDate": None},
            {"id": 1, "orderDate": ""},
            {"id": 1, "orderDate": "not-a-date"},
        ):
            with self.subTest(order=order):
                tap = TapUnit4(config=config())
                stream = tap.streams["sell_orders"]
                with requests_mock.Mocker() as m, patch("singer.write_message"):
                    m.get(f"{BASE}/salesOrders", json=[order])
                    with self.assertRaises(FatalAPIError):
                        stream.sync()
                bookmark = tap.state.get("bookmarks", {}).get("sell_orders", {})
                self.assertNotIn("replication_key_value", bookmark)

    def test_failed_page_does_not_advance_bookmark(self):
        for existing in (None, "2026-09-20"):
            with self.subTest(existing=existing):
                bookmark = {"replication_key": "orderDate"}
                if existing is not None:
                    bookmark["replication_key_value"] = existing
                tap = TapUnit4(
                    config=config(), state={"bookmarks": {"sell_orders": bookmark}}
                )
                page = [
                    {"id": 1, "orderDate": "2026-09-30T00:00:00"},
                    {"id": 2, "orderDate": "9999-12-31"},
                ]
                with requests_mock.Mocker() as m, patch("singer.write_message"):
                    m.get(f"{BASE}/salesOrders", [{"json": page}, {"status_code": 400}])
                    with self.assertRaises(FatalAPIError):
                        tap.streams["sell_orders"].sync()
                self.assertEqual(
                    tap.state["bookmarks"]["sell_orders"].get("replication_key_value"),
                    existing,
                )


if __name__ == "__main__":
    unittest.main()
