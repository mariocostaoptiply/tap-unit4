"""Credential-free tests for tap-unit4 (HTTP mocked with requests-mock)."""

import base64
import json
import unittest
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

    def test_password_is_secret(self):
        from hotglue_singer_sdk.helpers._secrets import is_common_secret_key

        self.assertTrue(is_common_secret_key("password"))


class AuthAndErrorTests(unittest.TestCase):
    def setUp(self):
        self.stream = TapUnit4(config=config()).streams["purchase_orders"]

    def test_basic_auth_header_and_url(self):
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/purchaseOrders", json=[{"id": 1}])
            self.assertEqual(records(self.stream), [{"id": 1}])
            sent = m.request_history[0]
            expected = base64.b64encode(b"user:secret").decode()
            self.assertEqual(sent.headers["Authorization"], f"Basic {expected}")
            self.assertIn("filter=OrderStatus%20in%20('O','B')", sent.url)
            self.assertEqual(query(sent)["select"], "lines,supplierInfo")

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
                        {"productCode": "P1", "unitCode": "ST"}
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

    @patch("time.sleep", lambda *_: None)
    def test_5xx_retries_then_succeeds(self):
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/purchaseOrders",
                [{"status_code": 503}, {"json": [{"id": 9}]}],
            )
            self.assertEqual(records(self.stream), [{"id": 9}])
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

    def test_offset_pages_until_short_page(self):
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
            self.assertEqual(offsets, ["0", "2"])
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


class ProductStockTests(unittest.TestCase):
    def test_one_post_per_product_with_alumio_payload(self):
        stream = TapUnit4(config=config()).streams["product_stock"]
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/products",
                json=[{"productCode": "P1", "unitCode": "ST", "inactive": False}],
            )
            m.post(
                f"{BASE}/requestProductStock",
                json={"resultData": [{"availableStockForSales": 7}]},
            )
            rows = records(stream)
            self.assertEqual(
                rows,
                [
                    {
                        "availableStockForSales": 7,
                        "productCode": "P1",
                        "unitCode": "ST",
                        "resultIndex": 0,
                    }
                ],
            )
            post = [r for r in m.request_history if r.method == "POST"][0]
            self.assertEqual(
                post.json(),
                {
                    "quantity": 1,
                    "productIdentifiers": [
                        {"unitCode": "ST", "productIdentifier": {"productCode": "P1"}}
                    ],
                    "propertiesToInclude": "*",
                },
            )
            self.assertNotIn("offset", post.url)

    def test_skip_inactive_and_empty_result(self):
        stream = TapUnit4(config=config(stock_skip_inactive=True)).streams["product_stock"]
        with requests_mock.Mocker() as m:
            m.get(
                f"{BASE}/products",
                json=[
                    {"productCode": "P1", "unitCode": "ST", "inactive": True},
                ],
            )
            m.post(f"{BASE}/requestProductStock", json={"resultData": []})
            self.assertEqual(records(stream), [])
            self.assertFalse(any(r.method == "POST" for r in m.request_history))

    def test_unexpected_stock_shape_is_fatal(self):
        stream = TapUnit4(config=config()).streams["product_stock"]
        with requests_mock.Mocker() as m:
            m.get(f"{BASE}/products", json=[{"productCode": "P1", "unitCode": "ST"}])
            m.post(f"{BASE}/requestProductStock", json={"resultData": {"x": 1}})
            with self.assertRaises(FatalAPIError):
                records(stream)


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

    def test_failed_page_does_not_advance_bookmark(self):
        tap = TapUnit4(config=config())
        stream = tap.streams["sell_orders"]
        page = [
            {"id": 1, "orderDate": "2026-09-30T00:00:00"},
            {"id": 2, "orderDate": "2026-09-30T00:00:00"},
        ]
        with requests_mock.Mocker() as m, patch("singer.write_message"):
            m.get(f"{BASE}/salesOrders", [{"json": page}, {"status_code": 400}])
            with self.assertRaises(FatalAPIError):
                stream.sync()
        bookmark = tap.state.get("bookmarks", {}).get("sell_orders", {})
        self.assertNotEqual(bookmark.get("replication_key_value"), "2026-09-30T00:00:00")


if __name__ == "__main__":
    unittest.main()
