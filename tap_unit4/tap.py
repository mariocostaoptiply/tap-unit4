"""Unit4 Wholesale tap using hotglue_singer_sdk."""

from __future__ import annotations

from typing import Any

from hotglue_singer_sdk import Tap
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_unit4.streams import (
    ProductStockStream,
    ProductsStream,
    PurchaseOrdersStream,
    SellOrdersStream,
)

STREAM_TYPES = [
    ProductsStream,
    ProductStockStream,
    SellOrdersStream,
    PurchaseOrdersStream,
]


class TapUnit4(Tap):
    """Singer tap for the Unit4 Wholesale REST API (v1)."""

    name = "tap-unit4"
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = th.PropertiesList(
        th.Property(
            "base_url",
            th.StringType,
            required=True,
            description="Unit4 host, e.g. https://unit4.example.com (Alumio UNIT4_BASE_URI).",
        ),
        th.Property(
            "environment",
            th.StringType,
            required=True,
            description="Path segment before /rest/api/v1 (Alumio UNIT4_ENVIRONMENT).",
        ),
        th.Property(
            "administration",
            th.StringType,
            default="01",
            description="Unit4 administration/company code in the API path.",
        ),
        th.Property("username", th.StringType, required=True, description="API username."),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="API password (treated as secret by key name).",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="First sell-order day to sync when no bookmark exists. Defaults to today.",
        ),
        th.Property(
            "sell_order_lookback_days",
            th.IntegerType,
            default=1,
            description="Days before the bookmark day to re-read sell orders on each run.",
        ),
        th.Property(
            "sell_order_statuses",
            th.StringType,
            default="A,Z,O",
            description="Comma-separated salesOrders status codes to include.",
        ),
        th.Property(
            "stock_skip_inactive",
            th.BooleanType,
            default=False,
            description="Skip stock calls for products flagged inactive.",
        ),
        th.Property(
            "stock_batch_size",
            th.IntegerType,
            default=100,
            description=(
                "Products per requestProductStock call; use more than 1 only when "
                "response identities are guaranteed."
            ),
        ),
        th.Property(
            "unit4_product_stock_request_workers",
            th.IntegerType,
            default=10,
            description=(
                "Concurrent requestProductStock HTTP requests; allowed range 1-32."
            ),
        ),
        th.Property("page_size", th.IntegerType, default=100, description="limit per page."),
        th.Property(
            "max_pages",
            th.IntegerType,
            default=10000,
            description="Abort a paging run after this many pages.",
        ),
        th.Property(
            "request_timeout",
            th.IntegerType,
            default=300,
            description="Per-request timeout in seconds.",
        ),
    ).to_dict()

    def load_state(self, state: dict[str, Any]) -> None:
        """Normalize legacy scalar bookmarks before delegating to the SDK."""
        super().load_state(self._normalize_legacy_bookmarks(state))

    def _normalize_legacy_bookmarks(self, state: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(state, dict):
            return {}

        bookmarks = state.get("bookmarks")
        if not isinstance(bookmarks, dict):
            return state

        normalized_state = dict(state)
        normalized_bookmarks: dict[str, dict[str, Any]] = {}

        for stream_name, stream_state in bookmarks.items():
            if isinstance(stream_state, dict):
                normalized_bookmarks[stream_name] = stream_state
                continue

            stream = self.streams.get(stream_name)
            replication_key = getattr(stream, "replication_key", None) if stream else None

            if replication_key and stream_state not in (None, ""):
                self.logger.warning(
                    "Coercing legacy scalar bookmark for stream '%s' into Singer state.",
                    stream_name,
                )
                normalized_bookmarks[stream_name] = {
                    "replication_key": replication_key,
                    "replication_key_value": stream_state,
                }
                continue

            self.logger.warning(
                "Ignoring malformed bookmark for stream '%s': expected an object, got %s.",
                stream_name,
                type(stream_state).__name__,
            )
            normalized_bookmarks[stream_name] = {}

        normalized_state["bookmarks"] = normalized_bookmarks
        return normalized_state

    def discover_streams(self):
        """Return discovered streams."""
        return [stream_type(tap=self) for stream_type in STREAM_TYPES]


if __name__ == "__main__":
    TapUnit4.cli()
