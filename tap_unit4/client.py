"""Unit4 Wholesale base stream: Basic auth, error mapping, and offset pagination."""

from __future__ import annotations

import json
import logging
import time
import typing as t
from email.utils import parsedate_to_datetime
from urllib.parse import quote, urlencode

import requests
import singer
from hotglue_singer_sdk.authenticators import BasicAuthenticator
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.streams import RESTStream
from hotglue_singer_sdk.tap_base import InvalidCredentialsError
from singer import StateMessage

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_PAGES = 10000
MAX_RETRY_AFTER_SECONDS = 120.0


class Unit4Stream(RESTStream):
    """Base stream for Unit4 Wholesale REST API v1 endpoints.

    URL layout: {base_url}/{environment}/rest/api/v1/{administration}/{resource}
    List endpoints return a top-level JSON array and page with limit/offset.
    """

    extra_retry_statuses = [408, 429]

    # Set on paginated streams. Used to fingerprint pages for loop detection.
    paginated = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._last_page_fingerprint: tuple | None = None
        self._pages_fetched = 0

    @property
    def url_base(self) -> str:
        """Return the Unit4 REST base for the configured environment and administration."""
        base_url = str(self.config["base_url"]).rstrip("/")
        environment = str(self.config["environment"]).strip("/")
        administration = str(self.config.get("administration") or "01").strip("/")
        return f"{base_url}/{environment}/rest/api/v1/{administration}"

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Unit4 Wholesale uses HTTP Basic auth."""
        return BasicAuthenticator(
            stream=self,
            username=self.config["username"],
            password=self.config["password"],
        )

    @property
    def http_headers(self) -> dict:
        """Return JSON request headers."""
        return {"Accept": "application/json", "Content-Type": "application/json"}

    @property
    def timeout(self) -> int:
        """Per-request timeout in seconds (Alumio used 300s for this client)."""
        try:
            return max(int(self.config.get("request_timeout", 300)), 10)
        except (TypeError, ValueError):
            return 300

    @property
    def page_size(self) -> int:
        """Return the configured limit per page."""
        try:
            return max(int(self.config.get("page_size", DEFAULT_PAGE_SIZE)), 1)
        except (TypeError, ValueError):
            return DEFAULT_PAGE_SIZE

    @property
    def max_pages(self) -> int:
        """Hard stop against runaway pagination."""
        try:
            return max(int(self.config.get("max_pages", DEFAULT_MAX_PAGES)), 1)
        except (TypeError, ValueError):
            return DEFAULT_MAX_PAGES

    # ------------------------------------------------------------------
    # Errors
    # ------------------------------------------------------------------

    def validate_response(self, response: requests.Response) -> None:
        """Map Unit4 status codes to SDK retry/fatal semantics."""
        status = response.status_code

        if status == 401:
            raise InvalidCredentialsError(
                "Unit4 authentication failed (401). Check username and password."
            )
        if status == 403:
            raise FatalAPIError(
                f"Unit4 403 Forbidden for {self.path}. Check the API user's permissions."
            )
        if status == 204:
            return
        if status == 404 and self.paginated and self._pages_fetched > 0:
            # Unit4 may signal the end of offset pagination with a 404.
            return
        if status == 429:
            wait = self._retry_after_seconds(response)
            if wait:
                logger.warning("Unit4 rate limited (429). Sleeping %.1fs.", wait)
                time.sleep(wait)
            raise RetriableAPIError(f"Unit4 rate limited (429) for {self.path}", response)
        if status in self.extra_retry_statuses or status >= 500:
            raise RetriableAPIError(
                f"Unit4 {status} for {self.path}: {response.text[:300]}", response
            )
        if 400 <= status < 500:
            raise FatalAPIError(f"Unit4 {status} for {self.path}: {response.text[:300]}")

    def _retry_after_seconds(self, response: requests.Response) -> float:
        """Parse Retry-After (seconds or HTTP date), bounded to a sane maximum."""
        value = response.headers.get("Retry-After")
        if not value:
            return 0.0
        try:
            seconds = float(value)
        except ValueError:
            try:
                seconds = parsedate_to_datetime(value).timestamp() - time.time()
            except (TypeError, ValueError):
                return 0.0
        return min(max(seconds, 0.0), MAX_RETRY_AFTER_SECONDS)

    # ------------------------------------------------------------------
    # Parsing and pagination
    # ------------------------------------------------------------------

    def _json(self, response: requests.Response) -> t.Any:
        """Decode JSON or fail loudly; a malformed page must not end a sync silently."""
        if response.status_code in (204, 404):
            return []
        if not response.content:
            raise FatalAPIError(
                f"Unit4 returned an empty response body for {self.path}."
            )
        try:
            return response.json()
        except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as exc:
            raise FatalAPIError(
                f"Unit4 returned non-JSON for {self.path}: {response.text[:300]}"
            ) from exc

    def _list_records(self, response: requests.Response) -> list[dict]:
        payload = self._json(response)
        if not isinstance(payload, list):
            raise FatalAPIError(
                f"Unit4 {self.path} returned {type(payload).__name__}, expected a list."
            )
        if any(not isinstance(record, dict) for record in payload):
            raise FatalAPIError(f"Unit4 {self.path} returned a non-object list item.")
        return payload

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Yield records from a top-level JSON array."""
        yield from self._list_records(response)

    def request_records(self, context) -> t.Iterable[dict]:
        """Reset pagination guards for each independent paging run."""
        self._last_page_fingerprint = None
        self._pages_fetched = 0
        yield from super().request_records(context)

    def get_url_params(self, context, next_page_token) -> dict[str, t.Any]:
        """Return query params; subclasses extend via extra_url_params()."""
        params: dict[str, t.Any] = {}
        if self.paginated:
            params["limit"] = self.page_size
            params["offset"] = next_page_token or 0
        params.update(self.extra_url_params(context))
        return params

    def extra_url_params(self, context) -> dict[str, t.Any]:
        """Endpoint-specific params such as filter and select."""
        return {}

    def prepare_request(self, context, next_page_token) -> requests.PreparedRequest:
        """Percent-encode the query string (spaces as %20, not '+') for OData-style filters."""
        url = self.get_url(context)
        params = self.get_url_params(context, next_page_token)
        if params:
            url = f"{url}?{urlencode(params, quote_via=quote, safe=chr(39) + ',()')}"
        return self.build_prepared_request(
            method=self.rest_method,
            url=url,
            params={},
            headers=self.http_headers,
            json=self.prepare_request_payload(context, next_page_token),
        )

    def get_next_page_token(self, response, previous_token) -> int | None:
        """Advance offset by records received; stop on a short, empty, or repeated page."""
        if not self.paginated or response.status_code in (204, 404):
            return None

        page = self._list_records(response)
        if len(page) < self.page_size:
            return None

        fingerprint = tuple(
            tuple(r.get(k) for k in self.primary_keys) for r in page
        )
        if fingerprint == self._last_page_fingerprint:
            raise FatalAPIError(
                f"Unit4 {self.path} returned the same page twice at offset "
                f"{previous_token}; the endpoint may be ignoring 'offset'."
            )
        self._last_page_fingerprint = fingerprint

        self._pages_fetched += 1
        if self._pages_fetched >= self.max_pages:
            raise FatalAPIError(
                f"Unit4 {self.path} exceeded max_pages={self.max_pages}; aborting."
            )

        return (previous_token or 0) + len(page)

    def _write_state_message(self) -> None:
        """Write HotGlue-compatible state without partition noise."""
        tap_state = self.tap_state
        if tap_state and tap_state.get("bookmarks"):
            for stream_state in tap_state["bookmarks"].values():
                if isinstance(stream_state, dict):
                    stream_state.pop("partitions", None)
        singer.write_message(StateMessage(value=tap_state))
