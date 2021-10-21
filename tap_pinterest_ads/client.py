"""REST client handling, including PinterestStream base class."""
import requests
from typing import Any, Dict, Optional, Iterable

from memoization import cached
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_pinterest_ads.auth import PinterestAuthenticator


class PinterestStream(RESTStream):
    """pinterest stream class."""

    url_base = "https://api.pinterest.com/v5/"

    records_jsonpath = "$.items[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.bookmark"  # Or override `get_next_page_token`.

    @property
    @cached
    def authenticator(self) -> PinterestAuthenticator:
        """Return a new authenticator object."""
        return PinterestAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {'Accept': 'application/json'}
        headers["Authorization"] = "Bearer {token}".format(token=self.authenticator.access_token)
        print(headers)
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["page_size"] = 100
        if next_page_token:
            params["bookmark"] = next_page_token
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
