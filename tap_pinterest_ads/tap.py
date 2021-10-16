"""pinterest tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_pinterest_ads.streams import (
    AdAccountStream,
    CampaignStream,
    AdGroupStream,
    AdStream,
    AdAnalyticStream
)
STREAM_TYPES = [
    AdAccountStream,
    CampaignStream,
    AdGroupStream,
    AdStream,
    AdAnalyticStream
]


class TapPinterestAds(Tap):
    """pinterest tap class."""
    name = "tap-pinterest-ads"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="App ID"
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="App secret key"
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            description="Refresh token obtained from the OAuth user flow"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="Date to start collection analytics from"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
