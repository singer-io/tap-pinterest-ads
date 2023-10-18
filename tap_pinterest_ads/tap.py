"""pinterest tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_pinterest_ads.utils import SingerCompatibilityMixin

from tap_pinterest_ads.streams import (
    AdAccountStream,
    CampaignStream,
    AdGroupStream,
    AdStream,
    AdAnalyticsStream,
    AccountAnalyticsStream
)
STREAM_TYPES = [
    AdAccountStream,
    CampaignStream,
    AdGroupStream,
    AdStream,
    AdAnalyticsStream,
    AccountAnalyticsStream
]


class TapPinterestAds(SingerCompatibilityMixin, Tap):
    """pinterest tap class."""
    name = "tap-pinterest-ads"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=False,
            description="App ID"
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=False,
            description="App secret key"
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=False,
            description="Refresh token obtained from the OAuth user flow"
        ),
        th.Property(
            "access_token",
            th.StringType,
            required=False,
            description="Refresh token obtained from the OAuth user flow"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2019-10-17T00:00:00Z",
            description="Date to start collection analytics from"
        ),
        th.Property(
            "is_backfilled",
            th.BooleanType,
            default=False,
            description="Set to True once backfilled in order to reduce API calls per day"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
