from tap_tester.base_suite_tests.sync_canary_test import SyncCanaryTest
from base import PinterestAdsBase


class PinterestAdsSyncCanaryTest(SyncCanaryTest, PinterestAdsBase):
    """Standard Sync Canary Test"""

    @staticmethod
    def name():
        return "tt_pinterest_ads_sync_canary_test"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)
