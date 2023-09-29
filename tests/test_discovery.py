from tap_tester.base_suite_tests.discovery_test import DiscoveryTest
from base import PinterestAdsBase


class PinterestAdsDiscoveryTest(DiscoveryTest, PinterestAdsBase):
    """Standard Discovery Test"""

    @staticmethod
    def name():
        return "tt_pinterest_ads_discovery"

    def streams_to_test(self):
        return self.expected_stream_names()
