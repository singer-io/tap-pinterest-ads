from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest
from base import PinterestAdsBase


class PinterestAdsMinimumSelectionTest(MinimumSelectionTest, PinterestAdsBase):
    """Standard Automatic Fields Test"""

    @staticmethod
    def name():
        return "tt_pinterest_ads_auto"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)
