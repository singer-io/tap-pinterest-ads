from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from base import PinterestAdsBase


class PinterestAdsAllFieldsTest(AllFieldsTest, PinterestAdsBase):
    """Standard All Fields Test"""
    MISSING_FIELDS = {
        "sequence_templates": {"negaitveReplyCount"},
        "tasks": {"taskThemeId"},
    }

    @staticmethod
    def name():
        return "tt_pinterest_ads_all_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)
