import os

from tap_tester.base_suite_tests.base_case import BaseCase


class PinterestAdsBase(BaseCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.
    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).
    """
    PAGE_SIZE = 100000
    start_date = "2019-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-pinterest-ads"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.pinterest-ads"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""

        return_value = {
            "start_date": self.start_date,
            "refresh_token": os.getenv("TAP_PINTEREST_ADS_REFRESH_TOKEN"),
            "access_token": os.getenv("TAP_PINTEREST_ADS_ACCESS_TOKEN"),
        }

        return return_value

    @staticmethod
    def get_credentials():
        return {
            "refresh_token": os.getenv("TAP_PINTEREST_ADS_REFRESH_TOKEN"),
            "access_token": os.getenv("TAP_PINTEREST_ADS_ACCESS_TOKEN"),
        }

    @classmethod
    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        return {
            "ad_accounts": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_KEYS: None,
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
            "campaigns": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_KEYS: None,
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
            "ad_groups": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_KEYS: None,
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
            "ads": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_KEYS: None,
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            "ad_analytics": {
                self.PRIMARY_KEYS: {"AD_ID", "DATE"},
                self.REPLICATION_KEYS: {"DATE"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            "account_analytics": {
                self.PRIMARY_KEYS: {"AD_ACCOUNT_ID", "DATE"},
                self.REPLICATION_KEYS: {"DATE"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            }
        }

    @classmethod
    def setUpClass(cls):
        super().setUpClass(logging="Ensuring environment variables are sourced.")
        missing_envs = [
            x
            for x in [
                "TAP_PINTEREST_ADS_REFRESH_TOKEN",
                "TAP_PINTEREST_ADS_ACCESS_TOKEN"
            ]
            if os.getenv(x) is None
        ]

        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################
