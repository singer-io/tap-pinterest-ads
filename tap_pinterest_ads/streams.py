"""Stream type classes for tap-pinterest."""
import copy
import datetime

import requests
from typing import Any, Callable, Optional

from tap_pinterest_ads.client import PinterestStream

from singer_sdk.typing import (
    ArrayType,
    ObjectType,
    BooleanType,
    Property,
    DateTimeType,
    NumberType,
    PropertiesList,
    StringType,
)

class AdAccountStream(PinterestStream):
    name = 'ad_accounts'
    path = 'ad_accounts'
    primary_keys = ["id"]
    replication_key = None
    schema = PropertiesList(
        Property("id", StringType),
        Property("name", StringType),
        Property("owner",
            ObjectType(
                Property("username", StringType)
            )
        ),
        Property("country", StringType),
        Property("currency", StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "ad_account_id": record["id"],
        }


class CampaignStream(PinterestStream):
    name = 'campaigns'
    parent_stream_type = AdAccountStream
    path = "ad_accounts/{ad_account_id}/campaigns"
    ignore_parent_replication_keys = True
    primary_keys = ["id"]
    replication_key = None
    schema = PropertiesList(
        Property("id", StringType),
        Property("ad_account_id", StringType),
        Property("name", StringType),
        Property("status", StringType),
        Property("lifetime_spend_cap", NumberType),
        Property("daily_spend_cap", NumberType),
        Property("order_line_id", StringType),
        Property("tracking_urls",
            ObjectType(
                Property("impression", ArrayType(StringType)),
                Property("click", ArrayType(StringType)),
                Property("engagement", ArrayType(StringType)),
                Property("buyable_button", ArrayType(StringType)),
                Property("audience_verification", ArrayType(StringType)),
            )
        ),
        Property("objective_type", StringType),
        Property("created_time", NumberType),
        Property("updated_time", NumberType),
        Property("type", StringType),
    ).to_dict()


class AdGroupStream(PinterestStream):
    name = 'ad_groups'
    parent_stream_type = AdAccountStream
    path = "ad_accounts/{ad_account_id}/ad_groups"
    ignore_parent_replication_keys = True
    primary_keys = ["id"]
    replication_key = None
    schema = PropertiesList(
        Property("name", StringType),
        Property("status", StringType),
        Property("budget_in_micro_currency", NumberType),
        Property("bid_in_micro_currency", NumberType),
        Property("budget_type", StringType),
        Property("start_time", NumberType),
        Property("end_time", NumberType),
        Property("targeting_spec",  # might need this to not specify dict keys
            ObjectType(
                Property("AGE_BUCKET", ArrayType(StringType)),
                Property("LOCATION", ArrayType(StringType)),
                Property("SHOPPING_RETARGETING", ArrayType(StringType)),
                Property("AUDIENCE_INCLUDE", ArrayType(StringType)),
                Property("TARGETING_STRATEGY", ArrayType(StringType)),
                Property("GENDER", ArrayType(StringType)),
                Property("INTEREST", ArrayType(StringType)),
                Property("LOCALE", ArrayType(StringType)),
                Property("APPTYPE", ArrayType(StringType)),
                Property("AUDIENCE_EXCLUDE", ArrayType(StringType)),
            )
        ),
        Property("lifetime_frequency_cap", NumberType),
        Property("tracking_urls",
             ObjectType(
                 Property("impression", ArrayType(StringType)),
                 Property("click", ArrayType(StringType)),
                 Property("engagement", ArrayType(StringType)),
                 Property("buyable_button", ArrayType(StringType)),
                 Property("audience_verification", ArrayType(StringType)),
             )
        ),
        Property("auto_targeting_enabled", BooleanType),
        Property("placement_group", StringType),
        Property("pacing_delivery_type", StringType),
        Property("conversion_learning_mode_type", StringType),
        Property("summary_status", StringType),
        Property("feed_profile_id", StringType),
        Property("campaign_id", StringType),
        Property("billable_event", StringType),
        Property("id", StringType),
        Property("type", StringType),
        Property("ad_account_id", StringType),
        Property("created_time", NumberType),
        Property("updated_time", NumberType),
    ).to_dict()


class AdStream(PinterestStream):
    name = 'ads'
    parent_stream_type = AdAccountStream
    path = "ad_accounts/{ad_account_id}/ads"
    ignore_parent_replication_keys = True
    primary_keys = ["id"]
    replication_key = None
    schema = PropertiesList(
        Property("ad_group_id", StringType),
        Property("android_deep_link", StringType),
        Property("carousel_android_deep_links", ArrayType(StringType)),
        Property("carousel_destination_urls", ArrayType(StringType)),
        Property("carousel_ios_deep_links", ArrayType(StringType)),
        Property("click_tracking_url", StringType),
        Property("creative_type", StringType),
        Property("destination_url", StringType),
        Property("ios_deep_link", StringType),
        Property("is_pin_deleted", BooleanType),
        Property("is_removable", BooleanType),
        Property("name", StringType),
        Property("pin_id", StringType),
        Property("status", StringType),
        Property("tracking_urls",
             ObjectType(
                 Property("impression", ArrayType(StringType)),
                 Property("click", ArrayType(StringType)),
                 Property("engagement", ArrayType(StringType)),
                 Property("buyable_button", ArrayType(StringType)),
                 Property("audience_verification", ArrayType(StringType)),
             )
        ),
        Property("view_tracking_url", StringType),
        Property("ad_account_id", StringType),
        Property("campaign_id", StringType),
        Property("collection_items_destination_url_template", StringType),
        Property("created_time", NumberType),
        Property("id", StringType),
        Property("rejected_reasons", ArrayType(StringType)),
        Property("rejection_labels", ArrayType(StringType)),
        Property("review_status", StringType),
        Property("type", StringType),
        Property("updated_time", NumberType),
        Property("summary_status", StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "ad_account_id": record["ad_account_id"],
            "ad_id": record["id"]
        }


AD_ANALYTICS_COLUMNS = [
    "SPEND_IN_DOLLAR", "ECPC_IN_DOLLAR", "CTR", "ECTR", "ECPE_IN_DOLLAR",
    "ENGAGEMENT_RATE", "EENGAGEMENT_RATE", "REPIN_RATE", "CTR_2", "CAMPAIGN_ID",
    "AD_ACCOUNT_ID", "AD_GROUP_ID", "CAMPAIGN_ENTITY_STATUS",
    "CPM_IN_DOLLAR", "AD_GROUP_ENTITY_STATUS", "TOTAL_CLICKTHROUGH",
    "TOTAL_IMPRESSION_FREQUENCY", "TOTAL_ENGAGEMENT_SIGNUP",
    "TOTAL_ENGAGEMENT_CHECKOUT", "TOTAL_CLICK_SIGNUP", "TOTAL_CLICK_CHECKOUT",
    "TOTAL_VIEW_SIGNUP", "TOTAL_VIEW_CHECKOUT", "TOTAL_CONVERSIONS",
    "TOTAL_ENGAGEMENT_SIGNUP_VALUE_IN_MICRO_DOLLAR",
    "TOTAL_ENGAGEMENT_CHECKOUT_VALUE_IN_MICRO_DOLLAR",
    "TOTAL_CLICK_SIGNUP_VALUE_IN_MICRO_DOLLAR",
    "TOTAL_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR",
    "TOTAL_VIEW_SIGNUP_VALUE_IN_MICRO_DOLLAR",
    "TOTAL_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR", "TOTAL_PAGE_VISIT",
    "TOTAL_SIGNUP", "TOTAL_CHECKOUT", "TOTAL_SIGNUP_VALUE_IN_MICRO_DOLLAR",
    "TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR", "PAGE_VISIT_COST_PER_ACTION",
    "PAGE_VISIT_ROAS", "CHECKOUT_ROAS", "VIDEO_3SEC_VIEWS_2",
    "VIDEO_P100_COMPLETE_2", "VIDEO_P0_COMBINED_2", "VIDEO_P25_COMBINED_2",
    "VIDEO_P50_COMBINED_2", "VIDEO_P75_COMBINED_2", "VIDEO_P95_COMBINED_2",
    "VIDEO_MRC_VIEWS_2", "ECPV_IN_DOLLAR", "ECPCV_IN_DOLLAR",
    "ECPCV_P95_IN_DOLLAR", "TOTAL_VIDEO_3SEC_VIEWS", "TOTAL_VIDEO_P100_COMPLETE",
    "TOTAL_VIDEO_P0_COMBINED", "TOTAL_VIDEO_P25_COMBINED",
    "TOTAL_VIDEO_P50_COMBINED", "TOTAL_VIDEO_P75_COMBINED",
    "TOTAL_VIDEO_P95_COMBINED", "TOTAL_VIDEO_MRC_VIEWS",
    "TOTAL_VIDEO_AVG_WATCHTIME_IN_SECOND",
    "TOTAL_REPIN_RATE", "WEB_CHECKOUT_COST_PER_ACTION", "WEB_CHECKOUT_ROAS",
    "TOTAL_WEB_CHECKOUT", "TOTAL_WEB_CHECKOUT_VALUE_IN_MICRO_DOLLAR",
    "TOTAL_WEB_CLICK_CHECKOUT", "TOTAL_WEB_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR",
    "TOTAL_WEB_ENGAGEMENT_CHECKOUT",
    "TOTAL_WEB_ENGAGEMENT_CHECKOUT_VALUE_IN_MICRO_DOLLAR",
    "TOTAL_WEB_VIEW_CHECKOUT", "TOTAL_WEB_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR"
]

class AdAnalyticsStream(PinterestStream):
    name = 'ad_analytics'
    parent_stream_type = AdStream
    path = "ad_accounts/{ad_account_id}/ads/analytics?ad_ids={ad_id}"
    records_jsonpath = "$[*]"
    ignore_parent_replication_keys = True
    primary_keys = ["AD_ID", "DATE"]
    replication_key = "DATE"
    properties = [
        Property("AD_ID", StringType),
        Property("DATE", DateTimeType),
    ]
    properties += [Property(a, NumberType) for a in AD_ANALYTICS_COLUMNS]
    schema = PropertiesList(*properties).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        start_date = self.get_starting_timestamp(context)
        yesterday = datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=1)
        params = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': (
                min(start_date + datetime.timedelta(days=100), yesterday)
            ).strftime('%Y-%m-%d'),
            'granularity': 'DAY',
            'columns': ','.join(AD_ANALYTICS_COLUMNS),
            'page_size': 100,
        }
        if next_page_token:
            params['bookmark'] = next_page_token
        self.logger.debug(params)
        return params

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["DATE"] = datetime.datetime.strptime(row["DATE"], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ")
        return row

    
ACCOUNT_ANALYTICS_COLUMNS = [
    "AD_GROUP_ENTITY_STATUS", "CAMPAIGN_DAILY_SPEND_CAP",
    "CAMPAIGN_ENTITY_STATUS", "CAMPAIGN_ID", "CAMPAIGN_LIFETIME_SPEND_CAP", "CAMPAIGN_NAME",
    "CHECKOUT_ROAS", "CLICKTHROUGH_1", "CLICKTHROUGH_1_GROSS", "CLICKTHROUGH_2",
    "CPC_IN_MICRO_DOLLAR", "CPM_IN_DOLLAR", "CPM_IN_MICRO_DOLLAR", "CTR", "CTR_2",
    "ECPCV_IN_DOLLAR", "ECPCV_P95_IN_DOLLAR", "ECPC_IN_DOLLAR", "ECPC_IN_MICRO_DOLLAR",
    "ECPE_IN_DOLLAR", "ECPM_IN_MICRO_DOLLAR", "ECPV_IN_DOLLAR", "ECTR", "EENGAGEMENT_RATE",
    "ENGAGEMENT_1", "ENGAGEMENT_2", "ENGAGEMENT_RATE", "IDEA_PIN_PRODUCT_TAG_VISIT_1",
    "IDEA_PIN_PRODUCT_TAG_VISIT_2", "IMPRESSION_1", "IMPRESSION_1_GROSS", "IMPRESSION_2",
    "INAPP_CHECKOUT_COST_PER_ACTION", "OUTBOUND_CLICK_1", "OUTBOUND_CLICK_2",
    "PAGE_VISIT_COST_PER_ACTION", "PAGE_VISIT_ROAS", "PAID_IMPRESSION", "PIN_ID", "REPIN_1",
    "REPIN_2", "REPIN_RATE", "SPEND_IN_DOLLAR", "SPEND_IN_MICRO_DOLLAR", "TOTAL_CHECKOUT",
    "TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR", "TOTAL_CLICKTHROUGH", "TOTAL_CLICK_CHECKOUT",
    "TOTAL_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR", "TOTAL_CLICK_SIGNUP",
    "TOTAL_CLICK_SIGNUP_VALUE_IN_MICRO_DOLLAR", "TOTAL_CONVERSIONS", "TOTAL_CUSTOM",
    "TOTAL_ENGAGEMENT", "TOTAL_ENGAGEMENT_CHECKOUT",
    "TOTAL_ENGAGEMENT_CHECKOUT_VALUE_IN_MICRO_DOLLAR", "TOTAL_ENGAGEMENT_SIGNUP",
    "TOTAL_ENGAGEMENT_SIGNUP_VALUE_IN_MICRO_DOLLAR", "TOTAL_IDEA_PIN_PRODUCT_TAG_VISIT",
    "TOTAL_IMPRESSION_FREQUENCY", "TOTAL_IMPRESSION_USER", "TOTAL_LEAD", "TOTAL_PAGE_VISIT",
    "TOTAL_REPIN_RATE", "TOTAL_SIGNUP", "TOTAL_SIGNUP_VALUE_IN_MICRO_DOLLAR",
    "TOTAL_VIDEO_3SEC_VIEWS", "TOTAL_VIDEO_AVG_WATCHTIME_IN_SECOND", "TOTAL_VIDEO_MRC_VIEWS",
    "TOTAL_VIDEO_P0_COMBINED", "TOTAL_VIDEO_P100_COMPLETE", "TOTAL_VIDEO_P25_COMBINED",
    "TOTAL_VIDEO_P50_COMBINED", "TOTAL_VIDEO_P75_COMBINED", "TOTAL_VIDEO_P95_COMBINED",
    "TOTAL_VIEW_CHECKOUT", "TOTAL_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR", "TOTAL_VIEW_SIGNUP",
    "TOTAL_VIEW_SIGNUP_VALUE_IN_MICRO_DOLLAR", "TOTAL_WEB_CHECKOUT",
    "TOTAL_WEB_CHECKOUT_VALUE_IN_MICRO_DOLLAR", "TOTAL_WEB_CLICK_CHECKOUT",
    "TOTAL_WEB_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR", "TOTAL_WEB_ENGAGEMENT_CHECKOUT",
    "TOTAL_WEB_ENGAGEMENT_CHECKOUT_VALUE_IN_MICRO_DOLLAR", "TOTAL_WEB_VIEW_CHECKOUT",
    "TOTAL_WEB_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR", "VIDEO_3SEC_VIEWS_2", "VIDEO_LENGTH",
    "VIDEO_MRC_VIEWS_2", "VIDEO_P0_COMBINED_2", "VIDEO_P100_COMPLETE_2", "VIDEO_P25_COMBINED_2",
    "VIDEO_P50_COMBINED_2", "VIDEO_P75_COMBINED_2", "VIDEO_P95_COMBINED_2", "WEB_CHECKOUT_COST_PER_ACTION",
    "WEB_CHECKOUT_ROAS"
]

class AccountAnalyticsStream(PinterestStream):
    name = 'account_analytics'
    parent_stream_type = AdAccountStream
    path = "ad_accounts/{ad_account_id}/analytics"
    records_jsonpath = "$[*]"
    ignore_parent_replication_keys = True
    primary_keys = ["DATE"]
    replication_key = "DATE"
    properties = [
        Property("AD_ACCOUNT_ID", StringType),
        Property("DATE", DateTimeType),
    ]
    properties += [Property(a, NumberType) for a in ACCOUNT_ANALYTICS_COLUMNS]
    schema = PropertiesList(*properties).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        start_date = self.get_starting_timestamp(context)
        yesterday = datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=1)
        params = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': (
                min(start_date + datetime.timedelta(days=100), yesterday)
            ).strftime('%Y-%m-%d'),
            'granularity': 'DAY',
            'columns': ','.join(ACCOUNT_ANALYTICS_COLUMNS),
            'page_size': 100,
        }
        if next_page_token:
            params['bookmark'] = next_page_token
        self.logger.debug(params)
        return params
    
    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["DATE"] = datetime.datetime.strptime(row["DATE"], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ")
        return row
