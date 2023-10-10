"""Pinterest Authentication."""
import base64

import requests

from singer_sdk.helpers._util import utc_now
from singer_sdk.authenticators import BearerTokenAuthenticator

class PinterestAuthenticator(BearerTokenAuthenticator):
    """
    Uses the access token directly instead of using Oauth
    """
    pass