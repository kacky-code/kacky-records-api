import base64
import datetime
import json


def _check_token_valid(payload):
    if payload["rat"] > datetime.datetime.now().timestamp():
        # access token is not expired and cannot be refreshed yet
        return 1
    if payload["rat"] < datetime.datetime.now().timestamp() < payload["exp"]:
        # access token is not expired, but can (and shall) be refreshed
        return 2
    if payload["exp"] < datetime.datetime.now().timestamp():
        # access token is expired
        return 0


class TokenContainer:
    def __init__(self, accessToken: str, refreshToken: str):
        self._access_token = None
        self._acc_payload = None
        self._refresh_token = None
        self._ref_payload = None
        self.access_token = accessToken
        self.refresh_token = refreshToken

    @property
    def access_token(self):
        return self._access_token

    @access_token.setter
    def access_token(self, access_token):
        self._access_token = access_token
        self._acc_payload = json.loads(
            base64.urlsafe_b64decode(
                access_token.split(".")[1]
                + "=" * (4 - len(access_token.split(".")[1]) % 4)
            )
        )

    @property
    def refresh_token(self):
        return self._refresh_token

    @refresh_token.setter
    def refresh_token(self, refresh_token):
        self._refresh_token = refresh_token
        self._ref_payload = json.loads(
            base64.urlsafe_b64decode(
                refresh_token.split(".")[1]
                + "=" * (4 - len(refresh_token.split(".")[1]) % 4)
            )
        )

    def check_accesstoken_valid(self):
        return _check_token_valid(self._acc_payload)

    def check_refreshtoken_valid(self):
        return _check_token_valid(self._ref_payload)
