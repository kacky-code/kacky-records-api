import requests

from kacky_records_api.record_aggregators.nadeo.authentication import (
    AuthenticationHandler,
)


class NadeoServices:
    def __init__(self, user: str, pwd: str, accounttype: str, user_agent: str):
        self._auth_handler = AuthenticationHandler(user, pwd, accounttype, user_agent)

    def _request_executor(self, url):
        self._auth_handler.services_auth_status()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": self._auth_handler.useragent,
            "Authorization": f"nadeo_v1 t={self._auth_handler.tokens['NadeoServices'].access_token}",
        }
        result = requests.get(url, headers=headers)
        return result.json()

    def get_account_display_name(self, account_id: str):
        url = f"https://prod.trackmania.core.nadeo.online/accounts/displayNames/?accountIdList={account_id}"
        return self._request_executor(url)
