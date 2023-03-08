import json
import time

import requests

from kacky_records_api.record_aggregators.nadeo.token_container import TokenContainer

AUDIENCES = ["NadeoServices", "NadeoLiveServices", "NadeoClubServices"]


class AuthenticationHandler:
    """
    Singleton. Handles all auth tokens and their updates for queries to Nadeo services
    """

    def __new__(cls, user: str, pwd: str, accounttype: str, user_agent: str):
        if not hasattr(cls, "_instance"):
            print("creating new AuthenticationHandler")
            cls._instance = super(AuthenticationHandler, cls).__new__(cls)
        return cls._instance

    def __init__(self, user: str, pwd: str, accounttype: str, user_agent: str):
        if not hasattr(self, "_nadeo_tokens"):
            self.__singleton__init__(user, pwd, accounttype, user_agent)

    def __singleton__init__(
        self, user: str, pwd: str, accounttype: str, user_agent: str
    ):
        self._credentials = (user, pwd)
        self._accounttype = accounttype
        self.useragent = user_agent
        if accounttype == "dedicated":
            self._nadeo_api_url = "https://prod.trackmania.core.nadeo.online/v2/authentication/token/basic"
            self._auth_protocol = "nadeo_v1"
        elif accounttype == "account":
            self._nadeo_api_url = "https://prod.trackmania.core.nadeo.online/v2/authentication/token/ubiservices"
            self._auth_protocol = "ubi_v1"
        self._ubi_auth = None
        self._nadeo_tokens = None
        self._services_login_initial()

    def _ubisoft_account_login(self):
        """
        Uses user provided user/password credentials to obtain an Ubisoft authentication ticket.

        Returns
        -------
        dict[str, str]:
            Authentication response
        """
        # Setup session to use for Ubisoft Auth and set account credentials
        session = requests.Session()
        session.auth = self._credentials

        # Headers for Ubisoft Auth
        # TODO: take User-Agent from config
        headers = {
            "Ubi-AppId": "86263886-327a-4328-ac69-527f0d20a237",
            "Content-Type": "application/json",
            "User-Agent": self.useragent,
        }

        # Post to Ubisoft
        r = session.post(
            "https://public-ubiservices.ubi.com/v3/profiles/sessions", headers=headers
        )

        # Check response
        if r.status_code != 200:
            raise ValueError("Authentication to Ubisoft Services failed!")
        session.close()
        return r.json()

    def _services_login_initial(self):
        """
        Initial login into Nadeo's different services (as defined in AUDIENCES).
        Initial login requires obtaining a ticket from Ubisoft, if Ubisoft credentials were provided.

        Returns
        -------

        """
        session = requests.Session()
        if self._accounttype == "dedicated":
            session.auth = self._credentials
        elif self._accounttype == "account":
            self._ubi_auth = self._ubisoft_account_login()

        nadeo_tokens = {}
        for audience in AUDIENCES:
            body = json.dumps({"audience": audience})
            headers = {
                "Content-Type": "application/json",
            }
            if self._accounttype == "account":
                headers[
                    "Authorization"
                ] = f"{self._auth_protocol} t={self._ubi_auth['ticket']}"
            request_result = session.post(
                self._nadeo_api_url, data=body, headers=headers
            )
            if request_result.status_code != 200:
                raise ValueError(
                    "Authentication to Nadeo with Dedicated Account failed!"
                )
            request_result_dict = request_result.json()
            nadeo_tokens[audience] = TokenContainer(
                request_result_dict["accessToken"], request_result_dict["refreshToken"]
            )
        self._nadeo_tokens = nadeo_tokens
        session.close()

    def _services_login_refresh(self):
        refresh_token_valid = map(
            lambda t: t.check_refreshtoken_valid(), self._nadeo_tokens.values()
        )
        if 0 in refresh_token_valid or 2 in refresh_token_valid:
            # at least one refresh token is expired or can be renewed. Reinitialize, as we need new Ubisoft Auth Ticket
            self._services_login_initial()
        else:
            # all refresh tokens valid. Use to refresh access tokens
            url = "https://prod.trackmania.core.nadeo.online/v2/authentication/token/refresh"
            for audience in AUDIENCES:
                body = json.dumps({"audience": audience})
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"nadeo_v1 t={self._nadeo_tokens[audience].refresh_token}",
                }
                request_result = requests.post(url, data=body, headers=headers)
                if request_result.status_code != 200:
                    raise ValueError(f"Refresh of '{audience}' token failed!")
                request_result_dict = request_result.json()
                self._nadeo_tokens[audience] = TokenContainer(
                    request_result_dict["accessToken"],
                    request_result_dict["refreshToken"],
                )

    def services_auth_status(self):
        # Initial auth required, var never got assigned a value
        if not self._nadeo_tokens:
            self._services_login_initial()

        # check validity of access tokens
        access_token_valid = list(
            map(lambda t: t.check_accesstoken_valid(), self._nadeo_tokens.values())
        )
        if 0 in access_token_valid or 2 in access_token_valid:
            # at least one token is expired or can be renewed
            self._services_login_refresh()
        # else:
        # all tokens valid and dont need renewal

    @property
    def tokens(self):
        return {audience: token for audience, token in self._nadeo_tokens.items()}


if __name__ == "__main__":
    a = AuthenticationHandler("corkscrew@live.de", "Sp33dboot", "account")
    # a = AuthenticationHandler("cork-kacky-checker", "Pa>Z!Ks~bdtxOG.r", "dedicated")
    minutes = 0
    loop_minutes = 3
    while True:
        print(minutes)
        print("rat times:")
        print(list(map(lambda t: t._acc_payload["rat"], a._nadeo_tokens.values())))
        a.services_auth_status()
        time.sleep(loop_minutes * 60)
        minutes += loop_minutes
