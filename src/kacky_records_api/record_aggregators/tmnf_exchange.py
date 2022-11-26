import logging

import requests


class TmnfTmxApi:
    BASEURL = "https://tmnf.exchange/api/"

    def __init__(self, config):
        self._config = config
        self._logger = logging.getLogger(config["logger_name"])

    def get_wr(self, trackid):
        urn = (
            f"replays?trackId={trackid}&best=1&count=3&fields=User.Name"
            "%2CReplayTime%2CPosition"
        )
        try:
            r = requests.get(self.BASEURL + urn, timeout=10)
        except requests.exceptions.RequestException:
            self._logger.error("Error connecting to TMX!")
            return {}
        return r.json()

    def get_kacky_wrs(self, raw=False):
        # https://tmnf.exchange/api/tracks?author=%23masters+of+kacky&count=40&
        # fields=TrackId%2CTrackName%2CAuthors%5B%5D%2CTags%5B%5D%2CAuthorTime%
        # 2CRoutes%2CDifficulty%2CEnvironment%2CCar%2CPrimaryType%2CMood%2CAwar
        # ds%2CHasThumbnail%2CImages%5B%5D%2CIsPublic%2CWRReplay.User.UserId%2C
        # WRReplay.User.Name%2CWRReplay.ReplayTime%2CWRReplay.ReplayScore%2CRep
        # layType%2CUploader.UserId%2CUploader.Name"
        urn = (
            "tracks?author=%23masters+of+kacky&count=99999&fields=TrackId"
            "%2CTrackName%2CWRReplay.User.Name%2CWRReplay.ReplayTime"
        )
        try:
            r = requests.get(self.BASEURL + urn, timeout=10)
        except requests.exceptions.RequestException:
            self._logger.error("Error connecting to TMX!")
            return {}
        if raw:
            return r.json()
        return {
            m["TrackName"].split("#")[1]: {
                "tid": m["TrackId"],
                "wrscore": m["WRReplay"]["ReplayTime"],
                "wruser": m["WRReplay"]["User"]["Name"],
            }
            for m in r.json()["Results"]
        }

    def get_activity(self, raw=False):
        # https://api2.mania.exchange/Method/Index/43
        urn = (
            "tracks?author=%23masters+of+kacky&count=10&order1=10&fields=TrackId"
            "%2CTrackName%2CWRReplay.User.Name%2CWRReplay.ReplayTime%2CActivityAt"
        )
        try:
            r = requests.get(self.BASEURL + urn, timeout=10)
        except requests.exceptions.RequestException:
            self._logger.error("Error connecting to TMX!")
            return {}
        if raw:
            return r.json()
        return {
            m["TrackName"].split("#")[1]: {
                "tid": m["TrackId"],
                "wrscore": m["WRReplay"]["ReplayTime"],
                "wruser": m["WRReplay"]["User"]["Name"],
                "lastactivity": m["ActivityAt"],
            }
            for m in r.json()["Results"]
        }

    def get_map_thumbnail(self, tmxid):
        url = f"https://tmnf.exchange/trackshow/{tmxid}/image/1"
        return url

    def get_tmx_ids(self):
        kacky_maps = (
            "https://tmnf.exchange/api/tracks?author=%23masters+of+kacky&"
            "count=500&fields=TrackId%2CTrackName"
        )
        try:
            r = requests.get(kacky_maps, timeout=10)
        except requests.exceptions.RequestException:
            self._logger.error("Error connecting to TMX!")
            return {}
        return {m["TrackName"].split("#")[1]: m["TrackId"] for m in r.json()["Results"]}
