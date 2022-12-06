import logging

import requests


class TmnfTmxApi:
    BASEURL = "https://tmnf.exchange/api/"

    def __init__(self, config):
        self._config = config
        self._logger = logging.getLogger(config["logger_name"])

    def get_wr(self, tmxid):
        urn = (
            f"replays?trackId={tmxid}&best=1&count=1&fields=User.Name"
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
            m["TrackName"]
            .split("#")[1]
            .replace("\u2013", "-"): {
                "tid": m["TrackId"],
                "wrscore": m["WRReplay"]["ReplayTime"],
                "wruser": m["WRReplay"]["User"]["Name"],
            }
            for m in r.json()["Results"]
        }

    def get_activity(self, raw=False):
        # https://api2.mania.exchange/Method/Index/43
        urn = (
            "tracks?author=%23masters+of+kacky&count=100&order1=10&fields=TrackId"
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
            m["TrackName"]
            .split("#")[1]
            .replace("\u2013", "-"): {
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

    def get_kacky_tmx_ids(self):
        kacky_maps = (
            "https://tmnf.exchange/api/tracks?author=%23masters+of+kacky&"
            "count=500&fields=TrackId%2CTrackName"
        )
        try:
            r = requests.get(kacky_maps, timeout=10)
        except requests.exceptions.RequestException:
            self._logger.error("Error connecting to TMX!")
            return {}
        return {
            m["TrackName"].split("#")[1].replace("\u2013", "-"): m["TrackId"]
            for m in r.json()["Results"]
        }

    def get_map_dedimania_wr(self, tmxid, kacky_id=None):
        urn_dedi = f"tracks/dedimania?trackId={tmxid}&count=1&fields=Time,Login"
        urn_info = f"tracks?id={tmxid}&fields=TrackName%5B%5D"
        try:
            r_dedi = requests.get(self.BASEURL + urn_dedi, timeout=10)
            if not kacky_id:
                r_info = requests.get(self.BASEURL + urn_info, timeout=10)
        except requests.exceptions.RequestException:
            self._logger.error(
                "Error connecting to TMX! Could not get Dedimania WR for tmxid = "
                f"{tmxid}/{kacky_id}"
            )
            return {}
        try:
            print(f"updating {kacky_id}")
            return {
                kacky_id
                if kacky_id
                else r_info.json()["Results"][0]["TrackName"]
                .split("#")[1]
                .replace("\u2013", "-"): {
                    "tid": tmxid,
                    "wrscore": r_dedi.json()["Results"][0]["Time"],
                    "wruser": r_dedi.json()["Results"][0]["Login"],
                    "lastactivity": "1970-01-01T01:00:00",
                }
            }
        except TypeError:
            self._logger.error("Track name does not look like a Kackiest Kacky name.")
            return {}
        except IndexError:
            if not r_dedi.json()["Results"]:
                self._logger.debug(f"No Dedimania records for tmxid = {tmxid}")
            else:
                self._logger.error(f"Error in request results for tmxid = {tmxid}")
            return {}

    def get_all_kacky_dedimania_wrs(self):
        # TODO: Make this parallel bc slow!
        kacky_ids = self.get_kacky_tmx_ids()
        dedi_wrs = {}
        for kid, tmx_kid in kacky_ids.items():
            dedi_wrs = {**dedi_wrs, **self.get_map_dedimania_wr(tmx_kid, kacky_id=kid)}

        return dedi_wrs


if __name__ == "__main__":
    t = TmnfTmxApi({"logger_name": "asd"})
    # a = t.get_all_kacky_dedimania_wrs()
    a = t.get_activity()
    print(a)
