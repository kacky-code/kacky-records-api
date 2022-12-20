import logging
from datetime import datetime as dt
from threading import Lock
from typing import Dict, Union

from kacky_records_api.db_operators.operators import DBConnection
from kacky_records_api.record_aggregators.kackiest_kacky_db import (
    KackiestKacky_KackyRecords,
)
from kacky_records_api.record_aggregators.nadeo.nadeo_live_services import (
    NadeoLiveServices,
)
from kacky_records_api.record_aggregators.nadeo.nadeo_services import NadeoServices
from kacky_records_api.record_aggregators.tmnf_exchange import TmnfTmxApi

kackiest_update_counter = 1
reloaded_update_counter = 1
kackiest_kacky_lock = Lock()
kacky_reloaded_lock = Lock()


def build_score(
    score: int,
    date: Union[str, dt],
    source: str,
    login: Union[str, None] = None,
    nick: Union[str, None] = None,
    tmx_id: Union[str, int, None] = None,
    tm_uid: Union[str, None] = None,
    kid: str = "",
) -> Dict[str, Union[str, int]]:
    # either tm_uid or tmx_id need to be set
    if not (tm_uid or tmx_id):
        raise ValueError("Need either tm_uid or tmx_id!")
    # either login or nick need to be set
    if not (login or nick):
        raise ValueError("Need either tm_uid or tmx_id!")
    # check if source value is legal
    if source not in ["KKDB", "KRDB", "DEDI", "TMX", "NADO"]:
        raise ValueError("Bad value for source")
    # build base result dict (missing tmx_id/tm_uid
    res = {
        "score": score,
        "date": date if isinstance(date, str) else date.strftime("%Y-%m-%d %H:%M:%S"),
        "source": source,
    }
    # add missing field for tmx_id XOR tm_uid or both
    if tmx_id:
        res["tmx_id"] = tmx_id if isinstance(tmx_id, str) else str(tmx_id)
    if tm_uid:
        res["tm_uid"] = tm_uid if isinstance(tm_uid, str) else str(tm_uid)
    # add missing field for login XOR nick or both
    if login:
        res["login"] = login
    if nick:
        res["nick"] = nick
    # add kacky id if given
    if kid:
        res["kid"] = kid if "#" not in kid else kid.split("#")[1].replace("\u2013", "-")
    return res


def check_new_scores(candidates, src: str, config, secrets):
    update_elements = []

    # set up connection to backend database
    backend_db = DBConnection(config, secrets)

    basequery = """
                SELECT worldrecords.id
                FROM worldrecords
                LEFT JOIN maps
                ON worldrecords.map_id = maps.id
                WHERE score > ? AND maps.
                """
    if src == "tmx":
        query = basequery + "tmx_id = ?;"
        for kid, data in candidates.items():
            check_score = backend_db.fetchall(query, (data["wrscore"], data["tid"]))
            if check_score:
                date = (
                    dt.strptime(data["lastactivity"], "%Y-%m-%dT%H:%M:%S.%f")
                    if "lastactivity" in data
                    else dt.fromtimestamp(0)
                )
                update_elements.append(
                    build_score(
                        data["wrscore"],
                        date,
                        "TMX",
                        nick=data["wruser"],
                        tmx_id=data["tid"],
                        kid=kid,
                    )
                )
    elif src == "kkdb":
        query = basequery + "tm_uid = ?;"
        for candidate in candidates:
            check_score = backend_db.fetchall(query, (candidate[4], candidate[0]))
            if check_score:
                update_elements.append(
                    build_score(
                        candidate[4],
                        candidate[5],
                        "KKDB",
                        login=candidate[6],
                        nick=candidate[7],
                        tm_uid=candidate[0],
                        kid=candidate[1],
                    )
                )
    elif src == "dedi":
        query = basequery + "tmx_id = ?;"
        for kid, data in candidates.items():
            check_score = backend_db.fetchall(query, (data["wrscore"], data["tid"]))
            if check_score:
                date = (
                    dt.strptime(data["lastactivity"], "%Y-%m-%d %H:%M:%S")
                    if "lastactivity" in data
                    else dt.fromtimestamp(0)
                )
                update_elements.append(
                    build_score(
                        data["wrscore"],
                        date,
                        "DEDI",
                        nick=data["wruser"],
                        tmx_id=data["tid"],
                        kid=kid,
                    )
                )
    elif src == "kkdb":
        query = basequery + "tm_uid = ?;"
        for candidate in candidates:
            check_score = backend_db.fetchall(query, (candidate[4], candidate[0]))
            if check_score:
                update_elements.append(
                    build_score(
                        candidate[4],
                        candidate[5],
                        "KKDB",
                        login=candidate[6],
                        nick=candidate[7],
                        tm_uid=candidate[0],
                        kid=candidate[1],
                    )
                )
    elif src == "nado":
        query = basequery + "tm_uid = ?;"
        for candidate in candidates:
            check_score = backend_db.fetchall(
                query, (candidate["score"], candidate["tm_uid"])
            )
            if check_score:
                update_elements.append(candidate)
    return update_elements


def dedup_new_scores(candidates):
    # stole stuff from https://stackoverflow.com/a/9835819
    # quick check for duplicates
    check_lst = list(map(lambda c: c["kid"], candidates))
    seen = set()
    dupes = [x for x in check_lst if x in seen or seen.add(x)]
    dupes = list(set(dupes))
    best_score = {}
    weak_elements = []
    for d in dupes:
        for cand in candidates:
            if d == cand["kid"]:
                # kacky track length limited to 10 min
                if cand["score"] == best_score.get("score", 15 * 60 * 1000):
                    # want earliest date
                    if dt.strptime(cand["date"], "%Y-%m-%d %H:%M:%S") < dt.strptime(
                        best_score["date"], "%Y-%m-%d %H:%M:%S"
                    ):
                        weak_elements.append(best_score)
                        best_score = cand.copy()
                    else:
                        weak_elements.append(cand)
                elif cand["score"] < best_score.get("score", 15 * 60 * 1000):
                    weak_elements.append(best_score)
                    best_score = cand.copy()
                else:
                    weak_elements.append(cand)
                # candidates.remove(cand)
        best_score = {}
    for w in weak_elements:
        if w == {}:
            continue
        candidates.remove(w)
    return candidates


def update_wrs_kackiest_kacky(config, secrets):
    # Set up logging
    logger = logging.getLogger(config["logger_name"])
    logger.setLevel(eval("logging." + config["loglevel"]))

    kackiest_kacky_lock.acquire(timeout=2)
    if not kackiest_kacky_lock:
        logger.info("Could not update wrs. Failed to acquire updating_records_lock!")
        return
    global kackiest_update_counter

    logger.info("updating KK wrs log")
    kk_upd = KackiestKacky_KackyRecords(secrets)
    tmx_upd = TmnfTmxApi(config)

    recent_wrs_kk_db = kk_upd.get_recent_world_records()
    recent_wrs_kk_tmx = tmx_upd.get_activity()
    # all_tmx = tmx_upd.get_kacky_wrs()
    # every 10 min check dedimania records
    if kackiest_update_counter == config["tmx_update_frequency"] - 1:
        all_dedi_wrs = tmx_upd.get_all_kacky_dedimania_wrs()
    else:
        all_dedi_wrs = {}

    update_wrs_kk = []
    update_wrs_kk += check_new_scores(recent_wrs_kk_db, "kkdb", config, secrets)
    update_wrs_kk += check_new_scores(recent_wrs_kk_tmx, "tmx", config, secrets)
    # update_wrs_kk += check_new_scores(all_tmx, "tmx")
    update_wrs_kk += check_new_scores(all_dedi_wrs, "dedi", config, secrets)
    update_wrs_kk_dedup = dedup_new_scores(update_wrs_kk)

    # set up connection to backend database
    backend_db = DBConnection(config, secrets)

    for e in update_wrs_kk_dedup:
        query_discord = f"""
                    UPDATE worldrecords_discord_notify AS wr_not
                    LEFT JOIN worldrecords AS wr
                        ON wr_not.id = wr.id
                    LEFT JOIN maps
                        ON wr.map_id = maps.id
                    SET notified = 0, time_diff = wr.score - ?
                    WHERE maps.{'tmx_id' if 'tmx_id' in e else 'tm_uid'} = ?;
                    """
        backend_db.execute(
            query_discord, (e["score"], e["tmx_id"] if "tmx_id" in e else e["tm_uid"])
        )
        query = f"""
                 UPDATE worldrecords AS wr
                 LEFT JOIN maps
                 ON wr.map_id = maps.id
                 SET score = ?, login = ?, nickname = ?, source = ?, date = ?
                 WHERE maps.{'tmx_id' if 'tmx_id' in e else 'tm_uid'} = ?;
                 """
        backend_db.execute(
            query,
            (
                e["score"],
                e["login"] if "login" in e else "",
                e["nick"] if "nick" in e else "",
                e["source"],
                e["date"],
                e["tmx_id"] if "tmx_id" in e else e["tm_uid"],
            ),
        )
    kackiest_update_counter = (kackiest_update_counter + 1) % 10
    kackiest_kacky_lock.release()


def update_wrs_kacky_reloaded(config, secrets):
    # Set up logging
    logger = logging.getLogger(config["logger_name"])
    logger.setLevel(eval("logging." + config["loglevel"]))

    kacky_reloaded_lock.acquire(timeout=2)
    if not kacky_reloaded_lock:
        logger.info("Could not update wrs. Failed to acquire updating_records_lock!")
        return
    global reloaded_update_counter

    logger.info("updating KR wrs log")

    if secrets["credentials_type"] == "account":
        nadeo_live_serv = NadeoLiveServices(
            secrets["ubisoft_account"],
            secrets["ubisoft_passwd"],
            secrets["credentials_type"],
            secrets["ubisoft-user-agent"],
        )
    elif secrets["credentials_type"] == "dedicated":
        nadeo_live_serv = NadeoLiveServices(
            secrets["tm20_dedicated_acc"],
            secrets["tm20_dedicated_passwd"],
            secrets["credentials_type"],
            secrets["ubisoft-user-agent"],
        )
    else:
        raise ValueError("Bad Value for 'credentials_type' in secrets.yaml")

    club_campaings = nadeo_live_serv.get_club_campaigns(
        config["kacky_reloaded_club_id"]
    )

    """
    # update all kr maps at once
    kr_maps = []
    for campaign in club_campaings:
        print(campaign["name"])
        campaing_info = nadeo_live_serv.get_campaign(config["kacky_reloaded_club_id"], campaign["campaignId"])
        for playlist in campaing_info["campaign"]["playlist"]:
            kr_maps.append(playlist["mapUid"])
    print(kr_maps)
    print(len(kr_maps))
    """

    # update one campaign every iteration (we dont want to spam Nadeo's API too much)
    campaing_info = nadeo_live_serv.get_campaign(
        config["kacky_reloaded_club_id"],
        club_campaings[reloaded_update_counter]["campaignId"],
    )
    campaign_maps = campaing_info["campaign"]["playlist"]
    print(campaing_info["name"])

    if secrets["credentials_type"] == "account":
        nadeo_serv = NadeoServices(
            secrets["ubisoft_account"],
            secrets["ubisoft_passwd"],
            secrets["credentials_type"],
            secrets["ubisoft-user-agent"],
        )
    elif secrets["credentials_type"] == "dedicated":
        nadeo_serv = NadeoServices(
            secrets["tm20_dedicated_acc"],
            secrets["tm20_dedicated_passwd"],
            secrets["credentials_type"],
            secrets["ubisoft-user-agent"],
        )
    else:
        raise ValueError("Bad Value for 'credentials_type' in secrets.yaml")

    scores = []
    for cmap in campaign_maps:
        mapscore = nadeo_live_serv.get_worldrecord_for_map(cmap["mapUid"])["tops"][0][
            "top"
        ][0]
        player = nadeo_serv.get_account_display_name(mapscore["accountId"])[0]
        scores.append(
            build_score(
                mapscore["score"],
                dt.now(),
                "NADO",
                login=player["displayName"],
                tm_uid=cmap["mapUid"],
            )
        )

    update_scores = check_new_scores(scores, "nado", config, secrets)
    print(f"found {len(scores)} wrs. updating {len(update_scores)}.")
    # no deduplication needed, as we have only one data source

    # set up connection to backend database
    backend_db = DBConnection(config, secrets)

    for new_wr in update_scores:
        print(f"updating in DB: {new_wr}")
        query_discord = """
                    UPDATE worldrecords_discord_notify AS wr_not
                    LEFT JOIN worldrecords AS wr
                        ON wr_not.id = wr.id
                    LEFT JOIN maps
                        ON wr.map_id = maps.id
                    SET notified = 0, time_diff = wr.score - ?
                    WHERE maps.tm_uid = ?;
                    """
        backend_db.execute(query_discord, (new_wr["score"], new_wr["tm_uid"]))
        query = """
                 UPDATE worldrecords AS wr
                 LEFT JOIN maps
                 ON wr.map_id = maps.id
                 SET score = ?, login = ?, nickname = ?, source = ?, date = ?
                 WHERE maps.tm_uid = ?;
                 """
        backend_db.execute(
            query,
            (
                new_wr["score"],
                new_wr["login"] if "login" in new_wr else "",
                new_wr["nick"] if "nick" in new_wr else "",
                new_wr["source"],
                new_wr["date"],
                new_wr["tm_uid"],
            ),
        )

    reloaded_update_counter = (reloaded_update_counter + 1) % len(club_campaings)
    kacky_reloaded_lock.release()


if __name__ == "__main__":
    from pathlib import Path

    import yaml

    # Reading config file
    with open(Path(__file__).parents[2] / "config.yaml", "r") as conffile:
        config = yaml.load(conffile, Loader=yaml.FullLoader)

    # Read flask secret (required for flask.flash and flask_login)
    with open(Path(__file__).parents[2] / "secrets.yaml", "r") as secfile:
        secrets = yaml.load(secfile, Loader=yaml.FullLoader)
    # update_wrs_kackiest_kacky(config, secrets)
    update_wrs_kacky_reloaded(config, secrets)
