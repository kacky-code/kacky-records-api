import logging
from datetime import datetime as dt
from threading import Lock
from typing import Dict, Union

from kacky_records_api.db_operators.operators import DBConnection
from kacky_records_api.record_aggregators.kackiest_kacky_db import (
    KackiestKacky_KackyRecords,
)
from kacky_records_api.record_aggregators.tmnf_exchange import TmnfTmxApi

# from kacky_records_api.record_aggregators.nadeo_webservices import NadeoAPI


update_counter = 1
updating_records_lock = Lock()


def build_score(
    kid: str,
    score: int,
    date: Union[str, dt],
    source: str,
    login: Union[str, None] = None,
    nick: Union[str, None] = None,
    tmx_id: Union[str, int, None] = None,
    tm_uid: Union[str, None] = None,
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
        "kid": kid if "#" not in kid else kid.split("#")[1].replace("\u2013", "-"),
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
    return res


def update_wrs(config, secrets):
    # Set up logging
    logger = logging.getLogger(config["logger_name"])
    logger.setLevel(eval("logging." + config["loglevel"]))

    updating_records_lock.acquire(timeout=2)
    if not updating_records_lock:
        logger.info("Could not update wrs. Failed to acquire updating_records_lock!")
        return
    global update_counter

    def check_new_scores(candidates, src: str):
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
                a = backend_db.fetchall(query, (data["wrscore"], data["tid"]))
                if a:
                    date = (
                        dt.strptime(data["lastactivity"], "%Y-%m-%dT%H:%M:%S.%f")
                        if "lastactivity" in data
                        else dt.fromtimestamp(0)
                    )
                    update_elements.append(
                        build_score(
                            kid,
                            data["wrscore"],
                            date,
                            "TMX",
                            nick=data["wruser"],
                            tmx_id=data["tid"],
                        )
                    )
        elif src == "kkdb":
            query = basequery + "tm_uid = ?;"
            for e in candidates:
                a = backend_db.fetchall(query, (e[4], e[0]))
                if a:
                    update_elements.append(
                        build_score(
                            e[1], e[4], e[5], "KKDB", login=e[6], nick=e[7], tm_uid=e[0]
                        )
                    )
        elif src == "dedi":
            query = basequery + "tmx_id = ?;"
            for kid, data in candidates.items():
                a = backend_db.fetchall(query, (data["wrscore"], data["tid"]))
                if a:
                    date = (
                        dt.strptime(data["lastactivity"], "%Y-%m-%d %H:%M:%S")
                        if "lastactivity" in data
                        else dt.fromtimestamp(0)
                    )
                    update_elements.append(
                        build_score(
                            kid,
                            data["wrscore"],
                            date,
                            "DEDI",
                            nick=data["wruser"],
                            tmx_id=data["tid"],
                        )
                    )
        elif src == "kkdb":
            query = basequery + "tm_uid = ?;"
            for e in candidates:
                a = backend_db.fetchall(query, (e[4], e[0]))
                if a:
                    update_elements.append(
                        build_score(
                            e[1], e[4], e[5], "KKDB", login=e[6], nick=e[7], tm_uid=e[0]
                        )
                    )

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

    logger.info("updating wrs log")
    kk_upd = KackiestKacky_KackyRecords(secrets)
    tmx_upd = TmnfTmxApi(config)
    # kr_nadeo_upd = NadeoAPI(secrets["ubisoft_account"], secrets["ubisoft_passwd"])
    # kr_upd = KackyReloaded_KackyRecords(secrets)

    recent_wrs_kk_db = kk_upd.get_recent_world_records()
    recent_wrs_kk_tmx = tmx_upd.get_activity()
    # all_tmx = tmx_upd.get_kacky_wrs()
    # every 10 min check dedimania records
    if update_counter == 10:
        all_dedi_wrs = tmx_upd.get_all_kacky_dedimania_wrs()
    else:
        all_dedi_wrs = {}

    update_wrs_kk = []
    update_wrs_kk += check_new_scores(recent_wrs_kk_db, "kkdb")
    update_wrs_kk += check_new_scores(recent_wrs_kk_tmx, "tmx")
    # update_wrs_kk += check_new_scores(all_tmx, "tmx")
    update_wrs_kk += check_new_scores(all_dedi_wrs, "dedi")
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
    update_counter = update_counter % 10 + 1
    updating_records_lock.release()
