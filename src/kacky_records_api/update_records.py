import logging
from datetime import datetime as dt
from threading import Lock
from typing import Dict, Union

from nadeo_api import NadeoAPI

from kacky_records_api.db_operators.operators import DBConnection
from kacky_records_api.record_aggregators.kackiest_kacky_db import (
    KackiestKacky_KackyRecords,
)

# TODO use kacky_records_api.record_aggregators.kacky_reloaded_db.KackyReloaded_KackyRecords
from kacky_records_api.record_aggregators.tmnf_exchange import TmnfTmxApi

kackiest_update_counter = 1
reloaded_update_counter = 1
reloaded_update_counter_onlyevent = 1
kackiest_kacky_lock = Lock()
kacky_reloaded_lock = Lock()
kacky_reloaded_lock_onlyevent = Lock()


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
    logger = logging.getLogger(config["logger_name"])
    logger.setLevel(eval("logging." + config["loglevel"]))
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
                try:
                    date = (
                        dt.strptime(data["lastactivity"], "%Y-%m-%dT%H:%M:%S.%f")
                        if "lastactivity" in data
                        else dt.fromtimestamp(0)
                    )
                except ValueError:
                    # This can happen if uploaded on 0th millisec - .%f does not exist in that case
                    date = (
                        dt.strptime(data["lastactivity"], "%Y-%m-%dT%H:%M:%S")
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
    elif src == "krdb":
        query = basequery + "tm_uid = ?;"
        for candidate in candidates:
            logger.debug(candidate)
            check_score = backend_db.fetchall(
                query, (candidate["score"], candidate["tm_uid"])
            )
            logger.debug(check_score)
            if check_score:
                update_elements.append(
                    build_score(
                        candidate["score"],
                        candidate["date"],
                        "KRDB",
                        login=candidate.get("login", ""),
                        nick=candidate.get("nick", ""),
                        tm_uid=candidate["tm_uid"],
                        kid=candidate["kid"],
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
        logger.error("Could not update wrs. Failed to acquire updating_records_lock!")
        return
    global kackiest_update_counter

    logger.info("updating KK wrs log")
    kk_upd = KackiestKacky_KackyRecords(secrets)
    tmx_upd = TmnfTmxApi(config)

    recent_wrs_kk_db = kk_upd.get_recent_world_records()
    # recent_wrs_kk_db = kk_upd.get_all_world_records()
    recent_wrs_kk_tmx = tmx_upd.get_activity()
    # all_tmx = tmx_upd.get_kacky_wrs()
    # every 10 min check dedimania records
    # if kackiest_update_counter == config["tmx_update_frequency"] - 1:
    #     all_dedi_wrs = tmx_upd.get_all_kacky_dedimania_wrs()
    # else:
    #     all_dedi_wrs = {}

    update_wrs_kk = []
    update_wrs_kk += check_new_scores(recent_wrs_kk_db, "kkdb", config, secrets)
    update_wrs_kk += check_new_scores(recent_wrs_kk_tmx, "tmx", config, secrets)
    # update_wrs_kk += check_new_scores(all_tmx, "tmx", config, secrets)
    # update_wrs_kk += check_new_scores(all_dedi_wrs, "dedi", config, secrets)
    update_wrs_kk_dedup = dedup_new_scores(update_wrs_kk)

    # set up connection to backend database
    backend_db = DBConnection(config, secrets)

    for e in update_wrs_kk_dedup:
        logger.info(f"updating in DB: {e}")
        # get old date
        query = f"""
                    SELECT date
                    FROM worldrecords AS wr
                    LEFT JOIN maps ON wr.map_id = maps.id
                    WHERE maps.{'tmx_id' if 'tmx_id' in e else 'tm_uid'} = ?
                """
        old_date = backend_db.fetchall(
            query, (e["tmx_id"] if "tmx_id" in e else e["tm_uid"],)
        )[0][0]
        days_diff = abs((dt.strptime(e["date"], "%Y-%m-%d %H:%M:%S") - old_date).days)
        # writing discord announcement FIRST
        # It should be the other way around so that a discord announcement quasi confirms sucessful storing of new wr.
        # But doing it this way allows to calculate `time_diff` in-place
        query_discord = f"""
                    UPDATE worldrecords_discord_notify AS wr_not
                    LEFT JOIN worldrecords AS wr
                        ON wr_not.id = wr.id
                    LEFT JOIN maps
                        ON wr.map_id = maps.id
                    SET notified = 0, time_diff = wr.score - ?, days_passed = ?
                    WHERE maps.{'tmx_id' if 'tmx_id' in e else 'tm_uid'} = ?;
                    """
        backend_db.execute(
            query_discord,
            (e["score"], days_diff, e["tmx_id"] if "tmx_id" in e else e["tm_uid"]),
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


def update_wrs_kacky_reloaded(
    config, secrets, excluded_event: int = None, only_event: int = None
):
    # Set up logging
    logger = logging.getLogger(config["logger_name"])
    logger.setLevel(eval("logging." + config["loglevel"]))

    if not only_event:
        kacky_reloaded_lock.acquire(timeout=2)
        if not kacky_reloaded_lock:
            logger.error("Could not update wrs. Failed to acquire kacky_reloaded_lock!")
            return
    if only_event:
        kacky_reloaded_lock_onlyevent.acquire(timeout=2)
        if not kacky_reloaded_lock_onlyevent:
            logger.error(
                "Could not update wrs. Failed to acquire kacky_reloaded_lock_onlyevent!"
            )
            return
    global reloaded_update_counter, reloaded_update_counter_onlyevent

    logger.info("updating KR wrs log")

    try:
        tm20_api = NadeoAPI(
            secrets["ubisoft_account"],
            secrets["ubisoft_passwd"],
            secrets["ubisoft-user-agent"],
        )
    except KeyError as ke:
        raise ValueError("Bad Value for 'credentials_type' in secrets.yaml") from ke

    club_campaings = tm20_api.nadeo_live_services.get_club_campaigns(
        config["kacky_reloaded_club_id"]
    )

    """
    # update all kr maps at once
    kr_maps = []
    for campaign in club_campaings:
        print(campaign["name"])
        campaing_info = tm20_api.nadeo_live_services.get_campaign(
            config["kacky_reloaded_club_id"],
            campaign["campaignId"]
        )
        for playlist in campaing_info["campaign"]["playlist"]:
            kr_maps.append(playlist["mapUid"])
    print(kr_maps)
    print(len(kr_maps))
    """

    # update one campaign every iteration (we dont want to spam Nadeo's API too much)
    while True:
        if not only_event:
            campaing_info = tm20_api.nadeo_live_services.get_campaign(
                config["kacky_reloaded_club_id"],
                club_campaings[reloaded_update_counter]["campaignId"],
            )
        if only_event:
            campaing_info = tm20_api.nadeo_live_services.get_campaign(
                config["kacky_reloaded_club_id"],
                club_campaings[reloaded_update_counter_onlyevent]["campaignId"],
            )
        campaign_maps = campaing_info["campaign"]["playlist"]
        if excluded_event or only_event:
            if excluded_event:
                if f"KR{excluded_event} MAPS" in campaing_info["name"]:
                    reloaded_update_counter = (reloaded_update_counter + 1) % len(
                        club_campaings
                    )
                    continue
            if only_event:
                if f"KR{only_event} MAPS" not in campaing_info["name"]:
                    reloaded_update_counter_onlyevent = (
                        reloaded_update_counter_onlyevent + 1
                    ) % len(club_campaings)
                    continue
        break

    logger.info(campaing_info["name"])

    scores = []
    for cmap in campaign_maps:
        mapscore_dbg = "uninitialized"
        player_dbg = "uninitialized"
        try:
            mapscore_dbg = tm20_api.nadeo_live_services.get_worldrecord_for_map(
                cmap["mapUid"]
            )
            mapscore = mapscore_dbg["tops"][0]["top"][0]
            logger.debug(mapscore["accountId"])
            webidentity = tm20_api.nadeo_services.get_account_webidentities(
                (mapscore["accountId"],), merge_results=True
            )[0]
            logger.debug(webidentity)
            player_dbg = tm20_api.ubisoft_services.get_player_profile(
                webidentity["uplay_uid"]
            )
            logger.debug(player_dbg)
            player = player_dbg["profiles"][0]
        except IndexError:
            # usually means no wr yet
            continue
        except Exception as e:
            logger.error(f"Error in updating data from Nadeo! {e}")
            logger.error(cmap)
            logger.error(mapscore_dbg)
            logger.error(player_dbg)
            continue
        #            kacky_reloaded_lock.release()
        #            return
        scores.append(
            build_score(
                mapscore["score"],
                dt.now(),
                "NADO",
                login=player["nameOnPlatform"],
                tm_uid=cmap["mapUid"],
            )
        )

    update_scores = check_new_scores(scores, "nado", config, secrets)
    logger.debug("=========================================================")
    logger.debug(update_scores)

    # no deduplication needed, as we have only one data source

    # set up connection to backend database
    backend_db = DBConnection(config, secrets)

    for new_wr in update_scores:
        logger.info(f"updating in DB: {new_wr}")
        # get old date
        query = "SELECT date FROM worldrecords AS wr LEFT JOIN maps ON wr.map_id = maps.id WHERE maps.tm_uid = ?"
        # backend_db.execute()
        old_date = backend_db.fetchall(query, (new_wr["tm_uid"],))[0][0]
        days_diff = abs(
            dt.strptime(new_wr["date"], "%Y-%m-%d %H:%M:%S") - old_date
        ).days
        # writing discord announcement FIRST
        # It should be the other way around so that a discord announcement quasi confirms sucessful storing of new wr.
        # But doing it this way allows to calculate `time_diff` in-place
        query_discord = """
                    UPDATE worldrecords_discord_notify AS wr_not
                    LEFT JOIN worldrecords AS wr
                        ON wr_not.id = wr.id
                    LEFT JOIN maps
                        ON wr.map_id = maps.id
                    SET notified = 0, time_diff = wr.score - ?, days_passed = ?
                    WHERE maps.tm_uid = ?;
                    """
        backend_db.execute(
            query_discord, (new_wr["score"], days_diff, new_wr["tm_uid"])
        )
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

    if not only_event:
        reloaded_update_counter = (reloaded_update_counter + 1) % len(club_campaings)
    if only_event:
        reloaded_update_counter_onlyevent = (
            reloaded_update_counter_onlyevent + 1
        ) % len(club_campaings)

    if kacky_reloaded_lock and not only_event:
        kacky_reloaded_lock.release()
    if kacky_reloaded_lock_onlyevent and only_event:
        kacky_reloaded_lock_onlyevent.release()


def restore_wr_after_reset(config, secrets):
    logger = logging.getLogger(config["logger_name"])
    logger.setLevel(eval("logging." + config["loglevel"]))
    logger.info("Checking for reset WRs")

    backend_db = DBConnection(config, secrets)
    # keep this minimal, because this most of the time will be empty
    reset_check_query = """SELECT id FROM worldrecords WHERE score = 1;"""
    reset_candidates = backend_db.fetchall(reset_check_query, ())
    if not reset_candidates:
        logger.debug("No reset WRs need updating")

    # get information of reset map
    reset_map_query = """
        SELECT map_id, tmx_id, tm_uid, kacky_id, type
        FROM worldrecords
        INNER JOIN maps ON maps.id = worldrecords.map_id
        INNER JOIN events ON events.id = maps.kackyevent
        WHERE score = 1;
    """
    reset_maps = backend_db.fetchall(reset_map_query, ())

    for reset_map in reset_maps:
        if reset_map[4].upper() == "KK":
            # this is inefficient af, but will run so rarely, should be fine
            db_wr = KackiestKacky_KackyRecords(secrets).get_all_world_records()[
                reset_map[3]
            ]
            tmx_wr = TmnfTmxApi(config).get_map_wr(reset_map[1])
            if db_wr["score"] < tmx_wr[1]:
                new_wr = {
                    "score": db_wr["score"],
                    "login": db_wr["login"],
                    "nick": db_wr["nick"],
                    "source": "KKDB",
                    "date": db_wr["date"],
                }
            else:
                new_wr = {"score": tmx_wr[1], "nick": tmx_wr[0], "source": "TMX"}
        else:
            raise NotImplementedError
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
                new_wr["date"]
                if "date" in new_wr
                else dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                reset_map[2],
            ),
        )


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
