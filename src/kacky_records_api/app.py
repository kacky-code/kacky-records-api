import atexit
import datetime
import logging
import os
from pathlib import Path
from threading import Lock

import flask
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from flask_cors import CORS

from kacky_records_api.db_operators.operators import DBConnection
from kacky_records_api.record_aggregators.kackiest_kacky_db import (
    KackiestKacky_KackyRecords,
)
from kacky_records_api.record_aggregators.tmnf_exchange import TmnfTmxApi

# from kacky_records_api.record_aggregators.nadeo_webservices import NadeoAPI

app = flask.Flask(__name__)
CORS(app)
app.config["CORS_HEADERS"] = "Content-Type"
logger = None
update_counter = 1
updating_records_lock = Lock()


def update_wrs():
    updating_records_lock.acquire(timeout=2)
    if not updating_records_lock:
        return
    global update_counter

    def check_new_scores(candidates, src: str):
        update_elements = []
        if src == "tmx":
            query = """
                    SELECT *
                    FROM worldrecords
                    LEFT JOIN maps
                    ON worldrecords.map_id = maps.id
                    WHERE score > ?
                        AND maps.tmx_id = ?;
                    """
            for kid, data in candidates.items():
                a = backend_db.fetchall(query, (data["wrscore"], data["tid"]))
                if a:
                    date = (
                        datetime.datetime.strptime(
                            data["lastactivity"], "%Y-%m-%dT%H:%M:%S.%f"
                        )
                        if "lastactivity" in data
                        else datetime.datetime.fromtimestamp(0)
                    )
                    upd = {
                        "kid": kid,
                        "score": data["wrscore"],
                        "login": "",
                        "nick": data["wruser"],
                        "tmx_id": str(data["tid"]),
                        "date": date.strftime("%Y-%m-%d %H:%M:%S"),
                        "source": "TMX",
                    }
                    update_elements.append(upd)
        elif src == "kkdb":
            query = """
                    SELECT *
                    FROM worldrecords
                    LEFT JOIN maps
                    ON worldrecords.map_id = maps.id
                    WHERE score > ?
                        AND maps.tm_uid = ?;
                    """
            for e in candidates:
                a = backend_db.fetchall(query, (e[4], e[0]))
                if a:
                    upd = {
                        "kid": e[1].split("#")[1],
                        "score": e[4],
                        "login": e[6],
                        "nick": e[7],
                        "tm_uid": e[0],
                        "date": e[5].strftime("%Y-%m-%d %H:%M:%S"),
                        "source": "KKDB",
                    }
                    update_elements.append(upd)
        elif src == "dedi":
            query = """
                    SELECT *
                    FROM worldrecords
                    LEFT JOIN maps
                    ON worldrecords.map_id = maps.id
                    WHERE score > ?
                        AND maps.tmx_id = ?;
                    """
            for kid, data in candidates.items():
                a = backend_db.fetchall(query, (data["wrscore"], data["tid"]))
                if a:
                    date = (
                        datetime.datetime.strptime(
                            data["lastactivity"], "%Y-%m-%dT%H:%M:%S"
                        )
                        if "lastactivity" in data
                        else datetime.datetime.fromtimestamp(0)
                    )
                    upd = {
                        "kid": kid,
                        "score": data["wrscore"],
                        "login": "",
                        "nick": data["wruser"],
                        "tmx_id": str(data["tid"]),
                        "date": date.strftime("%Y-%m-%d %H:%M:%S"),
                        "source": "DEDI",
                    }
                    update_elements.append(upd)

        return update_elements

    def dedup_new_scores(candidates):
        # stole stuff from https://stackoverflow.com/a/9835819
        # quick check for duplicates
        check_lst = list(map(lambda c: c["kid"], candidates))
        seen = set()
        dupes = [x for x in check_lst if x in seen or seen.add(x)]
        best_score = {}
        weak_elements = []
        for d in dupes:
            for cand in candidates:
                if d == cand["kid"]:
                    # kacky track length limited to 10 min
                    if cand["score"] <= best_score.get("score", 15 * 60 * 1000):
                        # want earliest date
                        if datetime.datetime.strptime(
                            cand["date"], "%Y-%m-%d %H:%M:%S"
                        ) < datetime.datetime.strptime(
                            best_score.get("date", "5555-05-05 05:05:05"),
                            "%Y-%m-%d %H:%M:%S",
                        ):
                            weak_elements.append(best_score)
                            best_score = cand.copy()
                        else:
                            weak_elements.append(cand)
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
    backend_db = DBConnection(config, secrets)

    recent_wrs_kk_db = kk_upd.get_recent_world_records()
    recent_wrs_kk_tmx = tmx_upd.get_activity()
    # every 10 min check dedimania records
    if update_counter == 10:
        all_dedi_wrs = tmx_upd.get_all_kacky_dedimania_wrs()
    else:
        all_dedi_wrs = {}

    update_wrs_kk = []
    update_wrs_kk += check_new_scores(recent_wrs_kk_tmx, "tmx")
    update_wrs_kk += check_new_scores(recent_wrs_kk_db, "kkdb")
    update_wrs_kk += check_new_scores(all_dedi_wrs, "dedi")
    dedup_new_scores(update_wrs_kk)

    for e in update_wrs_kk:
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
                e["login"],
                e["nick"],
                e["source"],
                e["date"],
                e["tmx_id"] if "tmx_id" in e else e["tm_uid"],
            ),
        )
    update_counter = update_counter % 10 + 1
    updating_records_lock.release()


@app.route("/")
def root():
    return "nothing to see here, go awaiii"


if __name__ == "__main__":
    # Reading config file
    with open(Path(__file__).parents[2] / "config.yaml", "r") as conffile:
        config = yaml.load(conffile, Loader=yaml.FullLoader)

    # Read flask secret (required for flask.flash and flask_login)
    with open(Path(__file__).parents[2] / "secrets.yaml", "r") as secfile:
        secrets = yaml.load(secfile, Loader=yaml.FullLoader)

    if config["logtype"] == "STDOUT":
        pass
        logging.basicConfig(
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    elif config["logtype"] == "FILE":
        # TODO: remove os usage
        config["logfile"] = config["logfile"].replace("~", os.getenv("HOME"))
        if not os.path.dirname(config["logfile"]) == "" and not os.path.exists(
            os.path.dirname(config["logfile"])
        ):
            os.mkdir(os.path.dirname(config["logfile"]))
        f = open(os.path.join(os.path.dirname(__file__), config["logfile"]), "w+")
        f.close()
        logging.basicConfig(
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            filename=config["logfile"],
        )
    else:
        print("ERROR: Logging not correctly configured!")
        exit(1)

    # Set up logging
    logger = logging.getLogger(config["logger_name"])
    logger.setLevel(eval("logging." + config["loglevel"]))

    # setup schedule to update wrs
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=update_wrs, trigger="interval", seconds=60)
    # start scheduler
    scheduler.start()
    # shutdown scheduler on exit
    atexit.register(lambda: scheduler.shutdown())

    # initial wr update on start
    update_wrs()

    app.run(host=config["bind_hosts"], port=config["port"])
