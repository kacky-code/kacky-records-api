import atexit
import json
import logging
import os
from pathlib import Path
from typing import Any

import flask
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from flask_cors import CORS
from update_records import update_wrs_kackiest_kacky, update_wrs_kacky_reloaded

from kacky_records_api.db_operators.operators import DBConnection
from kacky_records_api.record_aggregators.kackiest_kacky_db import (
    KackiestKacky_KackyRecords,
)
from kacky_records_api.record_aggregators.kacky_reloaded_db import (
    KackyReloaded_KackyRecords,
)
from kacky_records_api.tm_string.tm_format_resolver import TMstr

app = flask.Flask(__name__)
CORS(app)
app.config["CORS_HEADERS"] = "Content-Type"
logger = None


@app.route("/")
def root():
    return "nothing to see here, go awaiii"


@app.route("/wrs/<event>/<edition>")
def wrs_per_event(event, edition):
    # check if parameters are valid (this also is input sanitation)
    check_event_edition_legal(event, edition)
    # set up connection to backend database
    backend_db = DBConnection(config, secrets)
    # check if event exists
    event_id = backend_db.fetchone(
        "SELECT id FROM events WHERE type = ? AND edition = ?;", (event, edition)
    )[0]
    if not event_id:
        return "Error: parameters out of range", 404
    query = """
    SELECT maps.name, maps.kacky_id, wr.score, wr.nickname, wr.login
    FROM worldrecords AS wr
    INNER JOIN maps ON wr.map_id = maps.id
    WHERE maps.kacky_id = ?;
    """
    wrs_for_event = backend_db.fetchall(query, (event_id,))
    wrs_for_event_dicts = [
        {"map": wr[0], "kid": wr[1], "score": wr[2], "nick": wr[3], "login": wr[4]}
        for wr in wrs_for_event
    ]
    return json.dumps(wrs_for_event_dicts), 200


@app.route("/events")
def get_all_events():
    # set up connection to backend database
    backend_db = DBConnection(config, secrets)
    events_query_result = backend_db.fetchall(
        "SELECT name, type, edition FROM events;", ()
    )
    events = [
        {"name": TMstr(ev[0]).string, "type": ev[1], "edition": ev[2]}
        for ev in events_query_result
    ]
    return json.dumps(events)


@app.route("/pb/<user>/<eventtype>")
def get_user_pbs(user: str, eventtype: str):
    check_event_edition_legal(eventtype, "1")
    if eventtype.upper() == "KK":
        pbs = KackiestKacky_KackyRecords(secrets).get_user_pbs(user)
    elif eventtype.upper() == "KR":
        pbs = KackyReloaded_KackyRecords(secrets).get_user_pbs(user)
    else:
        return "ERROR, invalid params"
    return (
        json.dumps(
            {
                TMstr(x[0]).string.split("#")[1]: {
                    "score": x[1],
                    "kacky_rank": x[3],
                    "date": x[2].timestamp(),
                }
                for x in pbs
            }
        ),
        200,
    )


@app.route("/performance/<login>/<eventtype>")
def get_user_fin_count(login: str, eventtype: str):
    check_event_edition_legal(eventtype, "1")
    if eventtype.upper() == "KK":
        fins = KackiestKacky_KackyRecords(secrets).get_user_fin_count(login)
    elif eventtype.upper() == "KR":
        fins = KackyReloaded_KackyRecords(secrets).get_user_fin_count(login)
    else:
        return "ERROR, invalid params"
    return json.dumps(fins), 200


def check_event_edition_legal(event: Any, edition: Any):
    # check if parameters are valid (this also is input sanitation)
    if isinstance(event, str) and edition.isdigit() and event in ["kk", "kr"]:
        # Allowed arguments
        return True
    raise AssertionError


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
    scheduler.add_job(
        func=update_wrs_kackiest_kacky,
        args=(config, secrets),
        trigger="interval",
        seconds=60,
    )
    scheduler.add_job(
        func=update_wrs_kacky_reloaded,
        args=(config, secrets),
        trigger="interval",
        seconds=60,
    )
    # start scheduler
    scheduler.start()
    # shutdown scheduler on exit
    atexit.register(lambda: scheduler.shutdown())

    # initial wr update on start
    #update_wrs_kackiest_kacky(config, secrets)
    #update_wrs_kacky_reloaded(config, secrets)

    app.run(host=config["bind_hosts"], port=config["port"])
