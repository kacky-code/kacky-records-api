import atexit
import json
import logging
import os
from pathlib import Path

import flask
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from flask_cors import CORS
from update_records import update_wrs

from kacky_records_api.db_operators.operators import DBConnection

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
    if isinstance(event, str) and edition.isdigit() and event in ["kk", "kr"]:
        return "Error: bad path", 404
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
    events = backend_db.fetchone("SELECT name, shortname FROM events;")
    return json.dumps(events)


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
    update_wrs(config, secrets)

    app.run(host=config["bind_hosts"], port=config["port"])
