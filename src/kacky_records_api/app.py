import atexit
import datetime
import logging
import os
from typing import Any

import flask
from apscheduler.schedulers.background import BackgroundScheduler
from flask_cors import CORS

# from kacky_records_api.tm_string.tm_format_resolver import TMstr
from tmformatresolver import TMString

from kacky_records_api import config, key_required, logger, secrets
from kacky_records_api.db_operators.operators import DBConnection
from kacky_records_api.record_aggregators.kackiest_kacky_db import (
    KackiestKacky_KackyRecords,
)
from kacky_records_api.record_aggregators.kacky_reloaded_db import (
    KackyReloaded_KackyRecords,
)
from kacky_records_api.update_records import (
    restore_wr_after_reset,
    update_wrs_kackiest_kacky,
    update_wrs_kacky_reloaded,
)

app = flask.Flask(__name__)
CORS(app)
app.config["CORS_HEADERS"] = "Content-Type"


class UpdatedJSONProvider(flask.json.provider.DefaultJSONProvider):
    def default(self, o):
        if isinstance(o, datetime.date) or isinstance(o, datetime.datetime):
            return o.isoformat()
        return super().default(o)


def check_api_key(userkey):
    if userkey == secrets["djinn_api_key"]:
        return True
    return False


@app.route("/")
@key_required
def root():
    return "nothing to see here, go awaiii"


@app.route("/wrs/<event>/<edition>")
@key_required
def wrs_per_event(event, edition):
    # log_access(f"/wrs/{event}/{edition}")
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
    return flask.jsonify(wrs_for_event_dicts), 200


@app.route("/events")
@key_required
def get_all_events():
    # log_access("/events")
    # set up connection to backend database
    backend_db = DBConnection(config, secrets)
    events_query_result = backend_db.fetchall(
        "SELECT name, type, edition FROM events;", ()
    )
    events = [
        {"name": TMString(ev[0]).string, "type": ev[1], "edition": ev[2]}
        for ev in events_query_result
    ]
    return flask.jsonify(events)


@app.route("/pb/<user>/<eventtype>", methods=["GET", "POST"])
@key_required
def get_user_pbs(user: str, eventtype: str):
    # log_access(f"/pb/{user}/{eventtype}")
    if (
        flask.request.method == "POST"
        and flask.request.json.get("auth", None)
        and check_api_key(flask.request.json["auth"])
    ):
        # user holds API key
        logger.info("authenticated user")
    check_event_edition_legal(eventtype, "1")
    if eventtype.upper() == "KK":
        pbs = KackiestKacky_KackyRecords(secrets).get_user_pbs(user)
    elif eventtype.upper() == "KR":
        pbs = KackyReloaded_KackyRecords(secrets).get_user_pbs(user)
    else:
        return "ERROR, invalid params"
    return (
        flask.jsonify(
            {
                TMString(x[0])
                .string.split("#")[1]
                .split(" ")[0]: {
                    "score": x[1],
                    "kacky_rank": x[3],
                    "date": x[2].timestamp(),
                }
                for x in pbs
            }
        ),
        200,
    )


@app.route("/pb/<user>/<eventtype>/<edition>")
@key_required
def get_user_pbs_edition(user: str, eventtype: str, edition: int):
    # log_access(f"/pb/{user}/{eventtype}/{edition}")
    if (
        flask.request.method == "POST"
        and flask.request.json.get("auth", None)
        and check_api_key(flask.request.json["auth"])
    ):
        # user holds API key
        logger.info("authenticated user")
    check_event_edition_legal(eventtype, edition)
    if eventtype.upper() == "KK":
        pbs = KackiestKacky_KackyRecords(secrets).get_user_pbs_edition(user, edition)
    elif eventtype.upper() == "KR":
        pbs = KackyReloaded_KackyRecords(secrets).get_user_pbs_edition(user, edition)
    else:
        return "ERROR, invalid params"
    return (
        flask.jsonify(
            {
                TMString(x[0])
                .string.split("#")[1]
                .split(" ")[0]: {
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
@key_required
def get_user_fin_count(login: str, eventtype: str):
    # log_access(f"/performance/{login}/{eventtype}")
    check_event_edition_legal(eventtype, "1")
    if eventtype.upper() == "KK":
        fins = KackiestKacky_KackyRecords(secrets).get_user_fin_count(login)
    elif eventtype.upper() == "KR":
        fins = KackyReloaded_KackyRecords(secrets).get_user_fin_count(login)
    else:
        return "ERROR, invalid params"
    return flask.jsonify(fins), 200


@app.route("/event/leaderboard/<eventtype>/<edition>")
@key_required
def get_leaderboard(eventtype: str, edition: int):
    # log_access(f"/event/leaderboard/{eventtype}/{edition}")
    startrank = flask.request.args.get("start", default=0, type=int)
    elems = flask.request.args.get("elems", default=1, type=int)
    if eventtype.upper() == "KK":
        lb = KackiestKacky_KackyRecords(secrets).get_leaderboard(
            edition, startrank, elems, flask.request.args.get("html", "True")
        )
    else:
        # return "KR not yet implemented"
        return flask.jsonify([{"login": "you", "nick": "qt", "fins": 0, "avg": 0}]), 200
    return flask.jsonify(lb), 200


@app.route("/event/leaderboard/<eventtype>/<edition>/<login>")
@key_required
def get_player_rank(eventtype: str, edition: int, login: str):
    # log_access(f"/event/leaderboard/{eventtype}/{edition}/{login}")
    check_event_edition_legal(eventtype, edition)
    if eventtype.upper() == "KK":
        lb = KackiestKacky_KackyRecords(secrets).get_login_rank(
            edition, login, html=True
        )
    else:
        # return "KR not yet implemented"
        return flask.jsonify([{"login": "you", "nick": "qt", "fins": 0, "avg": 0}]), 200
    return flask.jsonify(lb), 200


@app.route("/leaderboard/<eventtype>/<kacky_id>")
@key_required
def get_map_leaderboard(eventtype: str, kacky_id: int):
    # log_access(f"/leaderboard/{eventtype}/{kacky_id}")
    check_event_edition_legal(eventtype, "1")
    try:
        int(kacky_id)
    except ValueError:
        return "Invalid Kacky Map ID", 400
    try:
        int(flask.request.args.get("positions", 10))
    except ValueError:
        return "Invalid positions argument", 400
    if eventtype.upper() == "KR":
        logger.debug(
            (
                kacky_id,
                flask.request.args.get("version", ""),
                flask.request.args.get("positions", 10),
            )
        )
        lb = KackyReloaded_KackyRecords(secrets).get_map_leaderboard(
            int(kacky_id),
            flask.request.args.get("version", ""),
            int(flask.request.args.get("positions", 10)),
        )
    else:
        return "KK not yet implemented"
    return flask.jsonify(lb), 200


def check_event_edition_legal(event: Any, edition: Any):
    # check if parameters are valid (this also is input sanitation)
    if (
        isinstance(event, str)
        and (isinstance(edition, int) or edition.isdigit())
        and event.lower() in ["kk", "kr"]
    ):
        # Allowed arguments
        return True
    raise AssertionError


def log_access(route: str, logged_in: bool = False):
    # temp suppress queries from own server
    if (
        flask.request.headers.get("X-Forwarded-For", "unknown-forward")
        == "213.109.163.46"
    ):
        return
    logger.info(
        f"{route} accessed by "
        f"{flask.request.headers.get('X-Forwarded-For', 'unknown-forward')}. "
    )


log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

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
    # kwargs={"excluded_event": 4},
    trigger="interval",
    seconds=60,
)
# scheduler.add_job(
#     func=update_wrs_kacky_reloaded,
#     args=(config, secrets),
#     kwargs={"only_event": 4},
#     trigger="interval",
#     seconds=60,
# )
scheduler.add_job(
    func=restore_wr_after_reset,
    args=(config, secrets),
    trigger="interval",
    seconds=60 * 10,
)
# start scheduler
scheduler.start()
# shutdown scheduler on exit
atexit.register(lambda: scheduler.shutdown())

# initial wr update on start
# update_wrs_kackiest_kacky(config, secrets)
# update_wrs_kacky_reloaded(config, secrets)

app.json = UpdatedJSONProvider(app)
if "gunicorn" not in os.environ.get("SERVER_SOFTWARE", ""):
    app.run(host=config["bind_hosts"], port=config["port"])
