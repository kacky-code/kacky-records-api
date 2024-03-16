import functools
import logging
import sys
from pathlib import Path

import flask
import yaml

if sys.version_info[:2] >= (3, 8):
    # TODO: Import directly (no need for conditional) when `python_requires = >= 3.8`
    from importlib.metadata import PackageNotFoundError, version  # pragma: no cover
else:
    from importlib_metadata import PackageNotFoundError, version  # pragma: no cover

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = "kacky-records-api"
    __version__ = version(dist_name)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError

# Reading config file
with open(Path(__file__).parents[2] / "config.yaml", "r") as conffile:
    config = yaml.load(conffile, Loader=yaml.FullLoader)

# Read flask secret (required for flask.flash and flask_login)
with open(Path(__file__).parents[2] / "secrets.yaml", "r") as secfile:
    secrets = yaml.load(secfile, Loader=yaml.FullLoader)

if config["logtype"] == "STDOUT":
    pass
    logging.basicConfig(format="%(name)s - %(levelname)s - %(message)s")
# YES, this totally ignores threadsafety. On the other hand, it is quite safe to assume
# that it only will occur very rarely that things get logged at the same time in this
# usecase. Furthermore, logging is absolutely not critical in this case and mostly used
# for debugging. As long as the
# SQLite DB doesn't break, we're safe!
elif config["logtype"] == "FILE":
    if config["logtype"] == "FILE":
        logfile_path = Path(config["logfile"])

        if logfile_path.parent != Path() and not logfile_path.parent.exists():
            logfile_path.parent.mkdir(parents=True)

        with logfile_path.open(mode="w+") as f:
            pass

        logging.basicConfig(
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            filename=logfile_path,
        )
else:
    print("ERROR: Logging not correctly configured!")
    exit(1)

# Set up logging
logger = logging.getLogger(config["logger_name"])
logger.setLevel(eval("logging." + config["loglevel"]))


def key_required(func):
    @functools.wraps(func)
    def decorator(*args, **kwargs):
        if flask.request.headers.get("X-ApiKey", "badkey") in secrets["api_keys"]:
            logger.debug(
                f'{flask.request.headers.get("X-ApiKey", "badkey")} authenticated'
            )
            return func(*args, **kwargs)
        logger.info(
            f"{flask.request.headers.get('X-Forwarded-For', 'unknown-forward')} unauthenticated"
        )
        return "bad authentication", 403

    return decorator
