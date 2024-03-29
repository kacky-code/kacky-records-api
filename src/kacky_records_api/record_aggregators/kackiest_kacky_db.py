import datetime

import mariadb

# from kacky_records_api.tm_string.tm_format_resolver import TMString
from tmformatresolver import TMString


class KackiestKacky_KackyRecords:
    def __init__(self, secrets):
        self.cursor, self.connection = None, None
        self.open_db_connection(secrets)

    def open_db_connection(self, secrets):
        try:
            self.connection = mariadb.connect(
                host=secrets["kkdb_host"],
                user=secrets["kkdb_user"],
                passwd=secrets["kkdb_passwd"],
                database=secrets["kkdb_db"],
            )
            print("Connection to MySQL DB successful")
        except mariadb.Error as e:
            print(f"The error '{e}' occurred")
            exit(-1)
        self.cursor = self.connection.cursor()

    def get_all_world_records_and_equals(self):
        query = """
                SELECT records.challenge_uid,
                       challenges.name,
                       challenges.edition,
                       challenges.author,
                       records.score,
                       records.date,
                       players.login,
                       players.nickname
                FROM   (SELECT     challenge_uid,
                                   Min(score) AS wr
                        FROM       records
                        JOIN players
                               ON  records.player_id = players.id
                            WHERE  players.banned = 0
                        GROUP  BY  challenge_uid) toprecords
                       INNER JOIN records
                               ON toprecords.challenge_uid = records.challenge_uid
                                  AND toprecords.wr = records.score
                       LEFT JOIN players
                              ON records.player_id = players.id
                       LEFT JOIN challenges
                              ON challenges.uid = records.challenge_uid;
        """
        self.cursor.execute(query)
        top_recs_raw = self.cursor.fetchall()
        top_recs = []
        for rec in top_recs_raw:
            record = {
                "kid": rec[1].split("#")[1].replace("\u2013", "-"),
                "uid": rec[0],
                "name": rec[1],
                "edition": rec[2],
                "author": rec[3],
                "score": rec[4],
                "date": rec[5],
                "login": rec[6],
                "nick": rec[7],
            }
            top_recs.append(record)
        return top_recs

    def get_all_world_records(self):
        top_recs = self.get_all_world_records_and_equals()
        wrs = {}
        for rec in top_recs:
            if rec["kid"] not in wrs:
                # add entry if map has no entry yet
                wrs[rec["kid"]] = rec
                # remove duplicate data from datastructure
                wrs[rec["kid"]].pop("kid")
            else:
                # if multiple records for a map exist, find earliest
                if rec["date"] < wrs[rec["kid"]]["date"]:
                    wrs[rec["kid"]] = rec
                    # remove duplicate data from datastructure
                    wrs[rec["kid"]].pop("kid")
        # wrs_list = [
        #   (v["uid"], v["name"], v["edition"], v["author"], v["score"], v["date"], v["login"], v["nick"]
        # ) for v in wrs.values()]
        return wrs

    def get_recent_world_records(
        self,
        since_datetime: datetime.datetime = datetime.datetime.now()
        - datetime.timedelta(days=7),
    ):
        since_str = since_datetime.strftime("%Y-%m-%d %H:%M:%S")
        query = """
                SELECT records.challenge_uid,
                       challenges.name,
                       challenges.edition,
                       challenges.author,
                       records.score,
                       records.date,
                       players.login,
                       players.nickname
                FROM   (SELECT challenge_uid,
                               Min(score) AS wr
                        FROM   records
                        INNER JOIN players
                               ON  records.player_id = players.id
                            WHERE  players.banned = 0
                        GROUP  BY challenge_uid) toprecords
                       INNER JOIN records
                               ON toprecords.challenge_uid = records.challenge_uid
                                  AND toprecords.wr = records.score
                       LEFT JOIN players
                              ON records.player_id = players.id
                       LEFT JOIN challenges
                              ON challenges.uid = records.challenge_uid
                WHERE records.date > ?;
                """
        self.cursor.execute(query, (since_str,))
        return self.cursor.fetchall()

    def get_maps(self):
        query = "SELECT uid, name, author, edition FROM challenges;"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_user_records(self, user_login, key: str = None, raw: bool = False):
        """

        Parameters
        ----------
        user_login :
        key :
            (None, "kacky_id", "challenge_uid")
        raw :

        Returns
        -------

        """
        if key not in (None, "kacky_id", "challenge_uid"):
            ValueError("Bad value for parameter 'key'!")
        # Check if user is in database. If so, collect ID for later query
        preflight_q = "SELECT id FROM players WHERE login = ?"
        self.cursor.execute(preflight_q, (user_login,))
        userid_from_db = self.cursor.fetchall()
        if not userid_from_db:
            return {"Error": "User not found"}
        # challenge_uid needs to be first argument in query. Relevant for key = "challenge_uid"
        records_q = "SELECT challenge_uid, score, date, created_at, updated_at FROM records WHERE player_id = ?;"
        self.cursor.execute(records_q, (int(userid_from_db[0][0]),))
        columns = [col[0] for col in self.cursor.description]
        records_vals = self.cursor.fetchall()
        if raw:
            return records_vals

        if not key:
            return [dict(zip(columns, row)) for row in records_vals]
        elif key == "challenge_uid":
            return {row[0]: dict(zip(columns[1:], row[1:])) for row in records_vals}
        else:
            # must be key = "kacky_id"
            # collect all challenge UIDs and corresponding map names
            id_query = "SELECT uid, name FROM challenges;"
            self.cursor.execute(id_query, ())
            kacky_ids = self.cursor.fetchall()
            # Build initial result with challenge_uids as key
            result_dict = {row[0]: dict(zip(columns, row)) for row in records_vals}
            # replace all keys (challenge_uid) with Kacky IDs
            for kid in kacky_ids:
                kid_number = int(kid[1].split("#")[1].replace("\u2013", "-"))
                try:
                    result_dict[kid_number] = result_dict.pop(kid[0])
                except KeyError:
                    # no record for map in result set - skip this entry
                    continue
            return result_dict

    def get_total_fins(self, all_time=False):
        if all_time:
            table = "times"
        else:
            table = "records"

        query = f"""
                SELECT challenges.name, COUNT(challenge_id) as fin_count
                FROM {table}
                LEFT JOIN challenges ON challenges.id = {table}.challenge_id
                LEFT JOIN players ON {table}.player_id = players.id
                WHERE players.banned = 0
                AND {table}.server_id not in (22,23,24,25)
                GROUP BY challenge_id;
                """
        query

    def get_user_pbs(self, user: str):
        q = """
            SELECT challenges.name, pbs.score, pbs.date, pbs.kacky_rank
            FROM (
                SELECT
                    records.challenge_id,
                    records.score,
                    records.date,
                    players.nickname,
                    players.login,
                    RANK() OVER (
                        PARTITION BY records.challenge_id
                        ORDER BY records.score, records.date ASC
                    ) AS kacky_rank
                FROM records
                INNER JOIN players ON records.player_id = players.id
                WHERE players.banned = 0
            ) AS pbs
            INNER JOIN challenges ON pbs.challenge_id = challenges.id
            WHERE pbs.login = ?;
        """
        self.cursor.execute(q, (user,))
        qres = self.cursor.fetchall()
        # replace \u2013 with - in map name
        return list(
            map(lambda elem: [elem[0].replace("\u2013", "-")] + list(elem[1:]), qres)
        )

    def get_user_fin_count(self, tmlogin: str):
        q = """
            SELECT edition, edition_finishes FROM (
                SELECT challenges.edition, COUNT(*) OVER (PARTITION BY challenges.edition) AS edition_finishes
                FROM records
                INNER JOIN players ON records.player_id = players.id
                INNER JOIN challenges ON records.challenge_id = challenges.id
                WHERE players.login = ?
            ) AS counter
            GROUP BY edition;
        """
        self.cursor.execute(q, (tmlogin,))
        qres = self.cursor.fetchall()
        return [{"edition": r[0], "fins": r[1]} for r in qres]

    def get_user_pbs_edition(self, tmlogin, edition):
        query = """
        SELECT challenges.name, pbs.score, pbs.date, pbs.kacky_rank
        FROM (
            SELECT
                records.challenge_id,
                records.score,
                records.date,
                players.nickname,
                players.login,
                RANK() OVER (
                    PARTITION BY records.challenge_id
                    ORDER BY records.score, records.date ASC
                ) AS kacky_rank
                FROM records
                INNER JOIN players ON records.player_id = players.id
                INNER JOIN challenges ON records.challenge_id = challenges.id
                WHERE players.banned = 0 AND challenges.edition = ?
        ) AS pbs
        INNER JOIN challenges ON pbs.challenge_id = challenges.id
        WHERE pbs.login = ?;
        """
        self.cursor.execute(query, (edition, tmlogin))
        qres = self.cursor.fetchall()
        # replace \u2013 with - in map name
        return list(
            map(lambda elem: [elem[0].replace("\u2013", "-")] + list(elem[1:]), qres)
        )

    def get_player_avg_edition(self, edition, login):
        query = """
            SELECT challenges.name, pbs.score, pbs.date, AVG(pbs.kacky_rank)
            FROM (
                SELECT
                    records.challenge_id,
                    records.score,
                    records.date,
                    players.nickname,
                    players.login,
                    RANK() OVER (
                        PARTITION BY records.challenge_id
                        ORDER BY records.score, records.date ASC
                    ) AS kacky_rank
                    FROM records
                    INNER JOIN players ON records.player_id = players.id
                    INNER JOIN challenges ON records.challenge_id = challenges.id
                    WHERE players.banned = 0 AND challenges.edition = ?
            ) AS pbs
            INNER JOIN challenges ON pbs.challenge_id = challenges.id
            WHERE pbs.login = ?;
        """
        self.cursor.execute(query, (edition, login))
        qres = self.cursor.fetchall()
        return qres

    def get_leaderboard(
        self,
        edition,
        startrank: int = 1,
        endrank: int = 1,
        html: bool = False,
        raw: bool = False,
        force: bool = False,
    ):
        if endrank - startrank > 100 and not force:
            raise ValueError("Range of ranks to big!")
        query = """
            SELECT
                COUNT(DISTINCT challenge_uid) as fins,
                players.login as llogin,
                players.nickname as lnick,
                players.id as lpid,
                (
                SELECT AVG(pbs.kacky_rank)
                    FROM (
                        SELECT
                            records.challenge_id,
                            records.score,
                            records.date,
                            players.nickname,
                            players.login,
                            RANK() OVER (
                                PARTITION BY records.challenge_id
                                ORDER BY records.score, records.date ASC
                            ) AS kacky_rank
                            FROM records
                            INNER JOIN players ON records.player_id = players.id
                            INNER JOIN challenges ON records.challenge_id = challenges.id
                            WHERE players.banned = 0 AND challenges.edition = ? AND server_id IN (1, 22,23,24,25)
                    ) AS pbs
                    INNER JOIN challenges ON pbs.challenge_id = challenges.id
                    WHERE pbs.login = llogin
                ) as ev_avg
            FROM records
            INNER JOIN challenges ON records.challenge_uid = challenges.uid
            INNER JOIN players ON players.id = player_id
            WHERE edition = 8 AND server_id IN (1, 22,23,24,25)
            GROUP BY player_id
            ORDER BY fins DESC, ev_avg ASC
            LIMIT ?, ?;
        """
        self.cursor.execute(
            query,
            (
                edition,
                startrank,
                (endrank - startrank if endrank - startrank > 0 else 1),
            ),
        )
        qres = self.cursor.fetchall()
        if raw:
            return qres
        if html:
            return [
                {
                    "login": elem[1],
                    "nick": TMString(elem[2]).html,
                    "fins": elem[0],
                    "avg": float(elem[4]),
                }
                for elem in qres
            ]
        return [
            {"login": elem[1], "nick": elem[2], "fins": elem[0], "avg": float(elem[4])}
            for elem in qres
        ]

    def get_login_rank(self, edition, login, html: bool = False):
        leaderboard = self.get_leaderboard(
            edition, startrank=0, endrank=100000, force=True, raw=True
        )
        rank = 1
        for elem in leaderboard:
            if elem[1] == login:
                break
            rank += 1
        # inequality when login not found in leaderboard. return empty dict
        if login != elem[1]:
            return {}
        if html:
            return {
                "rank": rank,
                "login": elem[1],
                "nick": TMString(elem[2]).html,
                "fins": elem[0],
                "avg": float(elem[4]),
            }
        return {
            "rank": rank,
            "login": elem[1],
            "nick": elem[2],
            "fins": elem[0],
            "avg": float(elem[4]),
        }

    def get_user_pbs_event(self, edition):
        # QUERY
        # CONTAINS
        # HARDCODED
        # LOGIN!!
        query = """
        SELECT challenges.name, pbs.score, pbs.date, pbs.kacky_rank
        FROM (
            SELECT
                times.challenge_id,
                times.score,
                times.date,
                players.nickname,
                players.login,
                RANK() OVER (
                    PARTITION BY times.challenge_id
                    ORDER BY times.score, times.date ASC
                ) AS kacky_rank
                FROM times
                INNER JOIN players ON times.player_id = players.id
                INNER JOIN challenges ON times.challenge_id = challenges.id
                WHERE players.banned = 0 AND challenges.edition = 8 AND server_id IN (1, 22,23,24,25)
        ) AS pbs
        INNER JOIN challenges ON pbs.challenge_id = challenges.id
        WHERE pbs.login = "simo_900";
        """
        self.cursor.execute(query, (edition,))
        qres = self.cursor.fetchall()
        return qres


def datetimetostr(dictin):
    dictin["date"] = dictin["date"].strftime("%m/%d/%Y, %H:%M:%S")
    return dictin


def open_db_connection():
    dbuser = "root"
    dbpw = input(f"Please provide mysql pwd for user {dbuser}:")
    try:
        connection = mariadb.connect(
            host="localhost", user=dbuser, passwd=dbpw, database="kacky_backend_rebuild"
        )
    except mariadb.Error as e:
        print(f"The error '{e}' occurred")
        exit(-1)
    cursor = connection.cursor()
    return connection, cursor


if __name__ == "__main__":
    from pathlib import Path

    import yaml

    # Read flask secret (required for flask.flash and flask_login)
    with open(Path(__file__).parents[3] / "secrets.yaml", "r") as secfile:
        secrets = yaml.load(secfile, Loader=yaml.FullLoader)

    k = KackiestKacky_KackyRecords(secrets)
    cork_recs = k.get_user_records("el-djinn", key="kacky_id")
    print(cork_recs)
    print(len(cork_recs))
