import datetime
import json
import zlib

import mariadb


class KackyReloaded_KackyRecords:
    def __init__(self, secrets):
        self.cursor, self.connection = None, None
        self.open_db_connection(secrets)

    def open_db_connection(self, secrets):
        try:
            self.connection = mariadb.connect(
                host=secrets["krdb_host"],
                user=secrets["krdb_user"],
                passwd=secrets["krdb_passwd"],
                database=secrets["krdb_db"],
            )
        except mariadb.Error as e:
            print(f"The error '{e}' occurred")
            exit(-1)
        self.cursor = self.connection.cursor()

    def get_all_world_records_and_equals(self):
        query = """
        SELECT kackychallenges.uid,
               kackychallenges.name,
               kackychallenges.edition,
               kackychallenges.author,
               localrecord.score,
               localrecord.created_at,
               player.login,
               player.nickname
        FROM   (SELECT map_id,
                       Min(score) AS wr
                FROM   localrecord
                GROUP  BY map_id) toprecords
               INNER JOIN localrecord
                       ON toprecords.map_id = localrecord.map_id
                          AND toprecords.wr = localrecord.score
               INNER JOIN player
                      ON localrecord.player_id = player.id
               INNER JOIN kackychallenges
                      ON kackychallenges.id = localrecord.map_id;
        """
        self.cursor.execute(query)
        top_recs_raw = self.cursor.fetchall()
        top_recs = []
        for rec in top_recs_raw:
            try:
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
            except AttributeError:
                # found deleted map, 'name', 'edition', 'author' are NULL
                pass
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
        return wrs

    def get_recent_world_records(
        self,
        since_datetime: datetime.datetime = datetime.datetime.now()
        - datetime.timedelta(days=7),
    ):
        since_str = since_datetime.strftime("%Y-%m-%d %H:%M:%S")
        query = """
            SELECT kackychallenges.uid,
                   kackychallenges.name,
                   kackychallenges.edition,
                   kackychallenges.author,
                   localrecord.score,
                   localrecord.created_at,
                   player.login,
                   player.nickname
            FROM   (SELECT map_id,
                           Min(score) AS wr
                    FROM   localrecord
                    GROUP  BY map_id) toprecords
                   INNER JOIN localrecord
                           ON toprecords.map_id = localrecord.map_id
                              AND toprecords.wr = localrecord.score
                   INNER JOIN player
                          ON localrecord.player_id = player.id
                   INNER JOIN kackychallenges
                          ON kackychallenges.id = localrecord.map_id
            WHERE  localrecord.created_at > ?;
        """
        self.cursor.execute(query, (since_str,))
        return self.cursor.fetchall()

    def get_maps(self):
        query = """
            SELECT
                uid,
                name,
                player.nickname AS author_nick,
                player.uplay_nickname AS author_uplay,
                edition
            FROM kackychallenges
            INNER JOIN player
            ON kackychallenges.author = player.login;
            """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_user_pbs(self, user: str):
        q = """
            SELECT
                map.name,
                pbs.score,
                pbs.updated_at,
                pbs.kacky_rank
            FROM (
                SELECT
                    localrecord.map_id,
                    localrecord.score,
                    localrecord.updated_at,
                    player.nickname,
                    player.login,
                    player.uplay_nickname,
                    RANK() OVER (
                        PARTITION BY localrecord.map_id
                        ORDER BY localrecord.score, localrecord.updated_at ASC
                    ) AS kacky_rank
                FROM localrecord
                INNER JOIN player ON localrecord.player_id = player.id
            ) AS pbs
            INNER JOIN map ON pbs.map_id = map.id AND UPPER(map.file) NOT LIKE UPPER("%%Lobby%")
            WHERE uplay_nickname = ?;
        """
        self.cursor.execute(q, (user,))
        qres = self.cursor.fetchall()
        # replace \u2013 with - in map name
        return list(
            map(lambda elem: [elem[0].replace("\u2013", "-")] + list(elem[1:]), qres)
        )

    def get_user_pbs_edition(self, user: str, edition: int):
        q = """
            SELECT
                map.name,
                pbs.score,
                pbs.updated_at,
                pbs.kacky_rank
            FROM (
                SELECT
                    localrecord.map_id,
                    localrecord.score,
                    localrecord.updated_at,
                    player.nickname,
                    player.login,
                    player.uplay_nickname,
                    RANK() OVER (
                        PARTITION BY localrecord.map_id
                        ORDER BY localrecord.score, localrecord.updated_at ASC
                    ) AS kacky_rank
                FROM localrecord
                INNER JOIN player ON localrecord.player_id = player.id
            ) AS pbs
            INNER JOIN map ON pbs.map_id = map.id AND UPPER(map.file) NOT LIKE UPPER("%%Lobby%")
            INNER JOIN kackychallenges ON map.uid = kackychallenges.uid
            WHERE uplay_nickname = ? and kackychallenges.edition = ?;
        """
        self.cursor.execute(q, (user, edition))
        qres = self.cursor.fetchall()
        # replace \u2013 with - in map name
        return list(
            map(lambda elem: [elem[0].replace("\u2013", "-")] + list(elem[1:]), qres)
        )

    def get_user_fin_count(self, tmlogin: str):
        q = """
            SELECT edition, edition_finishes FROM (
                SELECT kackychallenges.edition, COUNT(*) OVER (PARTITION BY kackychallenges.edition) AS edition_finishes
                FROM localrecord
                INNER JOIN player ON localrecord.player_id = player.id
                INNER JOIN map ON localrecord.map_id = map.id
                INNER JOIN kackychallenges ON map.uid = kackychallenges.uid
                WHERE player.uplay_nickname = ?
            ) AS counter
            GROUP BY edition;
        """
        self.cursor.execute(q, (tmlogin,))
        qres = self.cursor.fetchall()
        return [{"edition": r[0], "fins": r[1]} for r in qres]

    def get_map_leaderboard(
        self,
        kacky_id: int,
        version: str,
        positions: int = 10,
        raw: bool = False,
        compressed: bool = False,
    ):
        q = """
            SELECT
                localrecord.score,
                localrecord.updated_at,
                player.nickname,
                player.login,
                player.uplay_nickname,
                RANK() OVER (
                    PARTITION BY localrecord.map_id
                    ORDER BY localrecord.score, localrecord.updated_at ASC
                ) AS lb_rank
            FROM localrecord
            INNER JOIN player ON localrecord.player_id = player.id
            INNER JOIN map ON map.id = localrecord.map_id
            WHERE map.name LIKE ?
        """
        import logging

        a = logging.getLogger("KackyRecords")
        if positions > 0:
            q += f"LIMIT {positions}"
        a.debug(q)
        a.debug((f"{kacky_id}{(f' [{version}]' if version else '')}",))
        self.cursor.execute(
            q + ";", (f"%#{kacky_id}{(f' [{version}]' if version else '')}",)
        )
        qres = self.cursor.fetchall()
        a.debug(qres)
        if raw:
            return (
                zlib.compress(json.dumps(qres, default=json_serial).encode())
                if compressed
                else qres
            )
        keys = ["score", "date", "nickname", "login", "uplay", "rank"]
        if compressed:
            raise ValueError("compression only work with raw values")
        return [{k: v for k, v in zip(keys, entry)} for entry in qres]


def datetimetostr(dictin):
    dictin["date"] = dictin["date"].strftime("%m/%d/%Y, %H:%M:%S")
    return dictin


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    """https://stackoverflow.com/a/22238613"""

    if isinstance(obj, (datetime)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))
