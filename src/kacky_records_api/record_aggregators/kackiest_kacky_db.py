import datetime
import json

import mariadb


# noinspection SqlResolve
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
            			INNER JOIN players
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


def datetimetostr(dictin):
    dictin["date"] = dictin["date"].strftime("%m/%d/%Y, %H:%M:%S")
    return dictin


if __name__ == "__main__":
    s = KackiestKacky_KackyRecords()
    # top_recs = s.get_all_world_records_and_equals()
    # print(top_recs)
    # print(len(top_recs))
    wrs = s.get_all_world_records()
    # print(wrs)
    # print(len(wrs))
    print(json.dumps({key: datetimetostr(val) for key, val in wrs.items()}))


def open_db_connection():
    dbuser = "root"
    dbpw = input(f"Please provide mysql pwd for user {dbuser}:")
    try:
        connection = mariadb.connect(
            host="localhost", user=dbuser, passwd=dbpw, database="kacky_backend_rebuild"
        )
        print("Connection to MySQL DB successful")
    except mariadb.Error as e:
        print(f"The error '{e}' occurred")
        exit(-1)
    cursor = connection.cursor()
    return connection, cursor
