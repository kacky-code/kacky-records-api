import mariadb


# noinspection SqlResolve
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
            print("Connection to MySQL DB successful")
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
               LEFT JOIN player
                      ON localrecord.player_id = player.id
               LEFT JOIN kackychallenges
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

    def get_maps(self):
        query = """
        SELECT
            uid,
            name,
            player.nickname AS author_nick,
            player.uplay_nickname AS author_uplay,
            edition
        FROM kackychallenges
        LEFT JOIN player
        ON kackychallenges.author = player.login;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()


def datetimetostr(dictin):
    dictin["date"] = dictin["date"].strftime("%m/%d/%Y, %H:%M:%S")
    return dictin
