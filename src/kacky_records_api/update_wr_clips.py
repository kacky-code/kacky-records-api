import mariadb
import requests

from kacky_records_api import logger, secrets

playlist_ids_kk = (
    "PLXfxs_aJsOl5tXaWq_n7JFfsfttOhDI5p",  # kk1
    "PLXfxs_aJsOl7ePRsyEPK2mqzp9uPx3FyW",  # kk2
    "PLXfxs_aJsOl5RUKSxU15m8yLgHHtifueo",  # kk3
    "PLXfxs_aJsOl6IC4_m1SbMv6MW8pPccj9q",  # kk4
    "PLXfxs_aJsOl73dPvvW5Wz1rAQ0Ua6PRKp",  # kk5
    "PLXfxs_aJsOl7c0Vwmgfw4hVaAhEu4V_Ch",  # kk6
    "PLXfxs_aJsOl5Tm0oz6eGw7pqqE6neSR0R",  # kk7
    "PLXfxs_aJsOl65BRfkzAyXX8dYwwWwNtY7",  # kk8
)

# set up database connection
try:
    connection = mariadb.connect(
        host=secrets["backend_host"],
        user=secrets["backend_user"],
        passwd=secrets["backend_passwd"],
        database=secrets["backend_db"],
    )
except mariadb.Error as e:
    logger.error(f"Connecting to database failed! {e}")
    raise e
cursor = connection.cursor()

for playlist_id in playlist_ids_kk:
    logger.debug(f"Updating clips for playlist {playlist_id}")
    yt_video_ids = []
    while True:
        try:
            nextpage = r.json().get("nextPageToken", "")
        except NameError:
            nextpage = ""
        try:
            r = requests.get(
                f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults=50&"
                f"playlistId={playlist_id}&pageToken={nextpage}&key={secrets['youtube_api_key']}"
            )
        except Exception as e:
            logger.error(f"ERROR updating yt clips from playlist {playlist_id}")
            raise e
        if r.status_code != 200:
            logger.error(
                f"ERROR updating yt clips from playlist {playlist_id} - HTTP {r.status_code}! Got response {r.text}"
            )
        more_pages = "nextPageToken" in r.json()
        print("more_pages=" + str(more_pages))
        d = [
            (
                i["snippet"]["resourceId"]["videoId"],
                int(i["snippet"]["title"].split("#")[1].split(" ")[0]),
            )
            for i in r.json()["items"]
        ]
        yt_video_ids.extend(d)
        if not more_pages:
            break
    print(f"{playlist_id} with {len(yt_video_ids)}")

    query = """
            UPDATE maps
            INNER JOIN events ON maps.kackyevent = events.id
            SET default_clip=?
            WHERE kacky_id_int=? AND events.type = 'kk';
            """
    for clip in yt_video_ids:
        print(clip)
        cursor.execute(query, clip)
    connection.commit()
connection.close()
