from datetime import datetime, timedelta
import json
import os

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import structlog
import iso8601

from models import start_db, bulk_upsert, StopTime, Trip, Stop
from stations import stations

DATE_FORMAT = "%Y-%m-%d"

def parse_date(date_string):
    return datetime.strptime(date_string, DATE_FORMAT)

def unparse_date(date):
    return date.strftime(DATE_FORMAT)


# https://www.ianlewis.org/en/python-date-range-iterator
def daterange(from_date=datetime.now(), to_date=None):
    while to_date is None or from_date <= to_date:
        yield from_date
        from_date = from_date + timedelta(days = 1)
    return


def save_to_db(root_log, db_sess, response_data):
    stop_times = []
    trips = []
    stops = []
    for st in response_data:
        log = root_log.bind(stop_time=st["id"])
        log.info("Parsing")
        trip = {
            "mta_id": st["trip"]["id"],
            "route": st["trip"]["route"]["id"],
        }
        # sometimes the routes (line names) have an "S" or "N"
        # appended to them, so fix that
        if len(trip["route"]) > 1:
            if len(trip["route"]) == 2 and (trip["route"][-1] in ["N", "S"]):
                trip["route"] == trip["route"][0]
            else:
                log.warn("Unexpected route", route=trip["route"])

        stop = {
            "mta_id": st["stop"]["station"],
            "name": st["stop"]["name"],
        }
        stop_time = {
            "mta_id": st["id"],
            "trip_mta_id": trip["mta_id"],
            "stop_mta_id": stop["mta_id"],
            "direction": "N" if st["trip"]["direction"] == 0 else "S",
            "progress": st["progress"],
            "timestamp": iso8601.parse_date(st["timestamp"]),
        }
        if stop_time["direction"] != st["stop"]["id"][-1]:
            log.warn("Direction inconsistency",
                     numeric_dir=stop_time["direction"],
                     string_dir=st["stop"]["id"][-1],
            )
        stop_times.append(stop_time)
        trips.append(trip)
        stops.append(stop)
    # TODO dedupe these in memory before sending to DB?
    root_log.info("DB inserting")
    bulk_upsert(StopTime, db_sess, stop_times)
    bulk_upsert(Trip, db_sess, trips)
    bulk_upsert(Stop, db_sess, stops)
    # TODO not necessary?
    db_sess.commit()


def scrape_date(date, db_sess, requests_sess):
    log = structlog.get_logger(date=unparse_date(date))
    URL_TEMPLATE = "http://serviceadvisory.nyc/api/stop/{station}{direction}/{date}"
    for code in stations.keys():
        log = log.bind(station=code)
        for direction in ["N", "S"]:
            log.info("Scraping")
            resp = requests_sess.get(URL_TEMPLATE.format(station=code,
                                                         direction=direction,
                                                         date=unparse_date(date)))
            # TODO better error handling?
            resp.raise_for_status()
            save_to_db(log, db_sess, resp.json())


def requests_session():
    sess = requests.Session()

    retries = Retry(total=5,
                    backoff_factor=0.5,
                    status_forcelist=[ 500, 502, 503, 504 ])

    sess.mount('http://', HTTPAdapter(max_retries=retries))
    return sess

def test_db_load():
    _, Session = start_db()
    with open(os.path.join(os.path.dirname(__file__), "test_data.json")) as f:
        save_to_db(
            Session(),
            json.load(f)
        )
START_DATE = parse_date("2018-05-07")
END_DATE = parse_date("2018-05-14")

def scrape_range(start_date=START_DATE, end_date=END_DATE):
    _, Session = start_db(os.environ["DATABASE_URL"])
    for date in daterange(start_date, end_date):
        scrape_date(date, Session(), requests_session())

if __name__ == "__main__":
	scrape_range() 
