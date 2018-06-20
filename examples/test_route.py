import os
from datetime import datetime, timedelta

import iso8601
from sqlalchemy.orm import aliased

from models import StopTime, start_db


START_TIME = iso8601.parse_date("2018-05-08T05:00:00-04:00")

AL_trip = {
    "start_time": START_TIME,
    "legs": [
        {
            "from": "A12",
            "to": "A31",
            "route": "A",
            "direction": "S",
        },
        {
            "from": "L01",
            "to": "L03",
            "route": "L",
            "direction": "S"
        }
    ]
}


DQ_trip = {
    "start_time": START_TIME,
    "legs": [
        {
            "from": "D13",
            "to": "D17",
            "route": "D",
            "direction": "S",
        },
        {
            "from": "R17",
            "to": "R20",
            "route": "any",
            "direction": "S"
        }
    ]
}

D4_trip = {
    "start_time": START_TIME,
    "legs": [
        {
            "from": "D13",
            "to": "D11",
            "route": "D",
            "direction": "N",
        },
        {
            "from": "414",
            "to": "635",
            "route": "4",
            "direction": "S"
        }
    ]
}

def end_time(sess, start_time, leg):
    finish_train = aliased(StopTime)
    depart_train = aliased(StopTime)
    query = sess.query(finish_train).\
        join(depart_train, finish_train.trip_mta_id == depart_train.trip_mta_id).\
        filter(finish_train.timestamp > depart_train.timestamp).\
        filter(depart_train.stop_mta_id == leg["from"]).\
        filter(finish_train.stop_mta_id == leg["to"]).\
        filter(depart_train.timestamp > start_time).\
        filter(depart_train.direction == leg["direction"])

    if leg["route"] != "any":
        query = query.filter(depart_train.trip_mta_id.like(
            "%{}..%".format(leg["route"])
        ))
    query = query.order_by(finish_train.timestamp)
    end = query.first()
    if not end:
        raise ValueError("could not find route")
    return end.timestamp


def route_test(trip):
    _, Session = start_db(os.environ["DATABASE_URL"])
    time = trip["start_time"]
    for leg in trip["legs"]:
        time = end_time(Session(), time, leg)
        time += timedelta(minutes=1)  # assume all in station transfers take 1 minute
    return time - trip["start_time"]
