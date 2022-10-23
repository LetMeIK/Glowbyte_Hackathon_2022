import pandas as pd
from sqlalchemy import create_engine
from config import host1, userpwd1, database1

def data_fact_rides():

    engine = create_engine('postgresql://' + userpwd1 + '@' + host1 + '/' + database1)

    sql = "SELECT ride_id, point_from AS point_from_txt, point_to AS point_to_txt, distance AS distance_val, \
        price AS price_amt, client_phone AS client_phone_num, \
        car_plate_num,  ride_arrival_dt, ride_start_date, ride_end_date \
        FROM main.rides \
        INNER JOIN (SELECT car_plate_num, ride, dt AS ride_arrival_dt FROM main.movement WHERE event = 'READY') q_arr \
        ON ride_id=q_arr.ride \
        INNER JOIN (SELECT ride, dt AS ride_start_date FROM main.movement WHERE event IN ('BEGIN', 'CANCEL')) q_beg \
        ON ride_id=q_beg.ride \
        INNER JOIN (SELECT ride, dt AS ride_end_date FROM main.movement WHERE event IN ('END','CANCEL')) q_end \
        ON ride_id=q_end.ride \
        ORDER BY ride_id"

    fact_rides = pd.read_sql(sql, engine)

    return fact_rides
