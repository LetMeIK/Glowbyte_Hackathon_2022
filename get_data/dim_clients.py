import pandas as pd
from sqlalchemy import create_engine
from config import host1, userpwd1, database1


def data_dim_clients():
    engine = create_engine('postgresql://' + userpwd1 + '@' + host1 + '/' + database1)

    sql = "SELECT phone_num, card_num, start_dt, end_dt,  \
        CASE WHEN end_dt = '01.01.9999' THEN 'N' \
             ELSE 'Y' \
        END AS deleted_flag\
        FROM( \
        SELECT phone_num, card_num, start_dt, \
        LEAD(start_dt, 1, '01.01.9999') OVER(PARTITION BY phone_num ORDER BY start_dt) AS end_dt \
        FROM( \
        SELECT client_phone AS phone_num, card_num , MIN(dt) AS start_dt \
        FROM main.rides GROUP BY client_phone, card_num ORDER BY client_phone, start_dt) q1) q2 "

    dim_clients = pd.read_sql(sql, engine)
    return dim_clients
