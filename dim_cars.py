import pandas as pd
from sqlalchemy import create_engine
from config import host1, userpwd1, database1

def data_dim_cars():

    engine = create_engine('postgresql://' + userpwd1 + '@' + host1 + '/' + database1)

    sql = "SELECT plate_num, register_dt AS start_dt, model AS model_name, revision_dt, finished_flg AS deleted_flag, \
        CASE WHEN finished_flg='N' THEN '01.01.9999' \
             ELSE NOW() \
        END AS end_dt \
        FROM main.car_pool"

    dim_cars = pd.read_sql(sql, engine)
    return dim_cars
