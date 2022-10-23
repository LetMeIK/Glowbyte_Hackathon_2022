import pandas as pd
from sqlalchemy import create_engine
from config import host1, userpwd1, database1


def data_dim_drivers():
    engine = create_engine('postgresql://' + userpwd1 + '@' + host1 + '/' + database1)

    sql = "SELECT last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt, start_dt, end_dt, \
        CASE WHEN end_dt = '01.01.9999' THEN 'N' \
             ELSE 'Y' \
        END AS deleted_flag\
        FROM( \
        SELECT last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt, start_dt, \
        LEAD(start_dt, 1, '01.01.9999') OVER(PARTITION BY driver_license_num ORDER BY start_dt) AS end_dt \
        FROM( \
        SELECT last_name, first_name, middle_name, birth_dt, card_num, driver_license AS driver_license_num, \
        driver_valid_to AS driver_license_dt, update_dt AS start_dt \
        FROM main.drivers ORDER BY driver_license_num, start_dt) q1) q2 "

    dim_drivers = pd.read_sql(sql, engine)
    dim_drivers['personnel_num'] = dim_drivers.index + 1
    dim_drivers = dim_drivers[
        ['personnel_num', 'start_dt', 'last_name', 'first_name', 'middle_name', 'birth_dt', 'card_num',
         'driver_license_num', 'driver_license_dt', 'deleted_flag', 'end_dt']]
    return dim_drivers
