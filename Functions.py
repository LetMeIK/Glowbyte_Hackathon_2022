import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from config import host1, host2, userpwd1, userpwd2, database1, database2


# def search_save(fact_rides, fact_waybills, fact_payments):
#     N_rides = fact_rides.max(0) - fact_rides.loc(fact_rides.ride_id == fact_rides.index).max(0)
#     N_waybills = fact_waybills.max(0)
#     N_payments = timedelta(fact_payments.transaction_dt[-1] - datetime(2022, 10, 12, 16, 0, 0)).hours * 2
#
#     Nums = pd.DataFrame(N_rides, N_waybills, N_payments)
#     return Nums


# def take_save():
#     engine = connection1()
#
#     Save = pd.read_sql("SELECT * FROM main.work_save", engine)
#     return Save


# def connection1():
#     con1 = ('postgresql://' + userpwd1 + '@' + host1 + '/' + database1)
#     return con1
#
#
# def connection2():
#     con2 = ('postgresql://' + userpwd2 + '@' + host2 + '/' + database2)
#     return con2
