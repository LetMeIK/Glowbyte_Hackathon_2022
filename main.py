import pandas as pd
from sqlalchemy import create_engine
from get_data.fact_rides import data_fact_rides
from get_data.dim_cars import data_dim_cars
from get_data.dim_clients import data_dim_clients
from get_data.fact_waybills import data_fact_waybills
from get_data.dim_drivers import data_dim_drivers
from get_data.fact_payments import payment_from_csv
from config import host1, host2, userpwd1, userpwd2, database1, database2

engine = create_engine('postgresql://' + userpwd1 + '@' + host1 + '/' + database1)

fact_rides = data_fact_rides()
dim_drivers = data_dim_drivers()
dim_cars = data_dim_cars()
dim_clients = data_dim_clients()
# fact_waybills = data_fact_waybills(dim_drivers)
fact_payments = payment_from_csv()

engine = create_engine('postgresql://' + userpwd2 + '@' + host2 + '/' + database2)

dim_cars.to_sql('dim_cars', con=engine, if_exists='replace', index=False)
dim_clients.to_sql('dim_clients', con=engine, if_exists='replace', index=False)
dim_drivers.to_sql('dim_drivers', con=engine, if_exists='replace', index=False)