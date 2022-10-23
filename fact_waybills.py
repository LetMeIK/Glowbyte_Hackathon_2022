import pandas as pd
from sqlalchemy import create_engine
from ftplib import FTP_TLS
from config import host1, userpwd1, database1

def waybill_from_xml():
    host = 'de-edu-db.chronosavant.ru'
    port = 21
    user = 'etl_tech_user'
    password = 'etl_tech_user_password'

    class Reader:
        def __call__(self, data_):
            self.data = pd.DataFrame()

    waybill = pd.DataFrame()

    class XMLReader(Reader):
        def __init__(self):
            self.data = None

        def __call__(self, data_):
            datadriver = pd.read_xml(data_, xpath='//driver')
            dataperiod = pd.read_xml(data_, xpath='//period')
            datawb = pd.read_xml(data_).drop(columns=['driver', 'period'], axis=1)
            self.data = pd.concat([datawb, datadriver, dataperiod], axis=1)

    ftp = FTP_TLS(timeout=100)
    ftp.set_pasv(True)
    ftp.connect(host=host, port=port)
    ftp.login(user=user, passwd=password)
    ftp.prot_p()
    ftp.cwd('waybills')

    lst = ftp.nlst()

    r = XMLReader()
    for j in range(len(lst) // 200 + 1):
        for i in range(j * 200, len(lst)):
            ftp.retrbinary(f'RETR waybill_{i:06}.xml', r)
            waybill = pd.concat([waybill, r.data], axis=0)
    ftp.quit()

    return waybill


def data_fact_waybills(dim_drivers):
    engine = create_engine('postgresql://' + userpwd1 + '@' + host1 + '/' + database1)

    waybill = waybill_from_xml().rename(
        columns={'number': 'waybill_num', 'car': 'car_plate_num', 'start': 'work_start_dt', 'stop': 'work_end_dt',
                 'issuedt': 'issue_dt'}). \
        drop(columns=['model', 'name', 'validto'], axis=1)

    waybill = waybill.merge(dim_drivers[['personnel_num', 'driver_license_num']], left_on='license',
                            right_on='driver_license_num', how="left"). \
        drop(columns=['license', 'driver_license_num'])

    return waybill
