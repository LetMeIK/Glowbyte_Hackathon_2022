import pandas as pd
from ftplib import FTP_TLS
import io
from config import host3, port3, user3, pass3


def payment_from_csv():

    class Reader:
        def __call__(self, data_):
            self.data = pd.DataFrame()

    payment = pd.DataFrame()

    class CSVReader(Reader):
        def __init__(self):
            self.data = None

        def __call__(self, data_):
            self.data = pd.read_csv(io.BytesIO(data_), sep='\t',
                                    names=['transaction_dt', 'card_num', 'transaction_amt'])

    ftp = FTP_TLS(timeout=100)
    ftp.set_pasv(True)
    ftp.connect(host=host3, port=port3)
    ftp.login(user=user3, passwd=pass3)
    ftp.prot_p()
    ftp.cwd('payments')

    lst = ftp.nlst()

    r = CSVReader()

    for j in range(len(lst)//200 + 1):
        for i in range(len(lst)):
            ftp.retrbinary(f'RETR {lst[i]}', r)
            payment = pd.concat([payment, r.data])
    ftp.quit()

    payment['transaction_id'] = payment.index + 1
    payment = payment[['transaction_id', 'card_num', 'transaction_amt', 'transaction_dt']]
    return payment


#%%
