import cx_Oracle
import pandas as pd
import datetime

today = datetime.datetime.now()

uid = 'jlin'
pwd = 'Purple22'
service = 'cwprd1.fisalan.nycnet'
db = cx_Oracle.connect(uid + "/" + pwd + "@" + service)

cursor = db.cursor()

sql_list = ['Select TIN, VEND_CUST_CD FROM FMS01.R_VEND_CUST']

cursor.execute(sql_list[0])

df = pd.DataFrame([[x for x in y] for y in cursor], columns = ['TIN', 'VEND_CUST_CD'])

df.to_csv(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\LL1PROG\R_VEND_CUST.txt')
