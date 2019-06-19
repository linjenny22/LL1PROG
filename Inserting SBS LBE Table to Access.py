import pandas as pd
import pyodbc
import datetime
import os

#sbs_filename is the unprocessed MWBE Vendor List re-saved with password removed.
#MWBE SBS List can't be inserted into MWBE Database. Work-around: Insert into Blank Database, connect as linked table.

execfile(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\SBS MWBE List to Access\SBS LBE Vendor Table Python MAIN.py')

execfile(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\Date\date_func.py')

print FY

# Access_Path = r'S:\Contracts\Research and IT\Procurement Indicators\FY 2019 Procurement Indicators\Indicators2019_OpenContracts.accdb;'
# conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
# cnxn = pyodbc.connect(conn_str)
# crsr = cnxn.cursor()

Access_Path = r'S:\Contracts\Research and IT\Procurement Indicators\FY %s Procurement Indicators\MWBE\Working.accdb;' % (str(FY))
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

df_lbe['Expiration Date'] = df_lbe['Expiration Date'].astype(str)
df_lbe['Certification Date'] = df_lbe['Certification Date'].astype(str)
df_lbe['LEGAL_BUSINESS_NAME'] = df_lbe['LEGAL_BUSINESS_NAME'].str.replace('\'', '') #Remove all apostrophes for convienence in data processing.
df_lbe['ETHNICITY'] = df_lbe['ETHNICITY'].str.replace(' ', '')

df_lbe = df_lbe.fillna('0')

list = [str(tuple(x)).replace('u\'', '\'') for x in df_lbe.values] #Creates SBS Insert List

sbs_insert_list = ["""INSERT INTO tblSBS_LBE%s_%sTEST (RecordID, TAX_ID, FMS_VENDOR_ID, LEGAL_BUSINESS_NAME, ApplicationType, MWBEType, ETHNICITY, ExpirationDate, CertificationDate, EthGen, ReportCategory, LBE_FL) VALUES """ %(FY, FQ) + list[x] + ';' for x in xrange(len(list))]

sql_statement = ["""CREATE TABLE tblSBS_LBE%s_%sTEST(RecordID VARCHAR(100), TAX_ID VARCHAR(100), FMS_VENDOR_ID VARCHAR(100), LEGAL_BUSINESS_NAME VARCHAR(100), ApplicationType VARCHAR(100), MWBEType VARCHAR(100), ETHNICITY VARCHAR(100), ExpirationDate VARCHAR(100), CertificationDate VARCHAR(100), EthGen VARCHAR(100), ReportCategory VARCHAR(100), LBE_FL BIT);""" % (FY, FQ),
                 """DROP TABLE tblSBS_LBE%s_%sTEST;""" % (FY, FQ)]

try:
    crsr.execute(sql_statement[1])
    crsr.commit()
except Exception as e:
    print (e)
    pass

crsr.execute(sql_statement[0])
crsr.commit()

for x in range(0, len(sbs_insert_list)):
    try:
        crsr.execute(sbs_insert_list[x])
        crsr.commit()
    except:
        pass
        print sbs_insert_list[x]
