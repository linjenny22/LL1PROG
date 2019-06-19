import os
import pandas as pd
import datetime
import pyodbc
import numpy as np

today = datetime.datetime.now()

def date_range(today):
    if today.month >= 7 and today.month <= 9:
        date_range_start = datetime.date(today.year - 1, 7, 1) #Dates for whole cumulative year.
        date_range_end = datetime.date(today.year, 6, 30)
        FY = today.year
        FQ = 'Q4'
    elif today.month >= 10 and today.month <= 12:
        date_range_start = datetime.date(today.year, 7, 1) #First Quarter
        date_range_end = datetime.date(today.year, 9, 30)
        FY = today.year + 1
        FQ = 'Q1'
    elif today.month >= 1 and today.month <= 3:
        date_range_start = datetime.date(today.year - 1, 7, 1) #Second Quarter
        date_range_end = datetime.date(today.year - 1, 12, 31)
        FY = today.year + 1
        FQ = 'Q2'
    elif today.month >= 4 and today.month <= 6:
        date_range_start = datetime.date(today.year - 1, 7, 1) #Third Quarter
        date_range_end = datetime.date(today.year, 3, 31)
        FY = today.year
        FQ = 'Q3'
    return [date_range_start, date_range_end, FQ, FY]

[date_range_start, date_range_end, FQ, FY] = date_range(today)

start_date = str(date_range_start.month) + '/' + str(date_range_start.day) + '/' + str(date_range_start.year)
end_date = str(date_range_end.month) + '/' + str(date_range_end.day) + '/' + str(date_range_end.year)

Access_Path = r'S:\Contracts\Research and IT\Procurement Indicators\FY 2019 Procurement Indicators\MWBE\Working.accdb;' #% (str(FY))
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

execfile(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\SBS MWBE List to Access\SBS EBE Vendor Table Python MAIN.py')

ebe['Expiration Date'] = ebe['Expiration Date'].astype(str)
ebe['Certification Date'] = ebe['Certification Date'].astype(str)
ebe['LEGAL_BUSINESS_NAME'] = ebe['LEGAL_BUSINESS_NAME'].str.replace('\'', '') #Remove all apostrophes for convienence in data processing.
ebe[u'TAXID_SS'] = ebe[u'TAXID_SS'].astype(str)

ebe = ebe.fillna('0')

list = [str(tuple(x)).replace('u\'', '\'') for x in ebe.values] #Creates SBS Insert List

sbs_insert_list = ["""INSERT INTO tblSBS_EBE%s_%s (RecordID, TAX_ID, FMS_VENDOR_ID, LEGAL_BUSINESS_NAME, ApplicationType, MWBEType, ETHNICITY, ExpirationDate, CertificationDate, EthGen, ReportCategory, LBE_FL) VALUES """ %(FY, FQ) + list[x] + ';' for x in xrange(len(list))]

sql_statement = ["""CREATE TABLE tblSBS_EBE%s_%s (RecordID VARCHAR(100), TAX_ID VARCHAR(100), FMS_VENDOR_ID VARCHAR(100), LEGAL_BUSINESS_NAME VARCHAR(100), ApplicationType VARCHAR(100), MWBEType VARCHAR(100), ETHNICITY VARCHAR(100), ExpirationDate VARCHAR(100), CertificationDate VARCHAR(100), EthGen VARCHAR(100), ReportCategory VARCHAR(100), LBE_FL BIT);""" % (FY, FQ),
                   """DROP TABLE tblSBS_EBE%s_%s;""" % (FY, FQ)]

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
