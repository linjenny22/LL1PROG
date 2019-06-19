import os
import pandas as pd
import datetime
import pyodbc
import numpy as np
import sys

# execfile(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\FY18 MWBE Prog Files\Scripts\Optimized\LL1 Reporting - PRODUCTION\SBS MWBE Vendor Table MAIN.py')
#
# execfile(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\Date\date_func.py')

#When connected to MWBE Database, volatile Error Msg. Use Working db instead.

Access_Path = r'S:\Contracts\Research and IT\Procurement Indicators\FY %s Procurement Indicators\MWBE\Working.accdb;' % (str(FY))
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

sbs['Expiration Date'] = sbs['Expiration Date'].astype(str)
sbs['Certification Date'] = sbs['Certification Date'].astype(str)
sbs['LEGAL_BUSINESS_NAME'] = sbs['LEGAL_BUSINESS_NAME'].str.replace('\'', '') #Remove all apostrophes for convienence in data processing.

sbs = sbs.fillna('0')

sbs = sbs[[u'Record ID', u'TIN', u'VEND_CUST_CD', u'LEGAL_BUSINESS_NAME', u'Application Type', u'M/WBE Type', u'ETHNICITY', u'Expiration Date', u'Certification Date', u'EthGen', u'ReportCategory', u'LBE_FL']]

if set(sbs[u'ReportCategory'].unique()) == set(['Male-Owned MBE - Black','WBE - Hispanic','WBE - Caucasian Woman','WBE - Black','WBE - Asian','Male-Owned MBE - Asian','Male-Owned MBE - Hispanic']) and set(sbs[u'ETHNICITY']) == set(['Black','Hispanic','Non-Minority','Asian Indian','Asian Pacific']):
    pass
else:
    sys.exit()

list = [str(tuple(x)).replace('u\'', '\'') for x in sbs.values] #Creates SBS Insert List

sbs_insert_list = ["""INSERT INTO tblSBS_MWBE%s_%s (RecordID, TAX_ID, FMS_VENDOR_ID, LEGAL_BUSINESS_NAME, ApplicationType, MWBEType, ETHNICITY, ExpirationDate, CertificationDate, EthGen, ReportCategory, LBE_FL) VALUES """ %(FY, FQ) + list[x] + ';' for x in xrange(len(list))]

sql_statement = ["""CREATE TABLE tblSBS_MWBE%s_%s (RecordID VARCHAR(100), TAX_ID VARCHAR(100), FMS_VENDOR_ID VARCHAR(100), LEGAL_BUSINESS_NAME VARCHAR(100), ApplicationType VARCHAR(100), MWBEType VARCHAR(100), ETHNICITY VARCHAR(100), ExpirationDate VARCHAR(100),	CertificationDate VARCHAR(100), EthGen VARCHAR(100), ReportCategory VARCHAR(100), LBE_FL BIT);""" % (FY, FQ),
                  """DROP TABLE tblSBS_MWBE%s_%s;""" % (FY, FQ)]

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
