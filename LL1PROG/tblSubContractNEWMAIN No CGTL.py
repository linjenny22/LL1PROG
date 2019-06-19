import cx_Oracle
import pandas as pd
import datetime
from win32com.client import Dispatch

#Script Pulls subs from FMS.R_CNTRC_GOAL_LN
#Script Pulls subs from FMS.R_SCNTRC_DET

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

uid ='jlin'
pwd ='Purple22'

service = 'cwprd1.fisalan.nycnet'
db = cx_Oracle.connect(uid + "/" + pwd + "@" + service)

sql_list = ['''Select AMS_ROW_VERS_NO, CVL_INDUS_CLASS_SV, CVL_INDUS_CLASS_DV FROM FMS01.CVL_INDUS_CLASS''',
            '''Select SCNTRC_ID, VEND_CUST_CD, DOC_CD, DOC_DEPT_CD, DOC_ID, SCNTRC_VEND_CD, SCNTRC_VERS_NO,SCNTRC_STRT_DT,SCNTRC_END_DT, SCNTRC_MAX_AMT,SCNTRC_DSCR,SCNTRC_LGL_NM,INDUS_CLS,APRV_STA FROM FMS01.R_SCNTRC_DET WHERE SCNTRC_STRT_DT >= TO_DATE ('%s', \'yyyy-mm-dd\') AND SCNTRC_STRT_DT <= TO_DATE ('%s', \'yyyy-mm-dd\')''' % (date_range_start, date_range_end)]

cursor = db.cursor()
cursor.execute(sql_list[0]) #
indus_class = pd.DataFrame([[x for x in y] for y in cursor], columns = ['AMS_ROW_VERS_NO', 'CVL_INDUS_CLASS_SV', 'CVL_INDUS_CLASS_DV'])

#SCNTRC Subcontracts
cursor.execute(sql_list[1])
SCNTRC = pd.DataFrame([[x for x in y] for y in cursor], columns = ['PrimaryKey',
'PrimeVendorNumber',
'DOC_CD',
'DOC_DEPT_CD',
'DOC_ID',
'SubVendorNumber',
'SCNTRC_VERS_NO',
'SubStartDate',
'SubEndDate',
'SubValue',
'SubDescr',
'SubVendorName',
'INDUS_CLS',
'APRV_STA'])

SCNTRC['Source'] = 'SCNTRC'
SCNTRC = SCNTRC[SCNTRC['APRV_STA'] == 4]

SCNTRC['SubVendorNumber'] = SCNTRC['SubVendorNumber'].astype(str)
SCNTRC['SubStartDate'] = pd.to_datetime(SCNTRC['SubStartDate'])
SCNTRC['SubEndDate'] = pd.to_datetime(SCNTRC['SubEndDate'])

SCNTRC = SCNTRC[(SCNTRC['SubStartDate']>= pd.to_datetime(date_range_start)) & (SCNTRC['SubStartDate']<= pd.to_datetime(date_range_end))]
SCNTRC = SCNTRC.merge(indus_class, how = 'left', left_on ='INDUS_CLS', right_on ='CVL_INDUS_CLASS_SV') #This will result in SubIndustry CVL column appended. We do this because the final column in Subs table is 'CVL_INDUS_CLASS_DV'.

SCNTRC = SCNTRC[['Source', 'PrimeVendorNumber', 'DOC_CD', 'DOC_DEPT_CD','DOC_ID','PrimaryKey','SubVendorNumber','SubStartDate','SubEndDate','SubValue','SubDescr','SubVendorName','INDUS_CLS','CVL_INDUS_CLASS_DV']]
SCNTRC.columns = ['Source', 'PrimeVendorNumber', 'DOC_CD', 'DOC_DEPT_CD','DOC_ID','PrimaryKey','SubVendorNumber','SubStartDate','SubEndDate','SubValue','SubDescr','SubVendorName','SubIndustryCVL','SubIndustry']

tbl_subs = SCNTRC

tbl_subs['SubDescr'] = tbl_subs['SubDescr'].str.replace('\r',' ')
tbl_subs['SubDescr'] = tbl_subs['SubDescr'].str.replace('\n',' ')
tbl_subs['SubDescr'] = tbl_subs['SubDescr'].str.lower()

tbl_subs['ContractID'] = tbl_subs['DOC_CD'] + tbl_subs['DOC_DEPT_CD'] + tbl_subs['DOC_ID']

tbl_subs['SubDescr'] = pd.Series([str(x)[0:18] for x in tbl_subs['SubDescr']])

tbl_subs['SubContractID'] = tbl_subs['ContractID'] + tbl_subs['SubValue'].astype(str) + tbl_subs['SubVendorNumber'].astype(str) + tbl_subs['SubDescr'].astype(str) + tbl_subs['SubStartDate'].astype(str) + tbl_subs['PrimaryKey'].astype(str)

tbl_subs = tbl_subs.drop_duplicates(['SubContractID'])

tbl_subs['SubVendorNumber'] = tbl_subs['SubVendorNumber'].astype(str)

tbl_subs = tbl_subs[tbl_subs['SubIndustry'] != 'Human Services'] #Line should be taken out for OneNYC. Put in for LL1.

tbl_subs = tbl_subs.drop_duplicates('SubContractID')

tbl_subs = tbl_subs[[u'DOC_CD', u'DOC_DEPT_CD', u'DOC_ID', u'SubIndustry', u'SubValue',u'SubStartDate', u'Source', u'SubVendorName', u'SubVendorNumber',u'SubDescr', u'ContractID', u'SubContractID']]
tbl_subs[u'SubVendorNumber'] = tbl_subs[u'SubVendorNumber'].astype(str)
tbl_subs[u'DOC_DEPT_CD'] = tbl_subs[u'DOC_DEPT_CD'].astype(str)

tbl_subs.to_pickle(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\tblSubcontracts_FMS3\tbl_subs%s.pkl' % (str(today.date())))

tbl_subs.to_csv(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\tblSubcontracts_FMS3\tbl_subs%s.txt' % (str(today.date())))

tbl_subs.to_csv(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\tblSubcontracts_FMS3\tbl_subs_onenyc.txt')
