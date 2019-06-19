import pandas as pd
import datetime as dt
import pyarrow as pa
import pyarrow.parquet as pq
import time
import numpy as np
import pickle
import datetime
import os
from datetime import timedelta
import pyodbc

t = dt.datetime.now().date()

print (t)

data_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\PRODUCTION\Datasets'

def date_range(t):
    if t.month >= 7 and t.month <= 9:
        date_range_start = datetime.date(t.year - 1, 7, 1) #Dates for whole cumulative year.
        date_range_end = datetime.date(t.year, 6, 30)
        FY = t.year
        FQ = 'Q4'
    elif t.month >= 10 and t.month <= 12:
        date_range_start = datetime.date(t.year, 7, 1) #First Quarter
        date_range_end = datetime.date(t.year, 9, 30)
        FY = t.year + 1
        FQ = 'Q1'
    elif t.month >= 1 and t.month <= 3:
        date_range_start = datetime.date(t.year - 1, 7, 1) #Second Quarter
        date_range_end = datetime.date(t.year - 1, 12, 31)
        FY = t.year
        FQ = 'Q2'
    elif t.month >= 4 and t.month <= 6:
        date_range_start = datetime.date(t.year - 1, 7, 1) #Third Quarter
        date_range_end = datetime.date(t.year, 3, 31)
        FY = t.year
        FQ = 'Q3'
    return [date_range_start, date_range_end, FQ, FY]

[date_range_start, date_range_end, FQ, FY] = date_range(t)

start_date = str(date_range_start.month) + '/' + str(date_range_start.day) + '/' + str(date_range_start.year)
end_date = str(date_range_end.month) + '/' + str(date_range_end.day) + '/' + str(date_range_end.year)

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Open Contracts'

df = pq.read_table(path +'\\'+'open_contracts_%s.parquet' % (str(t))).to_pandas()

df.columns = ['Agency', 'DOC_CD','DOC_DEPT_CD','DOC_ID','ContractID','EPIN','ContractValue','MWBE_LL','Method', 'VendorTIN','VendorNumber','VendorName','Purpose','StartDate','EndDate','RegistrationDate','Industry','ExcludeAll','ExcludeCategory','STATE_FED_FUNDED','MWBE72Fed','MWBE_GOALS', 'NoTSPReason', 'Base_EPIN', 'TSP','Goal_Black','Goal_Asian', 'Goal_Hispanic', 'Goal_Woman', 'Goal_Unspecified']

start_date = pd.to_datetime(start_date) - timedelta(days = 1)

end_date = pd.to_datetime(end_date) + timedelta(days = 1)

df['RegistrationDate'] = pd.to_datetime(df['RegistrationDate'])

df = df[(df['RegistrationDate']>start_date) & (df['RegistrationDate']<end_date)]

oc = df[['Agency', 'ContractID', 'VendorName','VendorNumber','Method','Industry', 'Purpose', 'ExcludeAll','ExcludeCategory', 'RegistrationDate','StartDate', 'EndDate', 'ContractValue']]

oc = pd.concat([oc[(oc['ExcludeAll'] == True) & (oc['ExcludeCategory'] == 16)], oc[oc['ExcludeAll'] == False]])

#Subs Lockin Date

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\LL1PROG'

sl = pd.read_pickle(path + '\\' + r'subs_generation_dates%s_%s.pkl' % (str(FY), str(FQ)))

lock_in_date = min(sl)

print ('Subs Lock In Date: %s' % (str(lock_in_date)))

subs = pd.read_csv(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\tblSubcontracts_FMS3\tbl_subs%s.txt' % (str(lock_in_date)), header = 0)

subs.loc[:, 'SubDescr'] = pd.Series([x.encode('utf-8') for x in subs['SubDescr']])

subs['SubContractID'] = subs['DOC_CD'].astype(str) + subs['DOC_DEPT_CD'].astype(str) + subs['DOC_ID'].astype(str) + subs['SubVendorNumber'].astype(str) + subs['SubVendorName'].astype(str) + subs['SubDescr'].astype(str)

Access_Path = r'S:\Contracts\Research and IT\Procurement Indicators\FY 2019 Procurement Indicators\MWBE\Working.accdb;'
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

sql = ["""SELECT FMS_VENDOR_ID FROM tblSBS_EBE%s_%s;""" % (str(FY),str(FQ)),
        """SELECT FMS_VENDOR_ID FROM tblSBS_LBE%s_%s;""" % (str(FY),str(FQ))]

crsr.execute(sql[0])

sbs_ebe = pd.DataFrame([tuple(x) for x in crsr.fetchall()], columns = ['FMS_VENDOR_ID'])

crsr.execute(sql[1])

sbs_lbe = pd.DataFrame([tuple(x) for x in crsr.fetchall()], columns = ['FMS_VENDOR_ID'])

EBE_Primes = oc[oc['VendorNumber'].isin(sbs_ebe['FMS_VENDOR_ID'])]

LBE_Primes = oc[oc['VendorNumber'].isin(sbs_lbe['FMS_VENDOR_ID'])]

LBE_Primes = LBE_Primes[LBE_Primes['ContractID'].isnull() == False]
LBE_Primes = LBE_Primes.drop_duplicates(['ContractID'])

EBE_Primes = EBE_Primes[EBE_Primes['ContractID'].isnull() == False]
EBE_Primes = EBE_Primes.drop_duplicates(['ContractID'])

LBE_Subs = subs[subs['SubVendorNumber'].isin(sbs_lbe['FMS_VENDOR_ID'])]
EBE_Subs = subs[subs['SubVendorNumber'].isin(sbs_ebe['FMS_VENDOR_ID'])]

LBE_Subs = LBE_Subs[LBE_Subs['SubVendorNumber'].isnull() == False]
# LBE_Subs = LBE_Subs.drop_duplicates(['SubContractID'])

EBE_Subs = EBE_Subs[EBE_Subs['SubVendorNumber'].isnull() == False]
# EBE_Subs = EBE_Subs.drop_duplicates(['SubContractID'])

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\EBE LBE SBS'

writer = pd.ExcelWriter(path + '\\' + r'LBE EBE JV FY%s %s_%s.xlsx' % (str(FY)[2:4],FQ, str(t)), engine = 'xlsxwriter')

workbook = writer.book
worksheet = workbook.add_worksheet('1. FY%s %s EBE and LBE' % (str(FY)[2:4],FQ))

#Light Blue, RBG R 189 G 215 B 238
light_blue = workbook.add_format({'bold':1, 'bg_color':'#BDD7EE', 'border':1})
light_blue.set_align('center')
light_blue.set_align('vcenter')
dollar_signs = workbook.add_format({'bold':1,'num_format': '$###,###,###', 'border':1})
percent = workbook.add_format({'bold':1,'num_format': '##%', 'border':1})
bold = workbook.add_format({'bold':1, 'border':1})

gridlines = workbook.add_format({'bold': 1,'border': 1,'align': 'right', 'valign': 'vcenter'})

worksheet.write('B4', 'LBE Prime', light_blue)
worksheet.write('B5', 'LBE Sub', light_blue)
worksheet.write('B6', 'EBE Prime', light_blue)
worksheet.write('B7', 'EBE Sub', light_blue)
worksheet.write('C3','#', light_blue)
worksheet.write('D3','$', light_blue)
worksheet.merge_range('C2:D2','FY%s %s LBE and EBE' % (str(FY)[2:4], FQ), light_blue)
worksheet.merge_range('B2:B3','Type', light_blue)
worksheet.write('C4', LBE_Primes['ContractID'].nunique(), gridlines)
worksheet.write('C5', LBE_Subs['SubContractID'].nunique(), gridlines)
worksheet.write('C6', EBE_Primes['ContractID'].nunique(), gridlines)
worksheet.write('C7', EBE_Subs['SubContractID'].nunique(), gridlines)
worksheet.write('D4', LBE_Primes['ContractValue'].sum(), dollar_signs)
worksheet.write('D5', LBE_Subs['SubValue'].sum(), dollar_signs)
worksheet.write('D6', EBE_Primes['ContractValue'].sum(), dollar_signs)
worksheet.write('D7', EBE_Subs['SubValue'].sum(), dollar_signs)

worksheet.conditional_format('C4:C7', {'type': 'cell', 'criteria': '>=', 'value': 0, 'format':   bold})

worksheet.conditional_format('D4:D7', {'type': 'cell', 'criteria': '>=', 'value': 0, 'format':   dollar_signs})

worksheet.set_row(1, 18)
worksheet.set_row(2, 17)
worksheet.set_row(3, 17)
worksheet.set_row(4, 17)
worksheet.set_row(5, 17)
worksheet.set_row(6, 17)

worksheet.set_column('B:B', 13)
worksheet.set_column('C:C', 13)
worksheet.set_column('D:D', 13)

writer.save()