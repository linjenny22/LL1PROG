import pandas as pd
import pyodbc
import LL1PROG.PrimeUtil
import LL1PROG.SubUtil
import datetime as dt

pu = LL1PROG.PrimeUtil.prime_util
su = LL1PROG.SubUtil.sub_util

today = dt.datetime.now()

def date_range(today):
    if today.month >= 7 and today.month <= 9:
        date_range_start = dt.date(today.year - 1, 7, 1) #Dates for whole cumulative year.
        date_range_end = dt.date(today.year, 6, 30)
        FY = today.year + 1
        FQ = 'Q4'
    elif today.month >= 10 and today.month <= 12:
        date_range_start = dt.date(today.year, 7, 1) #First Quarter
        date_range_end = dt.date(today.year, 9, 30)
        FY = today.year + 1
        FQ = 'Q1'
    elif today.month >= 1 and today.month <= 3:
        date_range_start = dt.date(today.year - 1, 7, 1) #Second Quarter
        date_range_end = dt.date(today.year - 1, 12, 31)
        FY = today.year
        FQ = 'Q2'
    elif today.month >= 4 and today.month <= 6:
        date_range_start = dt.date(today.year - 1, 7, 1) #Third Quarter
        date_range_end = dt.date(today.year, 3, 31)
        FY = today.year
        FQ = 'Q3'
    return [date_range_start, date_range_end, FY,FQ]

[date_range_start, date_range_end, FY, FQ] = date_range(today)

start_date = str(date_range_start.month) + '/' + str(date_range_start.day) + '/' + str(date_range_start.year)
end_date = str(date_range_end.month) + '/' + str(date_range_end.day) + '/' + str(date_range_end.year)

Access_Path = r'S:\Contracts\Research and IT\Procurement Indicators\FY 2019 Procurement Indicators\MWBE2019.accdb;'
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

sql = ['Select Agency, ContractID, Industry, ContractValue FROM qryCompRpt_PrimeUtil_FY%s_%sa_Meth72;'  % (str(FY)[2:4], str(FQ))]

crsr.execute(sql[0])

df = pd.DataFrame([tuple(x) for x in crsr.fetchall()], columns = ['Agency','ContractID','Industry', 'ContractValue'])

df.loc[:,'ContractValue'] = df['ContractValue'].astype(float)

counts = df.groupby(['Agency','Industry'])['ContractID'].nunique().reset_index()['ContractID'] - pu.groupby(['Agency', 'Industry'])['ContractID'].nunique().reset_index()['ContractID']
dollars = df.groupby(['Agency', 'Industry'])['ContractValue'].sum().reset_index()['ContractValue'] - pu.groupby(['Agency', 'Industry'])['ContractValue'].sum().reset_index()['ContractValue']

if counts.sum() <0.01 and dollars.sum()<0.001:
    pass

####

Access_Path = r'S:\Contracts\Research and IT\Procurement Indicators\FY 2019 Procurement Indicators\MWBE2019.accdb;'
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

sql = ['Select Agency, ContractID, SubIndustry, SubValue FROM qryCompRpt_SubUtil_FY%s_%s;'  % (str(FY)[2:4], str(FQ))]

crsr.execute(sql[0])

df = pd.DataFrame([tuple(x) for x in crsr.fetchall()], columns = ['Agency', 'ContractID', 'SubIndustry','SubValue'])
df.loc[:,'SubValue'] = df['SubValue'].astype(float)

counts = df.groupby(['Agency', 'SubIndustry'])['ContractID'].count().reset_index()['ContractID'] - su.groupby(['Agency', 'SubIndustry'])['ContractID'].nunique().reset_index()['ContractID']
dollars = df.groupby(['Agency', 'SubIndustry'])['SubValue'].sum().reset_index()['SubValue'] - su.groupby(['Agency', 'SubIndustry'])['SubValue'].sum().reset_index()['SubValue']

if counts.sum() <0.01 and dollars.sum()<0.001:
    pass