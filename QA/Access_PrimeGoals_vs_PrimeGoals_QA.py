import LL1PROG.PrimeGoals as pg
import pyodbc
import pandas as pd
import datetime as dt
import numpy as np

output_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\QA'

t = dt.datetime.now().date()

def date_range(t):
    if t.month >= 7 and t.month <= 9:
        date_range_start = dt.date(t.year - 1, 7, 1) #Dates for whole cumulative year.
        date_range_end = dt.date(t.year, 6, 30)
        FY = t.year
        FQ = 'Q4'
    elif t.month >= 10 and t.month <= 12:
        date_range_start = dt.date(t.year, 7, 1) #First Quarter
        date_range_end = dt.date(t.year, 9, 30)
        FY = t.year + 1
        FQ = 'Q1'
    elif t.month >= 1 and t.month <= 3:
        date_range_start = dt.date(t.year - 1, 7, 1) #Second Quarter
        date_range_end = dt.date(t.year - 1, 12, 31)
        FY = t.year
        FQ = 'Q2'
    elif t.month >= 4 and t.month <= 6:
        date_range_start = dt.date(t.year - 1, 7, 1) #Third Quarter
        date_range_end = dt.date(t.year, 3, 31)
        FY = t.year
        FQ = 'Q3'
    return [date_range_start, date_range_end, FQ, FY]

[date_range_start, date_range_end, FQ, FY] = date_range(t)

def industry_map(x):
    if x == 'Architecture/Engineering':
        return 'Professional Services'
    else:
        return x

industry_map = np.vectorize(industry_map)

Access_Path = r'S:\Contracts\Research and IT\Procurement Indicators\FY 2019 Procurement Indicators\MWBE\Working.accdb;'
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

sql = ['Select Agency, ContractID, Industry2, ContractValue FROM PrimeGoals_AppendixC_FY%s_%s;' % (str(FY)[2:4], str(FQ))]

crsr.execute(sql[0])

df = pd.DataFrame([tuple(x) for x in crsr.fetchall()], columns = ['Agency','ContractID','Industry', 'ContractValue'])

pvt = df.groupby(['Agency', 'Industry'])['ContractID'].nunique()
pvt_dol = df.groupby(['Agency', 'Industry'])['ContractValue'].sum().astype(float)

df1 = pg.prime_goals

df1['Industry'] = industry_map(df1['Industry'])

pvt2 = df1.groupby(['Agency', 'Industry'])['ContractID'].nunique()
pvt2_dol = df1.groupby(['Agency', 'Industry'])['ContractValue'].sum().astype(float)

if (pvt - pvt2).sum() <0.001:
    pass

if (pvt_dol - pvt2_dol).sum() <0.001:
    pass

if len(set(df['ContractID']) - set(df1['ContractID'])) == 0:
    pass

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Compliance Appendices'

file = r'Table C and D.MWBE Participation Goals FY%s %s %s.xlsx' % (str(FY)[2:4],str(FQ),str(t))

pg_file = pd.read_excel(path +'\\'+ file, sheetname = 'Prime Goals Data', header = 0)

if len(set(df['ContractID']) - set(df1['ContractID'])) == 0:
    pass

if len(set(df['ContractID']) - set(df1['ContractID'])) == 0:
    pass

df = pd.read_excel(output_path +'\\'+'Prime_Goals_QA.xlsx', header = 0)

df.loc[df.shape[0]] = ['Yes','Yes']

writer = pd.ExcelWriter(output_path + '\\' + 'Prime_Goals_QA.xlsx', engine='xlsxwriter')

df.to_excel(writer, sheet_name = 'Prime_Goals_QA', index = False)

worksheet = writer.sheets['Prime_Goals_QA']

workbook = writer.book
center = workbook.add_format({'align': 'center', 'valign': 'vcenter'})

worksheet.set_column('A:A', 21, center)
worksheet.set_column('B:B', 23, center)
worksheet.set_column('C:C', 19, center)
worksheet.set_column('D:D', 14, center)
worksheet.set_column('E:E', 11, center)
worksheet.set_column('F:F', 11, center)

writer.save()