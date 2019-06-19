import pandas as pd
import datetime
import sys
import os

check_date = datetime.datetime.now().date()
doc_date = datetime.datetime.now().date()

def date_range(today):
    if today.month >= 7 and today.month <= 9:
        date_range_start = datetime.date(today.year - 1, 7, 1) #Dates for whole cumulative year.
        date_range_end = datetime.date(today.year, 6, 30)
        FY = today.year + 1
        FQ = 'Q4'
    elif today.month >= 10 and today.month <= 12:
        date_range_start = datetime.date(today.year, 7, 1) #First Quarter
        date_range_end = datetime.date(today.year, 9, 30)
        FY = today.year + 1
        FQ = 'Q1'
    elif today.month >= 1 and today.month <= 3:
        date_range_start = datetime.date(today.year - 1, 7, 1) #Second Quarter
        date_range_end = datetime.date(today.year - 1, 12, 31)
        FY = today.year
        FQ = 'Q2'
    elif today.month >= 4 and today.month <= 6:
        date_range_start = datetime.date(today.year - 1, 7, 1) #Third Quarter
        date_range_end = datetime.date(today.year, 3, 31)
        FY = today.year
        FQ = 'Q3'
    return [date_range_start, date_range_end, FY,FQ]

[date_range_start, date_range_end, FY, FQ] = date_range(check_date)

start_date = str(date_range_start.month) + '/' + str(date_range_start.day) + '/' + str(date_range_start.year)
end_date = str(date_range_end.month) + '/' + str(date_range_end.day) + '/' + str(date_range_end.year)

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Masters'

master = r'FY%s %s LL1 and LL129 Replicate_%s.xlsx'
omwbe = r'FY%s %s LL1 and LL129 Replicate OMWBE_%s.xlsx'

#Checks the Numbers in Each of the Four Tables in Combined Util and OMWBE Combined Util.

df = pd.read_excel(path + '\\' + master % (str(FY)[2:4],FQ, str(doc_date)), sheetname = 0, skiprows = 5, skipcols = 2, header = None)

df1 = pd.read_excel(path + '\\' + omwbe % (str(FY)[2:4],FQ, str(doc_date)), sheetname = 0, skiprows = 5, skipcols = 2, header = None)

if pd.DataFrame(df.ix[:35,2:].values - df1.ix[:35,2:].values).sum().sum() == 0:
    pass
else:
    for col in range(pd.DataFrame(df.ix[:3, 2:].values - df1.ix[:3, 2:].values).shape[1]):
        if pd.DataFrame(df.ix[:3, 2:].values - df1.ix[:3, 2:].values)[col].sum() != 0:
            print('ERROR in Column %s of Agency Summary Table' % (str(col)))
            print('Values in Master, OMWBE Master, Delta in Column %s:' % (str(col)))
            t = pd.concat([pd.DataFrame(df.ix[:3, 2:].values)[col], pd.DataFrame(df1.ix[:3, 2:].values)[col],
                           pd.DataFrame(df.ix[:3, 2:].values)[col] - pd.DataFrame(df1.ix[:3, 2:].values)[col]], axis=1)
            t.columns = ['Master', 'OMWBE Master', 'Delta']
            print(t)
            sys.exit()
        else:
            pass

df2 = pd.read_excel(path + '\\' + master % (str(FY)[2:4],FQ, str(doc_date)), sheetname = 0, skiprows = 44, skipcols = 2, header = None)

df3 = pd.read_excel(path + '\\' + omwbe % (str(FY)[2:4],FQ, str(doc_date)), sheetname = 0, skiprows = 44, skipcols = 2, header = None)

if pd.DataFrame(df2.ix[:4,2:].values - df3.ix[:4,2:].values).sum().sum() == 0:
    pass
else:
    for col in range(pd.DataFrame(df2.ix[:3, 2:].values - df3.ix[:3, 2:].values).shape[1]):
        if pd.DataFrame(df2.ix[:3, 2:].values - df3.ix[:3, 2:].values)[col].sum() != 0:
            print('ERROR in Column %s of Industry Summary Table' % (str(col)))
            print('Values in Master, OMWBE Master, Delta in Column %s:' % (str(col)))
            t = pd.concat([pd.DataFrame(df2.ix[:3, 2:].values)[col], pd.DataFrame(df3.ix[:3, 2:].values)[col],
                           pd.DataFrame(df2.ix[:3, 2:].values)[col] - pd.DataFrame(df3.ix[:3, 2:].values)[col]], axis=1)
            t.columns = ['Master', 'OMWBE Master', 'Delta']
            print(t)
            sys.exit()
        else:
            pass

df4 = pd.read_excel(path + '\\' + master % (str(FY)[2:4],FQ, str(doc_date)), sheetname = 0, skiprows = 52, skipcols = 2, header = None)

df5 = pd.read_excel(path + '\\' + omwbe % (str(FY)[2:4],FQ, str(doc_date)), sheetname = 0, skiprows = 52, skipcols = 2, header = None)

if pd.DataFrame(df4.ix[:4,2:].values - df5.ix[:4,2:].values).sum().sum() == 0:
    pass
else:
    for col in range(pd.DataFrame(df4.ix[:3, 2:].values - df5.ix[:3, 2:].values).shape[1]):
        if pd.DataFrame(df4.ix[:3, 2:].values - df5.ix[:3, 2:].values)[col].sum() not in [0, pd.DataFrame(df4.ix[:3, 2:].values)[5][0]]:
            print('ERROR in Column %s of Purchase Size Summary Table' % (str(col)))
            print('Values in Master, OMWBE Master, Delta in Column %s:' % (str(col)))
            t = pd.concat([pd.DataFrame(df4.ix[:3, 2:].values)[col], pd.DataFrame(df5.ix[:3, 2:].values)[col],
                           pd.DataFrame(df4.ix[:3, 2:].values)[col] - pd.DataFrame(df5.ix[:3, 2:].values)[col]], axis=1)
            t.columns = ['Master', 'OMWBE Master', 'Delta']
            print(t)
            sys.exit()
        else:
            pass

df6 = pd.read_excel(path + '\\' + master % (str(FY)[2:4],FQ, str(doc_date)), sheetname = 0, skiprows = 59, skipcols = 2, header = None)

df7 = pd.read_excel(path + '\\' + omwbe % (str(FY)[2:4],FQ, str(doc_date)), sheetname = 0, skiprows = 59, skipcols = 2, header = None)

if pd.DataFrame(df6.ix[:3,2:].values - df7.ix[:4,2:].values).sum().sum() == 0:
    pass
else:
    for col in range(pd.DataFrame(df6.ix[:3,2:].values - df7.ix[:3,2:].values).shape[1]):
        if pd.DataFrame(df6.ix[:3, 2:].values - df7.ix[:4,2:].values)[col].sum() != 0:
            print('ERROR in Column %s of Fiscal Quarter Summary Table' % (str(col)))
            print('Values in Master, OMWBE Master, Delta in Column %s:' % (str(col)))
            t = pd.concat([pd.DataFrame(df6.ix[:3, 2:].values)[col],pd.DataFrame(df7.ix[:4,2:].values)[col],pd.DataFrame(df6.ix[:3, 2:].values)[col] - pd.DataFrame(df7.ix[:4, 2:].values)[col]], axis = 1)
            t.columns = ['Master', 'OMWBE Master', 'Delta']
            print(t)
            sys.exit()
        else:
            pass

#Data Column - Internal Subs

df = pd.read_excel(path + '\\' + master % (str(FY)[2:4],FQ, str(doc_date)), sheetname = 'Subs', header = 0)

if set(df['SubIndustry'].unique()) < set(['Construction Services','Standardized Services','Professional Services', 'Goods']):
     pass
else:
     print ('SubIndustry Broken')
     sys.exit()

if set(df['MWBE_LL'].unique()) <= set(['LL1','LL129']):
     pass
else:
     print ('MWBE_LL Broken')
     sys.exit()

if set([x for x in df['ReportCategory'].unique() if x == x]) <= set(['Male-Owned MBE - Black','Male-Owned MBE - Asian','Male-Owned MBE - Hispanic','WBE - Asian','WBE - Black','WBE - Caucasian Woman','WBE - Hispanic']):
     pass
else:
     print ('ReportCategory Broken')
     sys.exit()

if set([x for x in df['EthGen'].unique() if x == x]) <=set(['Hispanic American','Caucasian Female', 'Black American', 'Asian American']):
      pass
else:
      print ('EthGen Broken')
      sys.exit()

#Data Column - OMWBE Master Subs Data

df1 = pd.read_excel(path + '\\' + omwbe % (str(FY)[2:4],FQ, str(doc_date)), sheetname = '3. Subs Data', header = 0)

if set(df1['SubIndustry'].unique()) < set(['Construction Services','Standardized Services','Professional Services', 'Goods']):
     pass
else:
     print ('SubIndustry Broken')
     sys.exit()

if set([x for x in df1['ReportCategory'].unique() if x == x]) <= set(['Male-Owned MBE - Black','Male-Owned MBE - Asian','Male-Owned MBE - Hispanic','WBE - Asian','WBE - Black','WBE - Caucasian Woman','WBE - Hispanic']):
     pass
else:
     print ('ReportCategory Broken')
     sys.exit()

if set([x for x in df1['EthGen'].unique() if x == x]) <= set(['Hispanic American','Caucasian Female', 'Black American', 'Asian American']):
     pass
else:
     print ('EthGen - OMWBE Master - Broken')
     sys.exit()

#Data Column - Primes Data

df = pd.read_excel(path + '\\' + master % (str(FY)[2:4],FQ, str(doc_date)), sheetname = 'Subs', header = 0)

if set(df['SubIndustry'].unique()) < set(['Construction Services','Standardized Services','Professional Services', 'Goods']):
     pass
else:
     print ('SubIndustry Broken')
     sys.exit()

if set(df['MWBE_LL'].unique()) <= set(['LL1','LL129']):
     pass
else:
     print ('MWBE_LL Broken')
     sys.exit()

if set([x for x in df['ReportCategory'].unique() if x == x]) <= set(['Male-Owned MBE - Black','Male-Owned MBE - Asian','Male-Owned MBE - Hispanic','WBE - Asian','WBE - Black','WBE - Caucasian Woman','WBE - Hispanic']):
     pass
else:
     print ('ReportCategory Broken')
     sys.exit()

if set([x for x in df['EthGen'].unique() if x == x]) <=set(['Hispanic American','Caucasian Female', 'Black American', 'Asian American']):
      pass
else:
      print ('EthGen Broken')
      sys.exit()

#Data Integrity QA - Primes Internal Masters

df = pd.read_excel(path + '\\' + master % (str(FY)[2:4],FQ, str(doc_date)), sheetname = 'Primes', header = 0)

if set([x for x in df['ReportCategory'].unique() if x == x]) <= set(['Male-Owned MBE - Black','Male-Owned MBE - Asian','Male-Owned MBE - Hispanic','WBE - Asian','WBE - Black','WBE - Caucasian Woman','WBE - Hispanic']):
     pass
else:
     print ('Primes Internal Masters ReportCategory Broken')
     sys.exit()

if set([x for x in df['MWBE_LL'].unique() if x == x]) <= set(['LL1','LL129']):
     pass
else:
     print ('Primes Internal Masters MWBE_LL Broken')
     sys.exit()

if set([x for x in df['EthGen'].unique() if x == x]) <= set(['Hispanic American','Caucasian Female', 'Black American', 'Asian American']):
     pass
else:
     print ('Primes Internal Masters EthGen Broken')
     sys.exit()

if set([x for x in df['SizeGroup'].unique() if x == x]) <= set(['Small Purchase', 'Micro Purchase','>$5M, <=$25M', '>$100K, <=$1M', '>$1M, <=$5M', '>$25M']):
     pass
else:
     print ('Primes Internal Masters SizeGroup Broken')
     sys.exit()

if set([x for x in df['MWBE_Status'].unique() if x == x]) <= set(['MWBE','Not MWBE']):
     pass
else:
     print ('Primes Internal Masters MWBE_Status Broken')
     sys.exit()

if set([x for x in df['MWBE_Status'].unique() if x == x]) <= set(['MWBE','Not MWBE']):
     pass
else:
     print ('Primes Internal Masters MWBE_Status Broken')
     sys.exit()

if set([x for x in df['REG_FY'].unique() if x == x]) <= set([FY]):
     pass
else:
     print ('Primes Internal Masters REG_FY Broken')
     sys.exit()

#Data Integrity QA - OMWBE Masters

df = pd.read_excel(path + '\\' + omwbe % (str(FY)[2:4],FQ, str(doc_date)), sheetname = '2. Primes Data', header = 0)

if set([x for x in df['ReportCategory'].unique() if x == x]) <= set(['Male-Owned MBE - Black','Male-Owned MBE - Asian','Male-Owned MBE - Hispanic','WBE - Asian','WBE - Black','WBE - Caucasian Woman','WBE - Hispanic']):
     pass
else:
     print ('Primes Internal Masters ReportCategory Broken')
     sys.exit()

if set([x for x in df['MWBE_LL'].unique() if x == x]) <= set(['LL1','LL129']):
     pass
else:
     print ('Primes Internal Masters MWBE_LL Broken')
     sys.exit()

if set([x for x in df['EthGen'].unique() if x == x]) <= set(['Hispanic American','Caucasian Female', 'Black American', 'Asian American']):
     pass
else:
     print ('Primes Internal Masters EthGen Broken')
     sys.exit()

if set([x for x in df['SizeGroup'].unique() if x == x]) <= set(['Small Purchase', 'Micro Purchase','>$5M, <=$25M', '>$100K, <=$1M', '>$1M, <=$5M', '>$25M']):
     pass
else:
     print ('Primes Internal Masters SizeGroup Broken')
     sys.exit()

if set([x for x in df['MWBE_Status'].unique() if x == x]) <= set(['MWBE','Not MWBE']):
     pass
else:
     print ('Primes Internal Masters MWBE_Status Broken')
     sys.exit()

if set([x for x in df['REG_FY'].unique() if x == x]) <= set([FY]):
     pass
else:
     print ('Primes Internal Masters REG_FY Broken')
     sys.exit()

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\QA Logs'

df = pd.read_excel(path +'\\'+ 'Masters_QA_Log.xlsx')

df['Doc_Version_Date'] = ''

df.loc[df.shape[0]] = [check_date, 'Passed', doc_date]

writer = pd.ExcelWriter(path + '\\' + 'Masters_QA_Log.xlsx', engine='xlsxwriter')

df.to_excel(writer, sheet_name = 'Masters_QA', index = False)

worksheet = writer.sheets['Masters_QA']

workbook = writer.book

center = workbook.add_format({'align': 'center', 'valign': 'vcenter'})

worksheet.set_column('A:A', 22, center)
worksheet.set_column('B:B', 12, center)
worksheet.set_column('C:C', 22, center)

writer.save()