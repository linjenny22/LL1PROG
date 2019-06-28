import pyodbc
import pandas as pd
import datetime
import string
import re
import math
import os

#Date Ranges: non-cumulative FQ
#3 Data Sets Dependencies -- 1 - fms_lookup.py : doc_hdr.txt -- 2 - FMS_PO_DOC_AWDDET.py doc_awddet.txt -- 3 - fms_lookup5.py -- orig_reg_date.txt

data_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\FY19 Q3 Pre Prod\Datasets'
int_data_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\TADs Creation'

t = datetime.datetime.now().date()

#Non Cumulative Calendar Dates

def date_range(today): #NON CUMULATIVE DATES
    if today.month >= 7 and today.month <= 9:
        date_range_start = datetime.date(today.year, 3, 1) #Fourth Quarter Only
        date_range_end = datetime.date(today.year, 6, 30)
        FY = today.year + 1
        FQ = 'Q4'
    elif today.month >= 10 and today.month <= 12:
        date_range_start = datetime.date(today.year, 7, 1) #First Quarter Only
        date_range_end = datetime.date(today.year, 9, 30)
        FY = today.year + 1
        FQ = 'Q1'
    elif today.month >= 1 and today.month <= 3:
        date_range_start = datetime.date(today.year-1, 10, 1) #Second Quarter Only
        date_range_end = datetime.date(today.year-1, 12, 31)
        FY = today.year
        FQ = 'Q2'
    elif today.month >= 4 and today.month <= 6:
        date_range_start = datetime.date(today.year, 1, 1) #Third Quarter Only
        date_range_end = datetime.date(today.year, 3, 31)
        FY = today.year
        FQ = 'Q3'
    return [date_range_start, date_range_end, FY, FQ]

[date_range_start, date_range_end, FY, FQ] = date_range(t)

start_date = str(date_range_start.month) + '/' + str(date_range_start.day) + '/' + str(date_range_start.year)
print(start_date)
end_date = str(date_range_end.month) + '/' + str(date_range_end.day) + '/' + str(date_range_end.year)
print(end_date)

sd = pd.to_datetime(start_date)
ed = pd.to_datetime(end_date)

#first query is qryTAD_MWBE_Primes
#second query is qryLoadContractToFindOrig

data_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\FY19 Q3 Pre Prod\Datasets'
int_data_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\TADs Creation'

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\PIP\Week 4\3.2'

df = pd.read_csv(path +'\\'+ r'open_contracts_unit_test.txt', header = None) #% (str(FY)[2:4], str(t)), low_memory = False)

df.columns = ['Agency', 'DOC_CD','DOC_DEPT_CD','DOC_ID','ContractID','EPIN','ContractValue','MWBE_LL','Method', 'VendorTIN','VendorNumber','VendorName','Purpose','StartDate','EndDate','RegistrationDate','Industry','ExcludeAll','ExcludeCategory','STATE_FED_FUNDED','MWBE72Fed','MWBE_GOALS', 'NoTSPReason', 'Base_EPIN', 'TSP','Goal_Black','Goal_Asian', 'Goal_Hispanic', 'Goal_Woman', 'Goal_Unspecified']

df = df.drop_duplicates(subset = ['ContractID'], keep = 'last')

df = df[~df['Agency'].isin(['DOE','City Hall'])]
df = df[df['Method'].isin(["Accelerated","Competitive Sealed Bid","Demonstration Project","Innovative","Micro Purchase","Negotiated Acquisition","Negotiated Acquisition Extension","Renewal","Request for Proposal","Small Purchase","Micropurchase", "MWBE 72"])]
df = df[df['ContractValue']>20000]

df.loc[:,'RegistrationDate'] = pd.to_datetime(df['RegistrationDate'])

df = df[(df['RegistrationDate']>=sd) & (df['RegistrationDate']<=ed)]

df = df[(df['Goal_Black'].isnull()) & (df['Goal_Woman'].isnull()) & (df['Goal_Hispanic'].isnull()) & (df['Goal_Asian'].isnull()) & (df['Goal_Unspecified'].isnull())]

df1 = df[df['ExcludeAll']==False]

df2 = df[(df['ExcludeAll']==True) & (df['ExcludeCategory']==16)]

df = pd.concat([df1,df2])

if 'nonpFY%s.txt' % (str(FY)[2:4]) in set(os.listdir(data_path)):
    nonp = pd.read_csv(data_path +'\\'+ 'nonpFY%s.txt' % (str(FY)[2:4]))

df = df[~df['VendorTIN'].isin(nonp['EIN'])]


Access_Path = r'S:\Contracts\Research and IT\Procurement Indicators\FY 2019 Procurement Indicators\MWBE2019.accdb;'
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

# #first query is qryTAD_MWBE_Primes
# #second query is qryLoadContractToFindOrig

sql = [r'SELECT AWARD_METHOD_CD, AwdMethodID FROM tblAMRT;',
       r'SELECT AwdMethod_ID, AwdMethodName FROM tblReportAwardMethods;']

df = df[['Agency', 'DOC_DEPT_CD', 'ContractID','VendorName', 'Purpose', 'Method', 'Industry', 'RegistrationDate', 'ContractValue']]

df['DOC_DEPT_CD'] = df['DOC_DEPT_CD'].astype(str)

def agency_mapping(row):
    if row['Agency'] == 'DCAS' and row['DOC_DEPT_CD'] == str(856):
        return 'DCAS_856'
    elif row['Agency'] == 'DCAS' and row['DOC_DEPT_CD'] == str(857):
        return 'DCAS_857'
    else:
        return row['Agency']

df.loc[:,'Agency'] = df.apply(agency_mapping, axis = 1)

if 'DCAS_856' in df['Agency'].unique(): #spot checking that DCAS was split appropriately
    pass
else:
    print('DCAS_856 TAD Not Made')
    #exit()
    pass

if 'DCAS_857' in df['Agency'].unique(): #spot checking that DCAS was split appropriately
    pass
else:
    print('DCAS_857 TAD Not Made')
    pass

if len(df['ContractID'][~df['ContractID'].isnull()].unique()) <=1000 and len(df['ContractID'][~df['ContractID'].isnull()].unique()) !=0:

    tup = str(tuple(df['ContractID'][~df['ContractID'].isnull()].unique()))
    tup = tup.replace('\'', '\\\'')
    tup = tup.replace('u', '')
    print (tup)

    fname = 'doc_hdr.py'
    line1 = """import pandas as pd\nimport pyodbc\nimport time\nimport xlsxwriter\nimport datetime\nimport string\nimport re\nimport numpy as np\nimport math\nimport cx_Oracle\n\nuid = \'jlin\'\npwd = \'Purple22\'\nservice = \'cwprd1.fisalan.nycnet\'\ndb = cx_Oracle.connect(uid + \"/\" + pwd + \"@\" + service)\n\ncursor = db.cursor()\n\nsql_list = [\'Select DOC_ID, DOC_CD, DOC_DEPT_CD, DOC_CD || DOC_DEPT_CD || DOC_ID "ContractID", DOC_VERS_NO, REG_DT, DOC_PHASE_CD, DOC_STA_CD, PO_REPL_DOC_CD, PO_REPL_DEPT_CD, PO_REPL_ID FROM FMS01.PO_DOC_HDR WHERE DOC_CD || DOC_DEPT_CD || DOC_ID in %s\', \'Select DOC_ID, DOC_CD, DOC_DEPT_CD, DOC_CD || DOC_DEPT_CD || DOC_ID "ContractID", DOC_VERS_NO, REG_DT, DOC_PHASE_CD, DOC_STA_CD, PO_REPL_DOC_CD, PO_REPL_DEPT_CD, PO_REPL_ID FROM FMS01.MA_DOC_HDR WHERE DOC_CD || DOC_DEPT_CD || DOC_ID in %s\']\ncursor.execute(sql_list[0])\ndf1 = pd.DataFrame([[x for x in y] for y in cursor], columns = [\'DOC_ID\',\'DOC_CD\', \'DOC_DEPT_CD\', \'ContractID\', \'DOC_VERS_NO\', \'REG_DT\', \'DOC_PHASE_CD\', \'DOC_STA_CD\', \'PO_REPL_DOC_CD\', \'PO_REPL_DEPT_CD\', \'PO_REPL_ID\'])\ncursor.execute(sql_list[1])\ndf2 = pd.DataFrame([[x for x in y] for y in cursor], columns = [\'DOC_ID\',\'DOC_CD\', \'DOC_DEPT_CD\', \'ContractID\', \'DOC_VERS_NO\', \'REG_DT\', \'DOC_PHASE_CD\', \'DOC_STA_CD\',\'PO_REPL_DOC_CD\', \'PO_REPL_DEPT_CD\', \'PO_REPL_ID\'])\ndoc_hdr = pd.concat([df1,df2])\ndoc_hdr = doc_hdr.drop_duplicates(subset = ['ContractID'])\ndoc_hdr.to_csv(r'S:\Contracts\Research and IT\\08 - MWBE\DAS Only\\09 - Python and R Scripts\Development\LL1 TADs Creation Outbound\Outbound TADs Creation\doc_hdr.txt')""" % (tup, tup)
    with open(fname, 'w') as f:  # w stands for writing
        f.write('{}'.format(line1))  # .format() replaces the {}

    os.system('python doc_hdr.py')  # writes over both py and txt files

    doc_hdr = pd.read_table(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\LL1 TADs Creation Outbound\Outbound TADs Creation\doc_hdr.txt', header=0, sep=',', dtype={'PO_REPL_DEPT_CD': str, 'PO_REPL_ID': str})

#Run 1 - DOC_HDR Localization.py updated with tuples above in line 83. currently in C:\Databases with Python 2.7 32-bit. The output will be returned this folder.

doc_hdr = doc_hdr[['DOC_ID','DOC_CD','DOC_DEPT_CD','ContractID','DOC_VERS_NO','REG_DT','DOC_PHASE_CD','DOC_STA_CD','PO_REPL_DOC_CD','PO_REPL_DEPT_CD','PO_REPL_ID']]

doc_hdr['OrigContractID'] = doc_hdr['PO_REPL_DOC_CD'].astype(str) + doc_hdr['PO_REPL_DEPT_CD'].astype(str) + doc_hdr['PO_REPL_ID'].astype(str)
doc_hdr['OrigContractID'] = doc_hdr['OrigContractID'].str.replace('nannannan', '')

join1 = doc_hdr[['ContractID', 'OrigContractID']]

join2 = df.merge(join1, how = 'left', left_on = 'ContractID', right_on = 'ContractID')

def remove_nan(row):
     if row['OrigContractID'] == 'nannn':
         return ''
     elif row['OrigContractID'] == 'nannannan':
         return ''
     else:
         return row['OrigContractID']

join2.loc[:,'OrigContractID'] = join2.apply(remove_nan, axis = 1)
join2 = join2.drop_duplicates(subset = ['ContractID', 'OrigContractID'])

if len(join2['OrigContractID'].dropna().unique())>1:
    tup = str(tuple(join2['OrigContractID'].dropna().unique()))
    tup = tup.replace('\'', '\\\'')
    tup = tup.replace('u', '')
else:
    tup = str(tuple(join2['OrigContractID'].dropna().unique()))
    tup = tup.replace('\'', '\\\'')
    tup = tup.replace('u', '')
    tup = tup.replace(',', '')

recur = []

A = 0

while A == 0:  # Spaceholder

    fname = 'orig_reg_date.py'
    line1 = """import cx_Oracle\nimport pandas as pd\nuid = \'jlin\'\npwd = \'Purple22\'\nservice = \'cwprd1.fisalan.nycnet\'\ndb = cx_Oracle.connect(uid + \"/\" + pwd + \"@\" + service)\n\ncursor = db.cursor()\nsql_list = [\'Select DOC_ID, DOC_CD, DOC_DEPT_CD, DOC_CD || DOC_DEPT_CD || DOC_ID "ContractID", DOC_VERS_NO, REG_DT, DOC_PHASE_CD, DOC_STA_CD, PO_REPL_DOC_CD, PO_REPL_DEPT_CD, PO_REPL_ID FROM FMS01.PO_DOC_HDR WHERE DOC_CD || DOC_DEPT_CD || DOC_ID in %s\', \'Select DOC_ID, DOC_CD, DOC_DEPT_CD, DOC_CD || DOC_DEPT_CD || DOC_ID "ContractID", DOC_VERS_NO, REG_DT, DOC_PHASE_CD, DOC_STA_CD, PO_REPL_DOC_CD, PO_REPL_DEPT_CD, PO_REPL_ID FROM FMS01.MA_DOC_HDR WHERE DOC_CD || DOC_DEPT_CD || DOC_ID in %s\']\ncursor.execute(sql_list[0])\ndf1 = pd.DataFrame([[x for x in y] for y in cursor], columns = [\'DOC_ID\',\'DOC_CD\', \'DOC_DEPT_CD\', \'ContractID\', \'DOC_VERS_NO\', \'REG_DT\', \'DOC_PHASE_CD\', \'DOC_STA_CD\', \'PO_REPL_DOC_CD\', \'PO_REPL_DEPT_CD\', \'PO_REPL_ID\'])\ncursor.execute(sql_list[1])\ndf2 = pd.DataFrame([[x for x in y] for y in cursor], columns = [\'DOC_ID\',\'DOC_CD\', \'DOC_DEPT_CD\', \'ContractID\', \'DOC_VERS_NO\', \'REG_DT\', \'DOC_PHASE_CD\', \'DOC_STA_CD\', \'PO_REPL_DOC_CD\', \'PO_REPL_DEPT_CD\', \'PO_REPL_ID\'])\ndoc_hdr = pd.concat([df1,df2])\ndoc_hdr = doc_hdr.drop_duplicates(subset = ['ContractID'])\ndoc_hdr = doc_hdr.dropna(thresh = 3)\ndoc_hdr.to_csv('S:\Contracts\Research and IT\\\\08 - MWBE\DAS Only\\\\09 - Python and R Scripts\Development\LL1 TADs Creation Outbound\Outbound TADs Creation\orig_reg_date1.txt')""" % (
        tup, tup)
    with open(fname, 'w') as f:  # w stands for writing
        f.write('{}'.format(line1))  # .format() replaces the {}

    os.system('python orig_reg_date.py')

    orig_reg_dt = pd.read_csv(
        r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\LL1 TADs Creation Outbound\Outbound TADs Creation\orig_reg_date1.txt',
        dtype={'PO_REPL_DEPT_CD': str, 'PO_REPL_ID': str})

    orig_reg_dt = orig_reg_dt[['ContractID', 'PO_REPL_DOC_CD', 'PO_REPL_DEPT_CD',
                               'PO_REPL_ID']]  # contractid here is orig contract id from prior step

    orig_reg_dt['OrigContractID'] = orig_reg_dt['PO_REPL_DOC_CD'] + orig_reg_dt['PO_REPL_DEPT_CD'] + orig_reg_dt[
        'PO_REPL_ID']

    recur.append(pd.DataFrame(orig_reg_dt))

    orig_reg_dt = orig_reg_dt.dropna(subset=['PO_REPL_DOC_CD', 'PO_REPL_DEPT_CD', 'PO_REPL_ID'])

    if len(orig_reg_dt['OrigContractID'].dropna().unique()) > 1:
        tup = str(tuple(orig_reg_dt['OrigContractID'].dropna().unique()))
        tup = tup.replace('\'', '\\\'')
        tup = tup.replace('u', '')
    else:
        tup = str(tuple(orig_reg_dt['OrigContractID'].dropna().unique()))
        tup = tup.replace('\'', '\\\'')
        tup = tup.replace('u', '')
        tup = tup.replace(',', '')

    if orig_reg_dt.shape[0] == 0:
        print('done')
        A = 1
    elif orig_reg_dt.shape[0] == 1 and orig_reg_dt['ContractID'].item() == orig_reg_dt['OrigContractID'].item():
        print('done')
        A = 1
    elif orig_reg_dt.shape[0] > 1 and orig_reg_dt['ContractID'].equals(orig_reg_dt['OrigContractID']):
        print('done')
        A = 1
    else:
        print('More Contract Lookups Needed!')

df = pd.concat(recur)
df = df.drop_duplicates(['ContractID'])

def f(row):
    if row.last_valid_index() is None:
        return np.nan
    else:
        return row[row.last_valid_index()]

df['last'] = df.apply(f, axis = 1)

dct = {a:b for a,b in zip(df['ContractID'], df['last'])}

l = []

def recur(row):
    while row in dct and dct[row] != '' and dct[row] != row:
        l.append(dct[row])
        if row in dct:
            row = dct[row]
        else:
            break
    else:
        l.append(row)
    return l[-1]

join2.loc[:, 'First'] = [recur(x) for x in join2['OrigContractID']]

tup = str(tuple(join2['OrigContractID'].dropna().unique()))
tup = tup.replace('\'','\\\'')
tup = tup.replace('u', '')

fname = 'po_doc_awddet.py'
line1 = """import cx_Oracle\nimport pandas as pd\nuid = \'jlin\'\npwd = \'Purple22\'\nservice = \'cwprd1.fisalan.nycnet\'\ndb = cx_Oracle.connect(uid + \"/\" + pwd + \"@\" + service)\n\ncursor = db.cursor()\nsql_list = [\'Select DOC_CD, DOC_DEPT_CD, DOC_ID, DOC_VERS_NO, AWD_METH_CD FROM FMS01.MA_DOC_AWDDET WHERE DOC_CD || DOC_DEPT_CD || DOC_ID IN %s\', \'Select DOC_CD, DOC_DEPT_CD, DOC_ID, DOC_VERS_NO, AWD_METH_CD FROM FMS01.PO_DOC_AWDDET WHERE DOC_CD || DOC_DEPT_CD || DOC_ID IN %s\']\ncursor.execute(sql_list[0])\ndf1 = pd.DataFrame([[x for x in y] for y in cursor], columns = [\'DOC_CD\', \'DOC_DEPT_CD\', \'DOC_ID\', \'DOC_VERS_NO\', \'AWD_METH_CD\'])\ncursor.execute(sql_list[1])\ndf2 = pd.DataFrame([[x for x in y] for y in cursor], columns = [\'DOC_CD\', \'DOC_DEPT_CD\', \'DOC_ID\', \'DOC_VERS_NO\', \'AWD_METH_CD\'])\ndf3 = pd.concat([df1, df2])\ndf3.to_csv('S:\Contracts\Research and IT\\\\08 - MWBE\DAS Only\\\\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\TADs Creation\doc_awddet.txt')""" % (tup, tup)
with open(fname, 'w') as f: #w stands for writing
     f.write('{}'.format(line1)) #.format() replaces the {}

os.system('python po_doc_awddet.py')

po_doc_awddet = pd.read_table(int_data_path+'\\'+r'doc_awddet.txt', header = 0, sep = ',', dtype = {'PO_REPL_DEPT_CD':str, 'PO_REPL_ID':str})

po_doc_awddet = po_doc_awddet[['DOC_CD','DOC_DEPT_CD','DOC_ID','DOC_VERS_NO','AWD_METH_CD']]

po_doc_awddet['ContractID'] = po_doc_awddet['DOC_CD'].astype(str) + po_doc_awddet['DOC_DEPT_CD'].astype(str) + po_doc_awddet['DOC_ID'].astype(str)

po_doc_awddet['AWD_METH_CD2'] = pd.Series([x.split('.')[0] if '.' in x else x for x in po_doc_awddet['AWD_METH_CD'].astype(str)])
po_doc_awddet['AWD_METH_CD2'] = pd.Series([x.lstrip('0') if x not in ['0W1','0W2'] else x for x in po_doc_awddet['AWD_METH_CD2']])

#####

join3 = join2.merge(po_doc_awddet, how = 'left', left_on = 'OrigContractID', right_on = 'ContractID')

join3 = join3.drop_duplicates(subset = ['OrigContractID', 'ContractID_x'], keep = 'first')

crsr.execute(sql[0])
data = crsr.fetchall()

tblAMRT = pd.DataFrame([[x for x in y] for y in data], columns = ['AWARD_METHOD_CD', 'AwdMethodID'])

tblAMRT['AWARD_METHOD_CD'] = tblAMRT['AWARD_METHOD_CD'].fillna(0)
tblAMRT['AWARD_METHOD_CD'] = tblAMRT['AWARD_METHOD_CD'].astype(str)
tblAMRT['AWARD_METHOD_CD'] = pd.Series([x.lstrip('0') if x not in ['0W1','0W2'] else x for x in tblAMRT['AWARD_METHOD_CD']])
tblAMRT['AWARD_METHOD_CD'] = pd.Series([x.rstrip(' ') if x not in ['0W1','0W2'] else x for x in tblAMRT['AWARD_METHOD_CD']])
tblAMRT['AWARD_METHOD_CD2'] = pd.Series([x.split(' ')[0] if ' ' in x else x for x in tblAMRT['AWARD_METHOD_CD'].astype(str)])

join4 = join3.merge(tblAMRT, how = 'left', left_on = 'AWD_METH_CD2', right_on = 'AWARD_METHOD_CD2') #NOT WORKING

join4['AwdMethodID'] = pd.Series([x.split('.')[0] if '.' in x else x for x in join4['AwdMethodID'].astype(str)])

crsr.execute(sql[1])
data = crsr.fetchall()

tblReportAwardMethods = pd.DataFrame([[x for x in y] for y in data], columns = ['AwdMethod_ID', 'AwdMethodName'])

tblReportAwardMethods['AwdMethod_ID'] = tblReportAwardMethods['AwdMethod_ID'].astype(str)

join5 = join4.merge(tblReportAwardMethods, how = 'left', left_on = 'AwdMethodID', right_on = 'AwdMethod_ID') #correct

tup = str(tuple(join5['OrigContractID'].dropna().unique()))
tup = tup.replace('\'','\\\'')
tup = tup.replace('u', '')

fname = 'orig_reg_date.py'
line1 = """import cx_Oracle\nimport pandas as pd\nuid = \'jlin\'\npwd = \'Purple22\'\nservice = \'cwprd1.fisalan.nycnet\'\ndb = cx_Oracle.connect(uid + \"/\" + pwd + \"@\" + service)\n\ncursor = db.cursor()\nsql_list = [\'Select DOC_ID, DOC_CD, DOC_DEPT_CD, DOC_CD || DOC_DEPT_CD || DOC_ID "ContractID", DOC_VERS_NO, REG_DT, DOC_PHASE_CD, DOC_STA_CD, PO_REPL_DOC_CD, PO_REPL_DEPT_CD, PO_REPL_ID FROM FMS01.PO_DOC_HDR WHERE DOC_PHASE_CD = 3 AND DOC_STA_CD = 4 AND DOC_CD || DOC_DEPT_CD || DOC_ID in %s\', \'Select DOC_ID, DOC_CD, DOC_DEPT_CD, DOC_CD || DOC_DEPT_CD || DOC_ID "ContractID", DOC_VERS_NO, REG_DT, DOC_PHASE_CD, DOC_STA_CD, PO_REPL_DOC_CD, PO_REPL_DEPT_CD, PO_REPL_ID FROM FMS01.MA_DOC_HDR WHERE DOC_PHASE_CD = 3 AND DOC_STA_CD = 4 AND DOC_CD || DOC_DEPT_CD || DOC_ID in %s\']\ncursor.execute(sql_list[0])\ndf1 = pd.DataFrame([[x for x in y] for y in cursor], columns = [\'DOC_ID\',\'DOC_CD\', \'DOC_DEPT_CD\', \'ContractID\', \'DOC_VERS_NO\', \'REG_DT\', \'DOC_PHASE_CD\', \'DOC_STA_CD\', \'PO_REPL_DOC_CD\', \'PO_REPL_DEPT_CD\', \'PO_REPL_ID\'])\ncursor.execute(sql_list[1])\ndf2 = pd.DataFrame([[x for x in y] for y in cursor], columns = [\'DOC_ID\',\'DOC_CD\', \'DOC_DEPT_CD\', \'ContractID\', \'DOC_VERS_NO\', \'REG_DT\', \'DOC_PHASE_CD\', \'DOC_STA_CD\', \'PO_REPL_DOC_CD\', \'PO_REPL_DEPT_CD\', \'PO_REPL_ID\'])\ndoc_hdr = pd.concat([df1,df2])\ndoc_hdr = doc_hdr.drop_duplicates(subset = ['ContractID'])\ndoc_hdr.to_csv('S:\Contracts\Research and IT\\\\08 - MWBE\DAS Only\\\\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\TADs Creation\orig_reg_date.txt')""" % (tup, tup)
with open(fname, 'w') as f: #w stands for writing
    f.write('{}'.format(line1)) #.format() replaces the {}

os.system('python orig_reg_date.py')

#Orig_Reg_dt.txt must be created by subsituting tuple form line 159 into C:Databases\ORIG_REG_DATE.py

orig_reg_dt = pd.read_csv(int_data_path + '\\' + r'orig_reg_date.txt')

orig_reg_dt = orig_reg_dt[['ContractID', 'REG_DT']]

join6 = join5.merge(orig_reg_dt, how = 'left', left_on = 'OrigContractID', right_on = 'ContractID')

join6 = join6[['Agency','DOC_DEPT_CD_x', 'ContractID_x', 'VendorName', 'Purpose', 'Method', 'Industry', 'RegistrationDate', 'ContractValue', 'OrigContractID', 'AwdMethodName', 'REG_DT']]

join6.to_excel('join6_tester.xlsx')

nypd_contracts = join6[join6['Agency'] == 'NYPD']['ContractID_x'].tolist()

if len(nypd_contracts)>1:
    nypd_contract = tuple(nypd_contracts)
    nypd_contract = str(nypd_contract)
    nypd_contract = nypd_contract.replace('u\'','\'')
    nypd_contract = nypd_contract.replace('\'','\\\'')
else:
    nypd_contract = tuple(nypd_contracts)
    nypd_contract = str(nypd_contract)
    nypd_contract = str(nypd_contract).replace(',','')
    nypd_contract = nypd_contract.replace('\'', '\\\'')

fname = 'nypd_budget_cd.py'
line1 = """import cx_Oracle\nimport pandas as pd\nuid = \'jlin\'\npwd = \'Purple22\'\nservice = \'cwprd1.fisalan.nycnet\'\ndb = cx_Oracle.connect(uid + \"/\" + pwd + \"@\" + service)\n\ncursor = db.cursor()\nsql_list = [\'Select DOC_CD,DOC_ID,DOC_DEPT_CD, FUNC_CD, DOC_CD || DOC_DEPT_CD || DOC_ID FROM FMS01.PO_DOC_ACTG WHERE DOC_CD || DOC_DEPT_CD || DOC_ID IN %s\']\ncursor.execute(sql_list[0])\nnypd = pd.DataFrame([[x for x in y] for y in cursor], columns = [\'DOC_CD\', \'DOC_ID\', \'DOC_DEPT_CD\', \'FUNC_CD\',\'ContractID\'])\nnypd.to_csv(\'S:\Contracts\Research and IT\\\\08 - MWBE\DAS Only\\\\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\TADs Creation\cops.txt\')""" % (nypd_contract)
with open(fname, 'w') as f: #w stands for writing
    f.write('{}'.format(line1)) #.format() replaces the {}

try:
    os.remove(int_data_path+'\\'+r'cops.txt')
except:
    pass

os.system('python nypd_budget_cd.py')

nypd = pd.read_csv(int_data_path+'\\'+'cops.txt', header = 0)
nypd = nypd[['DOC_CD', 'DOC_ID', 'DOC_DEPT_CD', 'FUNC_CD', 'ContractID']]
nypd = nypd.drop_duplicates()

# ####

df = join6

del df['DOC_DEPT_CD_x'] #vital

df.loc[:,'RegistrationDate'] = df['RegistrationDate'].dt.date
df.loc[:,'RegistrationDate'] = df['RegistrationDate'].astype(str)

df = df[df['Agency']!='OMB']

alphabet = [str(x) for x in string.ascii_uppercase]
alphabet = alphabet + ['AA', 'AB', 'AC', 'AD', 'AE','AF', 'AG','AH','AI','AJ','AK', 'AL', 'AM', 'AN','AO', 'AP']

newpath = path +'\\'+ r'FY%s_%s_%s' %(str(FY),str(FQ),str(t))
if not os.path.exists(newpath):
     os.makedirs(newpath)

if 'MWBE 72' not in df['Method'].unique():
    for x in df['Agency'].unique():
        if x != 'NYPD':
            df_Agency = df[df['Agency'] == x]
            writer = pd.ExcelWriter(newpath + '\\' + 'MWBE_Prime_FY' + str(FY)[2:4] + str(FQ) + '_TAD_' + str(x) + '.xlsx', engine='xlsxwriter')
            workbook = writer.book
            format = workbook.add_format({'bold': 1, 'text_wrap': 1, 'fg_color': '#FABF8F', 'border': 1})
            format.set_align('center')
            format.set_align('vcenter')
            format1 = workbook.add_format({'font_color': 'white'})
            format2 = workbook.add_format({'bold': 1, 'text_wrap': 1, 'fg_color': '#C5D9F1', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
            dollar_signs = workbook.add_format({'num_format': '$ ###,###,###,##0', 'align': 'center', 'valign': 'vcenter'})
            percentage_signs = workbook.add_format({'num_format': '0%', 'align': 'center', 'valign': 'vcenter'})
            gridlines = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
            center_align = workbook.add_format({'align': 'center', 'valign': 'vcenter'})
            format3 = workbook.add_format({'font_size':20, 'font_name':'Franklin Gothic Book','bold':1})
            format4 = workbook.add_format({'font_size':10, 'font_name': 'Franklin Gothic Book'})
            format4.set_italic()
            format5 = workbook.add_format({'font_size':11, 'font_name': 'Franklin Gothic Book', 'bold': 1})
            format6 = workbook.add_format({'font_size': 11, 'font_name': 'Franklin Gothic Book'})
            format7 = workbook.add_format({'font_size': 11, 'font_name': 'Calibri'})
            format7.set_italic()
            format7.set_bold()
            format8 = workbook.add_format({'font_size': 11, 'font_name': 'Calibri'})
            bottom_line = workbook.add_format()
            bottom_line.set_bottom()

            worksheet = workbook.add_worksheet('Instructions')
            worksheet.write('B2', 'Prime Contract Turn Around Document for FY ' + str(FY) + ' ' + FQ, format3)
            worksheet.write('B3', 'Source: FMS3 @' + str(datetime.datetime.today()), format4)
            worksheet.write('B4', 'Instructions', format3)
            worksheet.write('B5', 'Background', format5)
            worksheet.write('B6',
                             'This turnaround document will assist MOCS in identifying the M/WBE subcontracting goals on current contracts to prime vendors. Please follow the steps below in filling out the template.  ',
                             format6)
            worksheet.write('B7', 'Steps', format5)
            worksheet.write('B8', '1. Navigate to the \'Prime Contract Data\' sheet.', format6)
            worksheet.write('B9', '2. In Column L, please indicate whether or not the prime contract has an M/WBE goal.',
                             format6)
            worksheet.write('B10',
                             '     a. If the contract does not have an M/WBE goal, please select the best reason as listed under Column N.',
                             format6)
            worksheet.write('B11', '3. Please indicate if contract has any state or federal funding under Column M.',
                             format6)
            worksheet.write('B12',
                             '4. Please confirm Industry Assigment provided in column F. If wrong, correct under column P.',
                             format6)
            worksheet.write('B13', '5. If contract is MWBE72, please indicate whether it was federally funded under Column O.', format6)

            worksheet.write('B15', '6. Please add additional comments in the \'Agency Comments\' field if there is other information MOCS should be aware of.', format6)
            worksheet.write('B16', '7. Once completed, please email the excel file to MOCSReporting@mocs.nyc.gov.', format6)
            worksheet.conditional_format('B4:L4', {'type': 'no_errors', 'format':   bottom_line})

            df_Agency.to_excel(writer, sheet_name='Prime Contract Data', startcol=0, startrow=1, header=False, index=False)
            worksheet1 = writer.sheets['Prime Contract Data']
            worksheet1.set_row(0, 31)
            worksheet1.set_row(0, 48)
            worksheet1.set_column('L:L', 22)
            worksheet1.set_column('K:K', 13)
            worksheet1.write('A1','Agency',format2)
            worksheet1.write('B1', 'Contract ID', format2)
            worksheet1.write('C1','Vendor Name',format2)
            worksheet1.write('D1','Purpose',format2)
            worksheet1.write('E1','Method',format2)
            worksheet1.write('F1','Industry',format2)
            worksheet1.write('G1','Registration Date',format2)
            worksheet1.set_column('G:G', 19, center_align)
            worksheet1.write('H1','Contract Value',format2)
            worksheet1.write('I1','Original Contract ID',format2)
            worksheet1.write('J1','Original Method',format2)
            worksheet1.write('K1', 'Original Registration Date', format2)
            worksheet1.write('L1', 'Does this contract have an M/WBE goal?', format)
            worksheet1.data_validation('L2:L'+str(df_Agency.shape[0]+1), {'validate': 'list', 'source': ['Yes', 'No']})
            worksheet1.write('M1', 'Did this contract receive any State or Federal funding?', format)
            worksheet1.set_column('M:M', 34)
            worksheet1.data_validation('M2:M'+str(df_Agency.shape[0]+1), {'validate': 'list', 'source': ['Yes', 'No']})
            worksheet1.write('N1', 'If there were no ethnicity goals set on the contract, please select a reason.', format)
            worksheet1.set_column('N:N', 47)
            worksheet1.data_validation('N2:N'+str(df_Agency.shape[0]+1), {'validate': 'list', 'source': '=$AG$2:$AG$11'})
            worksheet1.write('O1', 'Agency Comments', format)
            worksheet1.set_column('O:O', 22)
            worksheet1.write('AG2', 'Subject to State/Federal Funding Requirements', format1)
            worksheet1.write('AG3', 'Subject to State/Federal Goals', format1)
            worksheet1.write('AG4', 'No Relevant Subcontracting Anticipated and No History of JVs', format1)
            worksheet1.write('AG5', 'Awarded vendor is not for profit', format1)
            worksheet1.write('AG6', 'Vendor Received Full Waiver', format1)
            worksheet1.write('AG7', 'Underlying Contract not subject to LL1 or LL129 (Provide details in the \'Comments\' column to the right)', format1)
            worksheet1.write('AG8', 'Procured Prior to LL129 Effective Date', format1)
            worksheet1.write('AG9', 'Standardized Services Procured Prior to 7/1/13', format1)
            worksheet1.write('AG10', 'Master Agreement; goals will be set on ensuing Task Orders', format1)
            worksheet1.write('AG11', 'Other (Provide details in the \'Comments\' column to the right)', format1)

            for x in df_Agency.columns:
                if x in ['Purpose', 'VendorName']:
                    try:
                        mx1 = max([len(a) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    worksheet1.set_column(alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[df_Agency.columns.tolist().index(x)], max(mx1, mx2) + 8, center_align)
                elif x in ['ContractValue']:
                    try:
                        mx1 = max([len(str(a)) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    mx = max(mx1, mx2) + 8
                    worksheet1.set_column(alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[df_Agency.columns.tolist().index(x)], mx, dollar_signs)
                else:
                    try:
                        mx1 = max([len(str(a)) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    mx = max(mx1, mx2) + 3
                    worksheet1.set_column(alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[df_Agency.columns.tolist().index(x)], mx, center_align)

            writer.save()
        else:
            df_Agency = df[df['Agency'] == x]
            writer = pd.ExcelWriter(newpath + '\\' +'MWBE_Prime_FY' + str(FY)[2:4] + str(FQ) + '_TAD_' + str(x) + '.xlsx', engine='xlsxwriter')
            workbook = writer.book
            format = workbook.add_format({'bold': 1, 'text_wrap': 1, 'fg_color': '#FABF8F', 'border': 1})
            format.set_align('center')
            format.set_align('vcenter')
            format1 = workbook.add_format({'font_color': 'white'})
            format2 = workbook.add_format({'bold': 1, 'text_wrap': 1, 'fg_color': '#C5D9F1', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
            dollar_signs = workbook.add_format({'num_format': '$ ###,###,###,##0', 'align': 'center', 'valign': 'vcenter'})
            percentage_signs = workbook.add_format({'num_format': '0%', 'align': 'center', 'valign': 'vcenter'})
            gridlines = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
            center_align = workbook.add_format({'align': 'center', 'valign': 'vcenter'})
            format3 = workbook.add_format({'font_size': 20, 'font_name': 'Franklin Gothic Book', 'bold': 1})
            format4 = workbook.add_format({'font_size': 10, 'font_name': 'Franklin Gothic Book'})
            format4.set_italic()
            format5 = workbook.add_format({'font_size': 11, 'font_name': 'Franklin Gothic Book', 'bold': 1})
            format6 = workbook.add_format({'font_size': 11, 'font_name': 'Franklin Gothic Book'})
            format7 = workbook.add_format({'font_size': 11, 'font_name': 'Calibri'})
            format7.set_italic()
            format7.set_bold()
            format8 = workbook.add_format({'font_size': 11, 'font_name': 'Calibri'})
            bottom_line = workbook.add_format()
            bottom_line.set_bottom()

            worksheet = workbook.add_worksheet('Instructions')
            worksheet.write('B2', 'Prime Contract Turn Around Document for FY ' + str(FY) + ' ' + FQ, format3)
            worksheet.write('B3', 'Source: FMS3 @' + str(datetime.datetime.today()), format4)
            worksheet.write('B4', 'Instructions', format3)
            worksheet.write('B5', 'Background', format5)
            worksheet.write('B6',
                            'This turnaround document will assist MOCS in identifying the M/WBE subcontracting goals on current contracts to prime vendors. Please follow the steps below in filling out the template.  ',
                            format6)
            worksheet.write('B7', 'Steps', format5)
            worksheet.write('B8', '1. Navigate to the \'Prime Contract Data\' sheet.', format6)
            worksheet.write('B9',
                            '2. In Column L, please indicate whether or not the prime contract has an M/WBE goal.',
                            format6)
            worksheet.write('B10',
                            '     a. If the contract does not have an M/WBE goal, please select the best reason as listed under Column N.',
                            format6)
            worksheet.write('B11', '3. Please indicate if contract has any state or federal funding under Column M.',
                            format6)
            worksheet.write('B12',
                            '4. Please confirm Industry Assigment provided in column F. If wrong, correct under column P.',
                            format6)
            worksheet.write('B13',
                            '5. If contract is MWBE72, please indicate whether it was federally funded under Column O.',
                            format6)

            worksheet.write('B15',
                            '6. Please add additional comments in the \'Agency Comments\' field if there is other information MOCS should be aware of.',
                            format6)
            worksheet.write('B16', '7. Once completed, please email the excel file to MOCSReporting@mocs.nyc.gov.',
                            format6)
            worksheet.conditional_format('B4:L4', {'type': 'no_errors', 'format': bottom_line})

            df_Agency.to_excel(writer, sheet_name='Prime Contract Data', startcol=0, startrow=1, header=False, index=False)
            worksheet1 = writer.sheets['Prime Contract Data']
            worksheet1.set_row(0, 31)
            worksheet1.set_row(0, 48)
            worksheet1.set_column('L:L', 22)
            worksheet1.set_column('K:K', 13)
            worksheet1.write('A1', 'Agency', format2)
            worksheet1.write('B1', 'Contract ID', format2)
            worksheet1.write('C1', 'Vendor Name', format2)
            worksheet1.write('D1', 'Purpose', format2)
            worksheet1.write('E1', 'Method', format2)
            worksheet1.write('F1', 'Industry', format2)
            worksheet1.write('G1', 'Registration Date', format2)
            worksheet1.set_column('G:G', 19, center_align)
            worksheet1.write('H1', 'Contract Value', format2)
            worksheet1.write('I1', 'Original Contract ID', format2)
            worksheet1.write('J1', 'Original Method', format2)
            worksheet1.write('K1', 'Original Registration Date', format2)
            worksheet1.write('L1', 'Does this contract have an M/WBE goal?', format)
            worksheet1.data_validation('L2:L' + str(df_Agency.shape[0]+1), {'validate': 'list', 'source': ['Yes', 'No']})
            worksheet1.write('M1', 'Did this contract receive any State or Federal funding?', format)
            worksheet1.set_column('M:M', 34)
            worksheet1.data_validation('M2:M' + str(df_Agency.shape[0]+1), {'validate': 'list', 'source': ['Yes', 'No']})
            worksheet1.write('N1', 'If there were no ethnicity goals set on the contract, please select a reason.', format)
            worksheet1.set_column('N:N', 47)
            worksheet1.data_validation('N2:N' + str(df_Agency.shape[0]+1), {'validate': 'list', 'source': '=$AG$2:$AG$11'})
            worksheet1.write('O1', 'Agency Comments', format)
            worksheet1.set_column('O:O', 22)
            worksheet1.write('AG2', 'Subject to State/Federal Funding Requirements', format1)
            worksheet1.write('AG3', 'Subject to State/Federal Goals', format1)
            worksheet1.write('AG4', 'No Relevant Subcontracting Anticipated and No History of JVs', format1)
            worksheet1.write('AG5', 'Awarded vendor is not for profit', format1)
            worksheet1.write('AG6', 'Vendor Received Full Waiver', format1)
            worksheet1.write('AG7', 'Underlying Contract not subject to LL1 or LL129 (Provide details in the \'Comments\' column to the right)', format1)
            worksheet1.write('AG8', 'Procured Prior to LL129 Effective Date', format1)
            worksheet1.write('AG9', 'Standardized Services Procured Prior to 7/1/13', format1)
            worksheet1.write('AG10', 'Master Agreement; goals will be set on ensuing Task Orders', format1)
            worksheet1.write('AG11', 'Other (Provide details in the \'Comments\' column to the right)', format1)
            worksheet1.set_column('G:G', 19, center_align)

            for x in df_Agency.columns:
                if x in ['Purpose', 'VendorName']:
                    try:
                        mx1 = max([len(a) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    worksheet1.set_column(
                        alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[df_Agency.columns.tolist().index(x)],
                        max(mx1, mx2) + 8, center_align)
                elif x in ['ContractValue']:
                    try:
                        mx1 = max([len(str(a)) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    mx = max(mx1, mx2) + 8
                    worksheet1.set_column(
                        alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[df_Agency.columns.tolist().index(x)],
                        mx, dollar_signs)
                else:
                    try:
                        mx1 = max([len(str(a)) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    mx = max(mx1, mx2) + 3
                    worksheet1.set_column(
                        alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[df_Agency.columns.tolist().index(x)],
                        mx, center_align)

            nypd.to_excel(writer, sheet_name='Budget Codes', startcol=0, startrow=1, header=True, index=False)
            worksheet2 = writer.sheets['Budget Codes']
            worksheet2.set_column('A:A', 11)
            worksheet2.set_column('B:B', 16)
            worksheet2.set_column('C:C', 17)
            worksheet2.set_column('D:D', 16)
            worksheet2.set_column('E:E', 23)

            writer.save()
else:
    for x in df['Agency'].unique():
        if x != 'NYPD':
            df_Agency = df[df['Agency'] == x]
            writer = pd.ExcelWriter(newpath + '\\' + 'MWBE_Prime_FY' + str(FY)[2:4] + str(FQ) + '_TAD_' + str(x) + '.xlsx', engine='xlsxwriter')
            workbook = writer.book
            format = workbook.add_format({'bold': 1, 'text_wrap': 1, 'fg_color': '#FABF8F', 'border': 1})
            format.set_align('center')
            format.set_align('vcenter')
            format1 = workbook.add_format({'font_color': 'white'})
            format2 = workbook.add_format(
                {'bold': 1, 'text_wrap': 1, 'fg_color': '#C5D9F1', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
            dollar_signs = workbook.add_format(
                {'num_format': '$ ###,###,###,##0', 'align': 'center', 'valign': 'vcenter'})
            percentage_signs = workbook.add_format({'num_format': '0%', 'align': 'center', 'valign': 'vcenter'})
            gridlines = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
            center_align = workbook.add_format({'align': 'center', 'valign': 'vcenter'})
            format3 = workbook.add_format({'font_size': 20, 'font_name': 'Franklin Gothic Book', 'bold': 1})
            format4 = workbook.add_format({'font_size': 10, 'font_name': 'Franklin Gothic Book'})
            format4.set_italic()
            format5 = workbook.add_format({'font_size': 11, 'font_name': 'Franklin Gothic Book', 'bold': 1})
            format6 = workbook.add_format({'font_size': 11, 'font_name': 'Franklin Gothic Book'})
            format7 = workbook.add_format({'font_size': 11, 'font_name': 'Calibri'})
            format7.set_italic()
            format7.set_bold()
            format8 = workbook.add_format({'font_size': 11, 'font_name': 'Calibri'})
            bottom_line = workbook.add_format()
            bottom_line.set_bottom()

            worksheet = workbook.add_worksheet('Instructions')
            worksheet.write('B2', 'Prime Contract Turn Around Document for FY ' + str(FY) + ' ' + FQ, format3)
            worksheet.write('B3', 'Source: FMS3 @' + str(datetime.datetime.today()), format4)
            worksheet.write('B4', 'Instructions', format3)
            worksheet.write('B5', 'Background', format5)
            worksheet.write('B6',
                            'This turnaround document will assist MOCS in identifying the M/WBE subcontracting goals on current contracts to prime vendors. Please follow the steps below in filling out the template.  ',
                            format6)
            worksheet.write('B7', 'Steps', format5)
            worksheet.write('B8', '1. Navigate to the \'Prime Contract Data\' sheet.', format6)
            worksheet.write('B9',
                            '2. In Column L, please indicate whether or not the prime contract has an M/WBE goal.',
                            format6)
            worksheet.write('B10',
                            '     a. If the contract does not have an M/WBE goal, please select the best reason as listed under Column N.',
                            format6)
            worksheet.write('B11', '3. Please indicate if contract has any state or federal funding under Column M.',
                            format6)
            worksheet.write('B12',
                            '4. Please confirm Industry Assigment provided in column F. If wrong, correct under column P.',
                            format6)
            worksheet.write('B13',
                            '5. If contract is MWBE72, please indicate whether it was federally funded under Column O.',
                            format6)

            worksheet.write('B15',
                            '6. Please add additional comments in the \'Agency Comments\' field if there is other information MOCS should be aware of.',
                            format6)
            worksheet.write('B16', '7. Once completed, please email the excel file to MOCSReporting@mocs.nyc.gov.',
                            format6)
            worksheet.conditional_format('B4:L4', {'type': 'no_errors', 'format': bottom_line})

            df_Agency.to_excel(writer, sheet_name='Prime Contract Data', startcol=0, startrow=1, header=False,
                               index=False)
            worksheet1 = writer.sheets['Prime Contract Data']
            worksheet1.set_row(0, 31)
            worksheet1.set_row(0, 48)
            worksheet1.set_column('L:L', 22)
            worksheet1.set_column('K:K', 13)
            worksheet1.write('A1', 'Agency', format2)
            worksheet1.write('B1', 'Contract ID', format2)
            worksheet1.write('C1', 'Vendor Name', format2)
            worksheet1.write('D1', 'Purpose', format2)
            worksheet1.write('E1', 'Method', format2)
            worksheet1.write('F1', 'Industry', format2)
            worksheet1.write('G1', 'Registration Date', format2)
            worksheet1.set_column('G:G', 19, center_align)
            worksheet1.write('H1', 'Contract Value', format2)
            worksheet1.write('I1', 'Original Contract ID', format2)
            worksheet1.write('J1', 'Original Method', format2)
            worksheet1.write('K1', 'Original Registration Date', format2)
            worksheet1.write('L1', 'Does this contract have an M/WBE goal?', format)
            worksheet1.data_validation('L2:L' + str(df_Agency.shape[0] + 1),
                                       {'validate': 'list', 'source': ['Yes', 'No']})
            worksheet1.write('M1', 'Did this contract receive any State or Federal funding?', format)
            worksheet1.set_column('M:M', 34)
            worksheet1.data_validation('M2:M' + str(df_Agency.shape[0] + 1),
                                       {'validate': 'list', 'source': ['Yes', 'No']})
            worksheet1.write('N1', 'If there were no ethnicity goals set on the contract, please select a reason.',
                             format)
            worksheet1.set_column('N:N', 47)
            worksheet1.data_validation('N2:N' + str(df_Agency.shape[0] + 1),
                                       {'validate': 'list', 'source': '=$AG$2:$AG$11'})
            worksheet1.write('O1', 'If contract method is MWBE 72, did the contract receive Federal funding?', format)
            worksheet1.set_column('O:O', 37)
            worksheet1.data_validation('O2:O' + str(df_Agency.shape[0] + 1), {'validate': 'list', 'source': ['Yes', 'No']})
            worksheet1.write('P1', 'Agency Comments', format)
            worksheet1.set_column('P:P', 22)
            worksheet1.write('AG2', 'Subject to State/Federal Funding Requirements', format1)
            worksheet1.write('AG3', 'Subject to State/Federal Goals', format1)
            worksheet1.write('AG4', 'No Relevant Subcontracting Anticipated and No History of JVs', format1)
            worksheet1.write('AG5', 'Awarded vendor is not for profit', format1)
            worksheet1.write('AG6', 'Vendor Received Full Waiver', format1)
            worksheet1.write('AG7',
                             'Underlying Contract not subject to LL1 or LL129 (Provide details in the \'Comments\' column to the right)',
                             format1)
            worksheet1.write('AG8', 'Procured Prior to LL129 Effective Date', format1)
            worksheet1.write('AG9', 'Standardized Services Procured Prior to 7/1/13', format1)
            worksheet1.write('AG10', 'Master Agreement; goals will be set on ensuing Task Orders', format1)
            worksheet1.write('AG11', 'Other (Provide details in the \'Comments\' column to the right)', format1)

            for x in df_Agency.columns:
                if x in ['Purpose', 'VendorName']:
                    try:
                        mx1 = max([len(a) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    worksheet1.set_column(alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[
                        df_Agency.columns.tolist().index(x)], max(mx1, mx2) + 8, center_align)
                elif x in ['ContractValue']:
                    try:
                        mx1 = max([len(str(a)) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    mx = max(mx1, mx2) + 8
                    worksheet1.set_column(alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[
                        df_Agency.columns.tolist().index(x)], mx, dollar_signs)
                else:
                    try:
                        mx1 = max([len(str(a)) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    mx = max(mx1, mx2) + 3
                    worksheet1.set_column(alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[
                        df_Agency.columns.tolist().index(x)], mx, center_align)

            writer.save()
        else:
            df_Agency = df[df['Agency'] == x]
            writer = pd.ExcelWriter(
                newpath + '\\' + 'MWBE_Prime_FY' + str(FY)[2:4] + str(FQ) + '_TAD_' + str(x) + '.xlsx',
                engine='xlsxwriter')
            workbook = writer.book
            format = workbook.add_format({'bold': 1, 'text_wrap': 1, 'fg_color': '#FABF8F', 'border': 1})
            format.set_align('center')
            format.set_align('vcenter')
            format1 = workbook.add_format({'font_color': 'white'})
            format2 = workbook.add_format(
                {'bold': 1, 'text_wrap': 1, 'fg_color': '#C5D9F1', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
            dollar_signs = workbook.add_format(
                {'num_format': '$ ###,###,###,##0', 'align': 'center', 'valign': 'vcenter'})
            percentage_signs = workbook.add_format({'num_format': '0%', 'align': 'center', 'valign': 'vcenter'})
            gridlines = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
            center_align = workbook.add_format({'align': 'center', 'valign': 'vcenter'})
            format3 = workbook.add_format({'font_size': 20, 'font_name': 'Franklin Gothic Book', 'bold': 1})
            format4 = workbook.add_format({'font_size': 10, 'font_name': 'Franklin Gothic Book'})
            format4.set_italic()
            format5 = workbook.add_format({'font_size': 11, 'font_name': 'Franklin Gothic Book', 'bold': 1})
            format6 = workbook.add_format({'font_size': 11, 'font_name': 'Franklin Gothic Book'})
            format7 = workbook.add_format({'font_size': 11, 'font_name': 'Calibri'})
            format7.set_italic()
            format7.set_bold()
            format8 = workbook.add_format({'font_size': 11, 'font_name': 'Calibri'})
            bottom_line = workbook.add_format()
            bottom_line.set_bottom()

            worksheet = workbook.add_worksheet('Instructions')
            worksheet.write('B2', 'Prime Contract Turn Around Document for FY ' + str(FY) + ' ' + FQ, format3)
            worksheet.write('B3', 'Source: FMS3 @' + str(datetime.datetime.today()), format4)
            worksheet.write('B4', 'Instructions', format3)
            worksheet.write('B5', 'Background', format5)
            worksheet.write('B6',
                            'This turnaround document will assist MOCS in identifying the M/WBE subcontracting goals on current contracts to prime vendors. Please follow the steps below in filling out the template.  ',
                            format6)
            worksheet.write('B7', 'Steps', format5)
            worksheet.write('B8', '1. Navigate to the \'Prime Contract Data\' sheet.', format6)
            worksheet.write('B9',
                            '2. In Column L, please indicate whether or not the prime contract has an M/WBE goal.',
                            format6)
            worksheet.write('B10',
                            '     a. If the contract does not have an M/WBE goal, please select the best reason as listed under Column N.',
                            format6)
            worksheet.write('B11', '3. Please indicate if contract has any state or federal funding under Column M.',
                            format6)
            worksheet.write('B12',
                            '4. Please confirm Industry Assigment provided in column F. If wrong, correct under column P.',
                            format6)
            worksheet.write('B13',
                            '5. If contract is MWBE72, please indicate whether it was federally funded under Column O.',
                            format6)

            worksheet.write('B15',
                            '6. Please add additional comments in the \'Agency Comments\' field if there is other information MOCS should be aware of.',
                            format6)
            worksheet.write('B16', '7. Once completed, please email the excel file to MOCSReporting@mocs.nyc.gov.',
                            format6)
            worksheet.conditional_format('B4:L4', {'type': 'no_errors', 'format': bottom_line})

            df_Agency.to_excel(writer, sheet_name='Prime Contract Data', startcol=0, startrow=1, header=False,
                               index=False)
            worksheet1 = writer.sheets['Prime Contract Data']
            worksheet1.set_row(0, 31)
            worksheet1.set_row(0, 48)
            worksheet1.set_column('L:L', 22)
            worksheet1.set_column('K:K', 13)
            worksheet1.write('A1', 'Agency', format2)
            worksheet1.write('B1', 'Contract ID', format2)
            worksheet1.write('C1', 'Vendor Name', format2)
            worksheet1.write('D1', 'Purpose', format2)
            worksheet1.write('E1', 'Method', format2)
            worksheet1.write('F1', 'Industry', format2)
            worksheet1.write('G1', 'Registration Date', format2)
            worksheet1.set_column('G:G', 19, center_align)
            worksheet1.write('H1', 'Contract Value', format2)
            worksheet1.write('I1', 'Original Contract ID', format2)
            worksheet1.write('J1', 'Original Method', format2)
            worksheet1.write('K1', 'Original Registration Date', format2)
            worksheet1.write('L1', 'Does this contract have an M/WBE goal?', format)
            worksheet1.data_validation('L2:L' + str(df_Agency.shape[0] + 1),
                                       {'validate': 'list', 'source': ['Yes', 'No']})
            worksheet1.write('M1', 'Did this contract receive any State or Federal funding?', format)
            worksheet1.set_column('M:M', 34)
            worksheet1.data_validation('M2:M' + str(df_Agency.shape[0] + 1),
                                       {'validate': 'list', 'source': ['Yes', 'No']})
            worksheet1.write('N1', 'If there were no ethnicity goals set on the contract, please select a reason.',
                             format)
            worksheet1.set_column('N:N', 47)
            worksheet1.data_validation('N2:N' + str(df_Agency.shape[0] + 1),
                                       {'validate': 'list', 'source': '=$AG$2:$AG$11'})
            worksheet1.write('O1', 'If contract method is MWBE 72, did the contract receive Federal funding?', format)
            worksheet1.set_column('O:O', 34)
            worksheet1.data_validation('O2:O' + str(df_Agency.shape[0] + 1),
                                       {'validate': 'list', 'source': ['Yes', 'No']})
            worksheet1.write('P1', 'Agency Comments', format)
            worksheet1.set_column('P:P', 22)
            worksheet1.write('AG2', 'Subject to State/Federal Funding Requirements', format1)
            worksheet1.write('AG3', 'Subject to State/Federal Goals', format1)
            worksheet1.write('AG4', 'No Relevant Subcontracting Anticipated and No History of JVs', format1)
            worksheet1.write('AG5', 'Awarded vendor is not for profit', format1)
            worksheet1.write('AG6', 'Vendor Received Full Waiver', format1)
            worksheet1.write('AG7',
                             'Underlying Contract not subject to LL1 or LL129 (Provide details in the \'Comments\' column to the right)',
                             format1)
            worksheet1.write('AG8', 'Procured Prior to LL129 Effective Date', format1)
            worksheet1.write('AG9', 'Standardized Services Procured Prior to 7/1/13', format1)
            worksheet1.write('AG10', 'Master Agreement; goals will be set on ensuing Task Orders', format1)
            worksheet1.write('AG11', 'Other (Provide details in the \'Comments\' column to the right)', format1)
            worksheet1.set_column('G:G', 19, center_align)

            for x in df_Agency.columns:
                if x in ['Purpose', 'VendorName']:
                    try:
                        mx1 = max([len(a) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    worksheet1.set_column(
                        alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[
                            df_Agency.columns.tolist().index(x)],
                        max(mx1, mx2) + 8, center_align)
                elif x in ['ContractValue']:
                    try:
                        mx1 = max([len(str(a)) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    mx = max(mx1, mx2) + 8
                    worksheet1.set_column(
                        alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[
                            df_Agency.columns.tolist().index(x)],
                        mx, dollar_signs)
                else:
                    try:
                        mx1 = max([len(str(a)) for a in df_Agency[x]])
                    except (TypeError, ValueError, IndexError):
                        mx1 = len(x)
                    mx2 = len(x)
                    mx = max(mx1, mx2) + 3
                    worksheet1.set_column(
                        alphabet[df_Agency.columns.tolist().index(x)] + ':' + alphabet[
                            df_Agency.columns.tolist().index(x)],
                        mx, center_align)

            nypd.to_excel(writer, sheet_name='Budget Codes', startcol=0, startrow=1, header=True, index=False)
            worksheet2 = writer.sheets['Budget Codes']
            worksheet2.set_column('A:A', 11)
            worksheet2.set_column('B:B', 16)
            worksheet2.set_column('C:C', 17)
            worksheet2.set_column('D:D', 16)
            worksheet2.set_column('E:E', 23)

            writer.save()
