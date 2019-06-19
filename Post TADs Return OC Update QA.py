import os
import pandas as pd
import pyodbc
import xlsxwriter
import datetime
import numpy as np

#This Script Checks both the MWBE Database And Open Contracts for the MWBE_LL and Fed_Fund Columns. Specifically it sees if FED_FUND has been updated.
#Change today variable to any point in Current Fiscal Year
#Will Return Error Msg If Update Has Not Taken Place or Has Not Worked.

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
        FY = today.year
        FQ = 'Q2'
    elif today.month >= 4 and today.month <= 6:
        date_range_start = datetime.date(today.year - 1, 7, 1) #Third Quarter
        date_range_end = datetime.date(today.year, 3, 31)
        FY = today.year
        FQ = 'Q3'
    return [date_range_start, date_range_end, FQ, FY]

[date_range_start, date_range_end, FQ, FY] = date_range(today)

#Date Range should not be cumulative here. Per FQ.
start_date = str(date_range_end.month-2) + '/' + str(date_range_start.day) + '/' + str(date_range_end.year)
if date_range_end.month in [1,3,5,7,8,10,12]:
    end_date = str(date_range_end.month) + '/' + str(max(date_range_end.day, 31)) + '/' + str(date_range_end.year)
else:
    end_date = str(date_range_end.month) + '/' + str(min(date_range_end.day, 30)) + '/' + str(date_range_end.year)

#TADs

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Outbound TADs OC Update'

tads = pd.read_csv(path +'\\' + r'ConsolidatedTADs%s_%s.txt' % (str(FY),FQ),header = 0)

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Outbound TADs OC Update'

NTR = pd.read_excel(path +'\\'+'NoTSPReason.xlsx', header = 0)

tads['No Ethnicity Goal'] = tads['No Ethnicity Goal'].astype(str)

tads['No Ethnicity Goal'] = [str(x).lower() for x in tads['No Ethnicity Goal']]

def tspid(row):
    row_label = 'No Ethnicity Goal'
    if row[row_label] == 'subject to state/federal goals':
        return 1
    elif row[row_label] == 'no relevant subcontracting anticipated and no history of jvs':
        return 2
    elif row[row_label] == 'procured prior to ll129 effective date':
        return 3
    elif row[row_label] == 'vendor received full waiver':
        return 4
    elif row[row_label] == 'other (provide details in the \'comments\' column to the right)':
        return 5
    elif row[row_label] == 'awarded vendor is not for profit':
        return 6
    elif row[row_label] == 'standardized services procured prior to 7/1/13':
        return 7
    elif row[row_label] == 'underlying contract not subject to ll1 or ll129 (provide details in the \'comments\' column to the right)':
        return 8
    elif row[row_label] == 'master agreement; goals will be set on ensuing task orders':
        return 10
    elif row[row_label] == 'subject to state/federal funding requirements':
        return 11
    elif row[row_label] == 'subject to state/federal funding requirements which preclude city from setting city mwbe goals':
        return 12

tads['NoTSPReasonID'] = tads.apply(tspid, axis = 1)

#Checks in MWBE Database

Access_Path = r'S:\Contracts\Research and IT\Procurement Indicators\FY %s Procurement Indicators\Indicators%s_OpenContracts.accdb;' % (str(FY), str(FY))
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

#Checking MWBE Goals, Fed Funds and MWBE 72 in Open Contracts DB
crsr.execute("""SELECT tblMainTable_OpenContracts.MWBE_GOALS, tblMainTable_OpenContracts.STATE_FED_FUNDED, tblMainTable_OpenContracts.MWBE72Fed, tblMainTable_OpenContracts.RegistrationDate, tblMainTable_OpenContracts.ContractID FROM tblMainTable_OpenContracts WHERE tblMainTable_OpenContracts.RegistrationDate BETWEEN #%s# AND #%s#""" % (start_date, end_date))

df = pd.DataFrame([tuple(x) for x in crsr.fetchall()], columns = ['MWBE_GOALS', 'STATE_FED_FUNDED', 'MWBE72Fed', 'RegistrationDate', 'ContractID'])

if df['MWBE_GOALS'].sum() == 0: #Should not be zero
      print('MWBE_Goals Update Did Not Work or Has Not Taken Place Yet')
if df[u'STATE_FED_FUNDED'].sum() == 0:
      print('STATE_FED_FUNDED Update Did Not Work or Has Not Taken Place Yet')
if df['MWBE72Fed'].sum() == int(tads[tads['MWBE72Fed'] == 'Yes'].shape[0]): #MWBEFed vs TADs Column
     pass
else:
     print (df['MWBE72Fed'].sum())
     print (-1*int(tads[tads['MWBE72Fed'] == 'Yes'].shape[0]))
     print ('MWBE_FED_FUNDED Update Did Not Work or Did Not Take Place')

crsr.execute("""SELECT tblMainTable_OpenContracts.ContractID, tblMainTable_OpenContracts.NoTSPReason, tblMainTable_OpenContracts.RegistrationDate FROM tblMainTable_OpenContracts WHERE tblMainTable_OpenContracts.RegistrationDate BETWEEN #%s# AND #%s#""" % (start_date, end_date))

df = pd.DataFrame([tuple(x) for x in crsr.fetchall()], columns = ['ContractID','NoTSPReason','RegistrationDate'])

df = df[df['ContractID'].isin(tads['Contract ID'])]

tsp1 = df.groupby(['NoTSPReason'])['ContractID'].count().reset_index()

######

#QA Process for Second Database

Access_Path = r'\\csc.nycnet\mocs\mocs_user_share\Contracts\Research and IT\Procurement Indicators\FY %s Procurement Indicators\MWBE\Working.accdb' % (str(FY))
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

#Checking MWBE Goals, Fed Funds and MWBE 72 in linked OC in Working DB
crsr.execute("""SELECT tblMainTable_OpenContracts.MWBE_GOALS, tblMainTable_OpenContracts.STATE_FED_FUNDED, tblMainTable_OpenContracts.MWBE72Fed, tblMainTable_OpenContracts.RegistrationDate, tblMainTable_OpenContracts.ContractID FROM tblMainTable_OpenContracts WHERE tblMainTable_OpenContracts.RegistrationDate BETWEEN #%s# AND #%s#""" % (start_date, end_date))

df = pd.DataFrame([tuple(x) for x in crsr.fetchall()], columns = ['MWBE_GOALS', 'STATE_FED_FUNDED', 'MWBE72Fed', 'RegistrationDate', 'ContractID'])

df = df[df['ContractID'].isin(tads['Contract ID'])]

if df.iloc[:,0].sum() == 0:
    print('MWBE_Goals Update Did Not Work or Has Not Taken Place Yet')
elif df.iloc[:,1].sum() == 0:
    print('STATE_FED_FUNDED Update Did Not Work or Has Not Taken Place Yet')
elif df['MWBE72Fed'].sum() == int(tads[tads['MWBE72Fed'] == 'Yes'].shape[0]): #MWBEFed vs TADs Column
     pass
else:
     print (df.iloc[:,2].sum() == -1*int(tads[tads['MWBE72Fed'] == 'Yes'].shape[0]))
     print (-1*int(tads['MWBE72Fed'].sum()))
     print('MWBE_FED_FUNDED Update Did Not Work or Has Not Taken Place Yet')

#Checking No TSP Reason
crsr.execute("""SELECT tblMainTable_OpenContracts.ContractID,tblMainTable_OpenContracts.NoTSPReason, tblMainTable_OpenContracts.RegistrationDate FROM tblMainTable_OpenContracts WHERE tblMainTable_OpenContracts.RegistrationDate BETWEEN #%s# AND #%s#""" % (start_date, end_date))

df = pd.DataFrame([tuple(x) for x in crsr.fetchall()], columns = ['ContractID','NoTSPReason','RegistrationDate'])

df = df[df['ContractID'].isin(tads['Contract ID'])]

tsp2 = df.groupby(['NoTSPReason'])['ContractID'].count().reset_index()

if (tsp1['ContractID'] - tsp2['ContractID']).sum() == 0:
    print ('TSP Reason Consistent')
    if len(set(tsp2['NoTSPReason']) - set(tads['NoTSPReasonID'].dropna())) == 0:
        print ('Access / Excel TADs Consistent')
else:
    print ('ERROR: TSP Reasons Does Not Match')
    print (tsp1['ContractID'])
    print (tsp2['ContractID'])

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Outbound TADs OC Update'

file = r'TADs_UpDate_QA.xlsx'

df = pd.read_excel(path +'\\'+ file, header = 0)

df.loc[df.shape[0]] = [today, 'Yes', 'Yes','Yes', 'Yes', 'Yes']

writer = pd.ExcelWriter(path + '\\' + file, engine='xlsxwriter')

df.to_excel(writer, sheet_name = 'TADs Update QA', index = False)

worksheet = writer.sheets['TADs Update QA']

workbook = writer.book

center = workbook.add_format({'align': 'center', 'valign': 'vcenter'})

worksheet.set_column('A:A', 21, center)
worksheet.set_column('B:B', 66, center)
worksheet.set_column('C:C', 35, center)
worksheet.set_column('D:D', 23, center)
worksheet.set_column('E:E', 24, center)
worksheet.set_column('F:F', 26, center)

writer.save()



