import os
import pandas as pd
import pyodbc
import xlsxwriter
import datetime
import numpy as np

#Creates PrimeTADs Table in Access. Prerequisite step to implementing qryUpdateTAD_FXX_FQ_MWBE_LL, qryUpdateTAD_FYXX_FQ_02_TSP, and qryUpdateTAD_FYXX_FQ_03_Goals

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

start_date = str(date_range_start.month) + '/' + str(date_range_start.day) + '/' + str(date_range_start.year)
end_date = str(date_range_end.month) + '/' + str(date_range_end.day) + '/' + str(date_range_end.year)

print start_date
print end_date

# # There are 2 Possible Scripts for the DOHMH\OCME Consolidated TAD
# # execfile(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\LL1 TADs Consolidation and DB Update\LL1 DOHMH TAD Consolidation.py')

filepath = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\04 - TAD\LL1 Primes\FY%s\%s\Received\FED FUND' %(str(FY)[2:4],FQ)

list = os.listdir(filepath)

try:
    list.remove('OCME.DOHMH')
except:
    pass

if 'Thumbs.db' in list:
      list.remove('Thumbs.db')

list.remove('old TAD')

dataframe_list = [pd.read_excel(filepath + '\\' + x, sheetname = 'Prime Contract Data') for x in list]

df = pd.concat(dataframe_list) #concat everything in folder

orig = df

def check(x,y):
    if x == 'MWBE 72' and y in ('Yes'):
        return 'All Good'
    elif x != 'MWBE 72' and y in ['Yes']: #Inappropriately Answered. Originally not MWBE contracts, agency filled out wrong col.
        return 'Bad'
    else:
        return 'Empty'

check = np.vectorize(check)

df['Check_Col']= check(df['Method'],df['If contract method is MWBE 72, did the contract receive Federal funding?'].astype(str))

orig1 = orig[['Agency','Contract ID','Method','If contract method is MWBE 72, did the contract receive Federal funding?', 'Check_Col']]

for x in orig1[orig1['Check_Col'] == 'Bad'].index: #Fixes the wrong answer to correct.
    for y in [orig1[orig1['Check_Col'] == 'Bad'].columns[3]]:
        # print x,y
        print x
        print y
        df.set_value(x,y,'No')

df['Check_Col'] = check(df['Method'], df['If contract method is MWBE 72, did the contract receive Federal funding?'].astype(str))

if 'Bad' not in df['Check_Col'].unique():

    agency_list = [x for x in df['Agency'].unique().tolist() if pd.isnull(x) == False] #why do this? probably because I saw dirty data

    df1 = df[df['Agency'].isin(agency_list)]

    df1 = df1[[u'Agency', u'Contract ID', u'Vendor Name', u'Purpose', u'Method', u'Industry', u'Registration Date', u'Contract Value', u'Original Contract ID', u'Original Method', u'Original Registration Date', u'Does this contract have an M/WBE goal?', u'Did this contract receive any State or Federal funding?', u'If there were no ethnicity goals set on the contract, please select a reason.',u'If contract method is MWBE 72, did the contract receive Federal funding?',u'Agency Comments']]
    df1.columns = [u'Agency', u'Contract ID', u'Vendor Name', u'Purpose', u'Method', u'Industry', u'Registration Date', u'Contract Value', u'Original Contract ID', u'Original Method', u'Original Registration Date', u'MWBE Goal', u'State Fed', u'No Ethnicity Goal', u'MWBE72_FED_FUND',u'Agency Comments']

    df1 = df1.fillna('')
    df1['Vendor Name'] = df1['Vendor Name'].str.replace('\'','')
    df1['Purpose'] = df1['Purpose'].str.replace('\'','')
    df1['Agency Comments'] = df1['Agency Comments'].str.replace('\'','')
    df1['No Ethnicity Goal'] = df1['No Ethnicity Goal'].str.replace('\'','')
    df1['Original Registration Date'] = df1['Original Registration Date'].str.replace('Timestamp\(','')
    df1['Original Registration Date'] = df1['Original Registration Date'].str.replace(')','')
    df1['Registration Date'] = df1['Registration Date'].astype(str)
    df1['Registration Date'] = df1['Registration Date'].str.replace('Timestamp\(','')
    df1['State Fed'] = df1['State Fed'].fillna('No') #If they leave blank assume  no.
    df1['State Fed'] = df1['State Fed'].replace('','No')

    def MWBE_GOALS(x): #df['MWBE Goal']
        if x == 'Yes':
            return '-1'
        elif x == 'No':
            return '0'
        elif x == '': #If its not filled in, assume No.
            return '0'

    MWBE_GOALS = np.vectorize(MWBE_GOALS)

    df1['MWBE_GOALS']= MWBE_GOALS(df1['MWBE Goal'])

    def fed_fund(x): #row['State Fed']
        if x == 'Yes':
            return '-1'
        elif x == 'No':
            return '0'

    fed_fund = np.vectorize(fed_fund)

    df1['STATE_FED_FUNDED'] = fed_fund(df1['State Fed'])
    df1['STATE_FED_FUNDED'] = df1['STATE_FED_FUNDED'].str.replace('None','0')

    def mwbe72(x): #row['State Fed']
        if x == 'Yes':
            return '-1'
        else:
            return '0'

    mwbe72 = np.vectorize(mwbe72)

    df1['MWBE72_FED_FUND'] = mwbe72(df1['MWBE72_FED_FUND'])
    df1['MWBE72_FED_FUND'] = df1['MWBE72_FED_FUND'].str.replace('None','0')
    df1['STATE_FED_FUNDED']= df1['STATE_FED_FUNDED'].fillna('0')
    df1['MWBE_GOALS']= df1['MWBE_GOALS'].fillna('0')
    df1['MWBE_GOALS']= df1['MWBE_GOALS'].str.replace('None','0') #Artifact of vectorization process.
    df1['State Fed'] = df1['State Fed'].replace('None','0')
    df1['MWBE72_FED_FUND'] = df1['MWBE72_FED_FUND'].fillna('0')
    df1['MWBE72_FED_FUND'] = df1['MWBE72_FED_FUND'].replace('None', '0')
    df1 = df1.fillna('')
    df1[u'Contract Value'] = df1[u'Contract Value'].astype(str)

    def trans(row):
        if row[u'No Ethnicity Goal'] == u'Underlying Contract not subject to LL1 or LL129 (Provide details in the Comments column to the right)':
            return 'Underlying Contract not subject to LL1 or LL129'
        else:
            return row[u'No Ethnicity Goal']

    df1.loc[:, u'No Ethnicity Goal']= df1.apply(trans, axis = 1)

    df1[u'Original Contract ID'] = df1[u'Original Contract ID'].astype(str)

    df1['Contract Value'] = df1['Contract Value'].astype(float)

    df1 = df1[[u'Agency', u'Contract ID', u'Vendor Name', u'Purpose', u'Method', u'Industry', u'Registration Date', u'Contract Value', u'Original Contract ID', u'Original Method', u'Original Registration Date', u'MWBE Goal', u'State Fed', u'No Ethnicity Goal',u'Agency Comments', u'MWBE_GOALS', u'STATE_FED_FUNDED', u'MWBE72_FED_FUND']]

    # df1.to_csv('ConsolidatedReceivedTADsFY%s_%s.txt' % (str(FY),str(FQ)))

    list = [str(tuple(x)).replace('u\'','\'') for x in df1.values]

    insert_list = ["""INSERT INTO tblMWBEPrimesTAD_FY%s_%s (Agency, ContractID, [Vendor Name], Purpose, Method, Industry, [Registration Date], [Contract Value], [Original ContractID], [Original Method], [Original Registration Date], [MWBE GOAL], [State Fed Funding], [Reason for No Ethnicity Goals], [Agency Comments],[MWBE_GOALS],[STATE_FED_FUNDED], [MWBE72Fed]) VALUES """ % (str(FY)[2:4],FQ) + list[x] + ';' for x in range(len(list))]

    Access_Path = r'S:\Contracts\Research and IT\Procurement Indicators\FY 2019 Procurement Indicators\MWBE\Working.accdb;'
    conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
    cnxn = pyodbc.connect(conn_str)
    crsr = cnxn.cursor()

    sql_statement = ["""DROP TABLE tblMWBEPrimesTAD_FY%s_%s;""" % (str(FY)[2:4],FQ),
                        """CREATE TABLE tblMWBEPrimesTAD_FY%s_%s (Agency VARCHAR(100), ContractID VARCHAR(255), [Vendor Name] VARCHAR(255), Purpose VARCHAR(255), Method VARCHAR(255), Industry VARCHAR(255), [Registration Date] VARCHAR(255), [Contract Value] VARCHAR(255), [Original ContractID] VARCHAR(255), [Original Method] VARCHAR(255), [Original Registration Date] VARCHAR(255), [MWBE GOAL] VARCHAR(255), [State Fed Funding] VARCHAR(255), [Reason for No Ethnicity Goals] VARCHAR(255), [Agency Comments] VARCHAR(255), [MWBE_GOALS] BIT, [STATE_FED_FUNDED] BIT, [MWBE72Fed] BIT);""" % (str(FY)[2:4],FQ),
                        """UPDATE tblMWBEPrimesTAD_FY%s_%s INNER JOIN tblMainTable_OpenContracts ON tblMWBEPrimesTAD_FY%s_%s.ContractID = tblMainTable_OpenContracts.ContractID SET tblMainTable_OpenContracts.MWBE_LL = IIf([Original Registration Date] Between #5/15/2006# And #12/31/2012#,?,IIf([Original Registration Date]>#1/1/2013#,?,IIf([Registration Date]>#1/1/2013#,?,?)));""" % (str(FY)[2:4],FQ,str(FY)[2:4],FQ),
                        """UPDATE (tblNoTSPReasons INNER JOIN tblMWBEPrimesTAD_FY%s_%s ON tblNoTSPReasons.NoTSPforTAD = tblMWBEPrimesTAD_FY%s_%s.[Reason for No Ethnicity Goals]) INNER JOIN tblMainTable_OpenContracts ON tblMWBEPrimesTAD_FY%s_%s.ContractID = tblMainTable_OpenContracts.ContractID SET tblMainTable_OpenContracts.NoTSPReason = [tblNoTSPReasons].[NoTSPReasonID], tblMainTable_OpenContracts.NoTSPComment = [tblMWBEPrimesTAD_FY%s_%s].[Agency Comments];""" % (str(FY)[2:4],FQ,str(FY)[2:4],FQ,str(FY)[2:4],FQ,str(FY)[2:4],FQ),
                        """UPDATE tblMWBEPrimesTAD_FY%s_%s INNER JOIN tblMainTable_OpenContracts ON tblMWBEPrimesTAD_FY%s_%s.ContractID = tblMainTable_OpenContracts.ContractID SET tblMainTable_OpenContracts.MWBE_GOALS = tblMWBEPrimesTAD_FY%s_%s.MWBE_GOALS, tblMainTable_OpenContracts.STATE_FED_FUNDED = tblMWBEPrimesTAD_FY%s_%s.STATE_FED_FUNDED;""" % (str(FY)[2:4],FQ, str(FY)[2:4],FQ, str(FY)[2:4],FQ, str(FY)[2:4],FQ),
                        """UPDATE tblMWBEPrimesTAD_FY%s_%s INNER JOIN tblMainTable_OpenContracts ON tblMWBEPrimesTAD_FY%s_%s.ContractID = tblMainTable_OpenContracts.ContractID SET tblMainTable_OpenContracts.MWBE72Fed = tblMWBEPrimesTAD_FY%s_%s.MWBE72Fed;""" % (str(FY)[2:4],FQ, str(FY)[2:4],FQ, str(FY)[2:4],FQ)]

    try:
         crsr.execute(sql_statement[0]) #Dropping PrimeTADs
         crsr.commit()
    except:
         pass

    try:
         crsr.execute(sql_statement[1]) #Creating PrimeTADs
         crsr.commit()
    except:
         pass

    for x in range(len(insert_list)): #Inserting Prime_TADs
         try:
             crsr.execute(insert_list[x])
             crsr.commit()
         except:
             print(insert_list[x])
             pass

    l1 = 'LL129'
    l2 = 'LL1'
    l3 = 'N/A'
    params = (l1,l2,l2,l3)
    crsr.execute(sql_statement[2], params)  #Updating
    crsr.commit()

    crsr.execute(sql_statement[3])  #Updating NoTSPReason
    crsr.commit()

    l1 = 'LL129'
    l2 = 'LL1'
    l3 = 'N/A'
    params = (l1,l2,l2,l3)

    crsr.execute(sql_statement[4])  #Updating MWBE Goals
    crsr.commit()

    crsr.execute(sql_statement[5])  #Updating MWBE Goals
    crsr.commit()
