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

t = dt.datetime.now().date()

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

def industry_cleanup(row):
    if row['Industry'] == u'Architecture/Engineering':
        return 'Professional Services'
    else:
        return row['Industry']

def reportcategory(row):  #df1['MWBEType'], df1['EthGen']
    a = u'MWBEType'
    b = u'EthGen'
    if row[a] == 'MBE' and row[b] == 'Asian American':
        return 'Male-Owned MBE - Asian'
    elif row[a] == 'MBE' and row[b] == 'Black American':
        return 'Male-Owned MBE - Black'
    elif row[a] == 'MBE' and row[b] == 'Hispanic American':
        return 'Male-Owned MBE - Hispanic'
    elif row[a] == 'WBE':
        return 'WBE - Caucasian Woman'
    elif row[a] in ['M/WBE','MWBE'] and row[b] == 'Asian American':
        return 'WBE - Asian'
    elif row[a] in ['M/WBE','MWBE'] and row[b] == 'Black American':
        return 'WBE - Black'
    elif row[a] in ['M/WBE','MWBE'] and row[b] == 'Hispanic American':
        return 'WBE - Hispanic'

def FiscalQuarter(row):
    a = r'RegistrationMonth'
    if row[a] in [7, 8, 9]:
        return 'Q1'
    elif row[a] in [10, 11, 12]:
        return 'Q2'
    elif row[a] in [1, 2, 3]:
        return 'Q3'
    elif row[a] in [4, 5, 6]:
        return 'Q4'

def FiscalYear(x,y): #df1['Month'], df1['RegistrationYear']
    if x in [7, 8, 9]:
        return y + 1
    elif x in [10, 11, 12]:
        return y + 1
    elif x in [1, 2, 3]:
        return y
    elif x in [4, 5, 6]:
        return y

FiscalYear = np.vectorize(FiscalYear)

def size_group(x,y):
    if x <= 20000 and y != ['Construction Services']:
        return 'Micro Purchase'
    elif x <= 35000 and y == 'Construction Services':
        return 'Micro Purchase'
    elif x > 20000 and x <= 100000 and y != ['Construction Services']:
        return 'Small Purchase'
    elif x > 35000 and x <= 100000 and y == 'Construction Services':
        return 'Small Purchase'
    elif x > 100000 and x <= 1000000:
        return '>$100K, <=$1M'
    elif x > 1000000 and x <= 5000000:
        return '>$1M, <=$5M'
    elif x > 5000000 and x <= 25000000:
        return '>$5M, <=$25M'
    elif x > 25000000:
        return '>$25M'
    else:
        return 'NA'

size_group = np.vectorize(size_group)

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Open Contracts'

df = pq.read_table(path +'\\'+'open_contracts_%s.parquet' % (str(t))).to_pandas()

df.columns = ['Agency', 'DOC_CD','DOC_DEPT_CD','DOC_ID','ContractID','EPIN','ContractValue','MWBE_LL','Method', 'VendorTIN','VendorNumber','VendorName','Purpose','StartDate','EndDate','RegistrationDate','Industry','ExcludeAll','ExcludeCategory','STATE_FED_FUNDED','MWBE72Fed','MWBE_GOALS', 'NoTSPReason', 'Base_EPIN', 'TSP','Goal_Black','Goal_Asian', 'Goal_Hispanic', 'Goal_Woman', 'Goal_Unspecified']

start_date = pd.to_datetime(start_date) - timedelta(days = 1)

end_date = pd.to_datetime(end_date) + timedelta(days = 1)

df['RegistrationDate'] = pd.to_datetime(df['RegistrationDate'])

df = df[(df['RegistrationDate']>start_date) & (df['RegistrationDate']<end_date)]

df = df[(df['Agency'] !='OMB') & (df['Agency'] !='City Hall')]
df = df[df['MWBE72Fed']== False]
df = df[~df['DOC_CD'].isin(['CTA1','RCT1'])]
df = df[~df['DOC_DEPT_CD'].isin(['040'])]
df = df[~df['Industry'].isin(['Non-Procurement'])]
# METROPOLITAN TRANSPORTATION AUTHORITY  0000775990, NEW YORK CITY HOUSING AUTHORITY 0000947209, NEW YORK CITY TRANSIT AUTHORITY 0000650614, NYC TRANSIT AUTHORITY 0000949814, PORT AUTHORITY OF NEW YORK AND NEW JERSEY 0000947155
df = df[~df['VendorNumber'].isin(["0000775990","0000947209","0000650614","0000949814","0000947155"])]
df = df[df['Method'].isin(['Accelerated','Competitive Sealed Bid','Demonstration Project','Innovative','Micro Purchase','Negotiated Acquisition','Negotiated Acquisition Extension','Renewal','Request for Proposal','Small Purchase','Micropurchase', 'MWBE 72'])]

df1 = df[df['ExcludeAll']==False]
df1 = df1[~df1['NoTSPReason'].isin([float(1),float(3),float(6),float(7),float(8),float(11), float(12)])] #include only contracts outside of 1,3,6,8.
df1_1 = df1[~df1['Industry'].isin(['Goods','Human Services'])]
df1_2 = df1[df1['Industry'].isin(['Goods']) & (df1['ContractValue']<=float(100000))]
df1 = pd.concat([df1_1,df1_2])


df2 = df[(df['ExcludeAll']==True) & (df['ExcludeCategory']==float(16))]
df2 = df2[~df2['NoTSPReason'].isin([float(1),float(3),float(6),float(7),float(8),float(11), float(12)])]
df2_1 = df2[~df2['Industry'].isin(['Goods','Human Services'])]
df2_2 = df2[df2['Industry'].isin(['Goods']) & (df2['ContractValue']<=float(100000))]
df2 = pd.concat([df2_1,df2_2])

df = pd.concat([df1,df2])

if 'nonpFY%s.txt' % (str(FY)[2:4]) in set(os.listdir(data_path)):
    nonp = pd.read_csv(data_path +'\\'+ 'nonpFY%s.txt' % (str(FY)[2:4]))

df = df[~df['VendorTIN'].isin(nonp['EIN'])] #Select only for profits

df.loc[:,'RegistrationMonth'] = [x.month for x in df['RegistrationDate']]
df.loc[:,'RegistrationYear'] = [x.year for x in df['RegistrationDate']]

p = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\PRODUCTION\SBS MWBE'

if [x for x in os.listdir(p) if str(FY)[2:4] in x if FQ in x][0]:
    sbs = pd.read_csv(p+'\\'+'sbsFY%s_%s.txt' % (str(FY)[2:4], str(FQ)))
else:
    print ('No SBS List')

# #Set MWBE_Status
df_1 = df[df[u'VendorNumber'].isin(sbs['FMS_VENDOR_ID'])]
df_1.is_copy = False
df_1['MWBE_Status'] = 'MWBE'

df_2 = df[~df['VendorNumber'].isin(sbs['FMS_VENDOR_ID'])]
df_2.is_copy = False
df_2['MWBE_Status'] = 'Not MWBE'

df = pd.concat([df_1,df_2])

df1 = df.merge(sbs, how = 'left', left_on = 'VendorNumber', right_on ='FMS_VENDOR_ID')

df1 = df1.drop_duplicates(subset = ['ContractID'])

df1.loc[:,'Industry'] = df1.apply(industry_cleanup, axis = 1)

df1.loc[:,'ReportCategory'] = df1.apply(reportcategory, axis = 1)

df1.loc[:,'REG_FQ'] = df1.apply(FiscalQuarter, axis = 1)

df1.loc[:,'REG_FY'] = FiscalYear(df1['RegistrationMonth'],df1['RegistrationYear'])

df1.loc[:,'SizeGroup'] = size_group(df1['ContractValue'],df1['Industry'])

prime_util = df1

prime_util['MWBE_LL'] = prime_util['MWBE_LL'].fillna('LL1')
