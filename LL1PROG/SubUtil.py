import datetime
import pandas as pd
import numpy as np
import os
from os import path
import pyarrow.parquet as pq
from datetime import timedelta

#Python 2.7.0.32-bit.
#Pulls Data From Access. Does Not Push.

pd.options.mode.chained_assignment = None

t = datetime.datetime.now().date()

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

####

def si(row):
    if row['SubIndustry'] == 'Construction':
        return 'Construction Services'
    elif row['SubIndustry'] == 'Standard Services':
        return 'Standardized Services'
    elif row['SubIndustry'] == 'Arch and Enginrng':
         return 'Professional Services'
    elif row['SubIndustry'] == 'Professional Services':
         return 'Professional Services'
    elif 'Arch' in row['Industry']:
         return 'Professional Services'
    elif row['SubIndustry'] == 'Other' and row['Industry'] == 'Architecture/Engineering':
        return 'Professional Services'
    elif row['SubIndustry'] == 'Other' and row['Industry'] == 'Standardized Services':
        return 'Standardized Services'
    elif row['SubIndustry'] == 'Other' and row['Industry'] == 'Human Services':
        return 'Human Services'
    elif row['SubIndustry'] == 'Other' and row['Industry'] == 'Construction Services':
        return 'Construction Services'
    else:
        return row['Industry']

def size_group(x,y): #row['ContractValue'], row['Industry']
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

def FiscalQuarter(x):
    if pd.to_datetime(x).month in [7, 8, 9]:
        return 'Q1'
    elif pd.to_datetime(x).month in [10, 11, 12]:
        return 'Q2'
    elif pd.to_datetime(x).month in [1, 2, 3]:
        return 'Q3'
    elif pd.to_datetime(x).month in [4, 5, 6]:
        return 'Q4'

fq = np.vectorize(FiscalQuarter)

if os.path.isfile('subs_generation_dates%s_%s.pkl' % (str(FY), str(FQ)))==True:
    d = pd.read_pickle('subs_generation_dates%s_%s.pkl' % (str(FY), str(FQ)))
    d = d.append(pd.Series([t]))
    d.to_pickle('subs_generation_dates%s_%s.pkl' % (str(FY), str(FQ)))
else: #first date
    subs_lists = []
    subs_lists.append(t)
    d = pd.Series(subs_lists, name='Dates')
    d.to_pickle('subs_generation_dates%s_%s.pkl' % (str(FY), str(FQ)))

sl = pd.read_pickle('subs_generation_dates%s_%s.pkl' % (str(FY), str(FQ)))

sl = pd.to_datetime(sl)

lock_in_date = min(sl)

print ('Subs Lock in Date: %s' % (str(lock_in_date)))

oc_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Open Contracts'

df = pq.read_table(oc_path +'\\'+'open_contracts_%s.parquet' % (str(t))).to_pandas()

df.columns = ['Agency', 'DOC_CD','DOC_DEPT_CD','DOC_ID','ContractID','EPIN','ContractValue','MWBE_LL','Method', 'VendorTIN','VendorNumber','VendorName','Purpose','StartDate','EndDate','RegistrationDate','Industry','ExcludeAll','ExcludeCategory','STATE_FED_FUNDED','MWBE72Fed','MWBE_GOALS', 'NoTSPReason', 'Base_EPIN', 'TSP','Goal_Black','Goal_Asian', 'Goal_Hispanic', 'Goal_Woman', 'Goal_Unspecified']

df = df[~df['NoTSPReason'].isin(['1.','3.','6.','7.','8.','11.','12.'])]

df1 = df[df['MWBE_GOALS']==True]
open = df1[df1['MWBE72Fed'] ==False]

open.loc[:,'ContractID'] = open['ContractID'].astype(str)

# Option Point: Pull Fresh Data. Else Data will be from last pickle pull.

pkl_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\tblSubcontracts_FMS3'

try:
    subs = pd.read_pickle(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\tblSubcontracts_FMS3\tbl_subs%s.pkl' % (str(lock_in_date.date())))
except:
    os.system('python S:\Contracts\\\\\"Research and IT\"\\\\\"08 - MWBE\"\\\\\"DAS Only\"\\\\\"09 - Python and R Scripts\"\\\LL1ProgFY19Q3\\LL1PROG\\\\\"tblSubContractNEWMAIN No CGTL\".py')
    subs = pd.read_pickle(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\tblSubcontracts_FMS3\tbl_subs%s.pkl' % (str(lock_in_date.date())))

df = open.merge(subs, how = 'inner', on = ['ContractID'])

df = df.drop_duplicates(['SubContractID'])

subs1 = df[~df['SubIndustry'].isin(['Goods', 'Human Services'])]

subs2 = df[(df['MWBE_LL'] == 'LL129') & (~df['SubIndustry'].isin(["Goods","Standard Services","Human Services"])) & (df['SubValue']<=100000)]

subs = pd.concat([subs1,subs2])

subs = subs.drop_duplicates(['SubContractID'])

subs = subs[subs['Agency'] != '']
subs.loc[:,'SubValue'] = subs['SubValue'].astype(float)

subs.loc[:,'SubIndustry'] = subs.apply(si, axis = 1)

subs.loc[:,'SizeGroup'] = size_group(subs['SubValue'],subs['SubIndustry'])

sbs = pd.read_table(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\PRODUCTION\SBS MWBE\sbsfy19_q3.txt', sep = ',',header = 0)

sub_1 = subs[subs['SubVendorNumber'].isin(sbs.loc[:,'FMS_VENDOR_ID'])]
sub_1.loc[:,'MWBE_Status'] = 'MWBE'
sub_2 = subs[~subs['SubVendorNumber'].isin(sbs.loc[:,'FMS_VENDOR_ID'])]
sub_2.loc[:,'MWBE_Status'] = 'Not MWBE'
subs = pd.concat([sub_1,sub_2])

subs = subs.merge(sbs, how = 'left', left_on = 'SubVendorNumber', right_on ='FMS_VENDOR_ID') #BLOCK

subs = subs[subs['SizeGroup'] != 'NA']
subs.loc[:,'FQuarter'] = fq(subs['SubStartDate'])

subs = subs.drop_duplicates(['SubContractID'])

subs = subs.dropna(subset = ['SubValue'])

sub_util = subs

del sub_util['MWBE72Fed'] #Undesired Printout

sub_util = sub_util[[u'Agency', u'Method', u'Industry', u'ContractValue', u'ContractID', u'DOC_CD_x', u'DOC_DEPT_CD_x', u'DOC_ID_x', u'MWBE_LL', u'RegistrationDate', u'SubIndustry', u'SubValue', u'SubStartDate', u'Source', u'SubVendorName', u'SubVendorNumber', u'SubDescr', u'SubContractID',u'MWBE_Status', u'FMS_VENDOR_ID', u'TIN',u'VendorNumber',u'Application Type', u'MWBEType', u'ETHNICITY',  u'EthGen', u'ReportCategory',u'SizeGroup', u'FQuarter']]
sub_util.columns = [u'Agency', u'Method', u'Industry', u'ContractValue', u'ContractID', u'DOC_CD', u'DOC_DEPT_CD', u'DOC_ID', u'MWBE_LL', u'RegistrationDate', u'SubIndustry', u'SubValue', u'SubStartDate', u'Source', u'SubVendorName', u'SubVendorNumber', u'SubDescr', u'SubContractID', u'MWBE_Status', u'VEND_CUST_CD', u'TIN', u'FMS Vendor Number', u'Application Type', u'MWBEType', u'ETHNICITY', u'EthGen', u'ReportCategory',u'SizeGroup', u'FQuarter']

sub_util = sub_util.drop_duplicates()

