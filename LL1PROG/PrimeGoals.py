import pandas as pd
import os
import datetime as dt
import pyarrow.parquet as pq
from datetime import timedelta
import datetime
import LL1PROG.SBSMWBETable as sbstable

#Python 2.7.0
#Pulls Data From Access. Does Not Push.

os.system('python S:\Contracts\\\\\"Research and IT\"\\\\\"08 - MWBE\"\\\\\"DAS Only\"\\\\\"09 - Python and R Scripts\"\\\\\"LL1 PROG\"\\\\R_VEND_CUST.py')

data_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\PRODUCTION\Datasets'

oc_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Open Contracts'

t = datetime.datetime.now().date()

###

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

start_date = pd.to_datetime(date_range_start)
end_date = pd.to_datetime(date_range_end)

###

def size_group(row):
    if row['ContractValue'] <= 20000 and row['Industry'] != ['Construction Services']:
        return 'Micro Purchase'
    elif row['ContractValue'] <= 35000 and row['Industry'] == 'Construction Services':
        return 'Micro Purchase'
    elif row['ContractValue'] > 20000 and row['ContractValue'] <= 100000 and row['Industry'] != ['Construction Services']:
        return 'Small Purchase'
    elif row['ContractValue'] > 35000 and row['ContractValue'] <= 100000 and row['Industry'] == 'Construction Services':
        return 'Small Purchase'
    elif row['ContractValue'] > 100000 and row['ContractValue'] <= 1000000:
        return '>$100K, <=$1M'
    elif row['ContractValue'] > 1000000 and row['ContractValue'] <= 5000000:
        return '>$1M, <=$5M'
    elif row['ContractValue'] > 5000000 and row['ContractValue'] <= 25000000:
        return '>$5M, <=$25M'
    elif row['ContractValue'] > 25000000:
        return '>$25M'
    else:
        return 'NA'

df = pq.read_table(oc_path +'\\'+'open_contracts_%s.parquet' % (str(t))).to_pandas()

df.columns = ['Agency', 'DOC_CD','DOC_DEPT_CD','DOC_ID','ContractID','EPIN','ContractValue','MWBE_LL','Method', 'VendorTIN','VendorNumber','VendorName','Purpose','StartDate','EndDate','RegistrationDate','Industry','ExcludeAll','ExcludeCategory','STATE_FED_FUNDED','MWBE72Fed','MWBE_GOALS', 'NoTSPReason', 'Base_EPIN', 'TSP','Goal_Black','Goal_Asian', 'Goal_Hispanic', 'Goal_Woman', 'Goal_Unspecified']

start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

df['RegistrationDate'] = pd.to_datetime(df['RegistrationDate'])

df = df[(df['RegistrationDate']>=start_date) & (df['RegistrationDate']<=end_date)]

df = df[df['MWBE_GOALS']==True]

df = df[~df['Industry'].isin(["Non-Procurement","Human Services","Goods"])]

df = df[df['Method'].isin(["Accelerated","Competitive Sealed Bid","Demonstration Project","Innovative","Micro Purchase","Negotiated Acquisition","Negotiated Acquisition Extension","Renewal","Request for Proposal","Small Purchase","Micropurchase", "MWBE PURCHASE 150K", "MWBE 72"])]
df = df[~df['NoTSPReason'].isin([float(1),float(3),float(6),float(7),float(8), float(11), float(12)])]

df = df[df['ExcludeAll'].isin([False])]

#Non-Profit List from Access
if 'nonpFY%s.txt' % (str(FY)[2:4]) in set(os.listdir(data_path)):
    nonp = pd.read_csv(data_path +'\\'+ 'nonpFY%s.txt' % (str(FY)[2:4]))

df = df[~df['VendorTIN'].isin(nonp['EIN'])]
prime_goals = df.drop_duplicates(subset = ['ContractID'])

sbs = sbstable.sbs

sbs['FMS_VENDOR_ID']= sbs['FMS_VENDOR_ID'].astype(str)
prime_goals['VendorNumber'] = prime_goals['VendorNumber'].astype(str)

prime_goals1 = prime_goals[prime_goals['VendorNumber'].isin(sbs['FMS_VENDOR_ID'])]
prime_goals1.loc[:,'MWBE_Status'] = 'MWBE'
prime_goals2 = prime_goals[~prime_goals['VendorNumber'].isin(sbs['FMS_VENDOR_ID'])]
prime_goals2.loc[:,'MWBE_Status'] = 'Not MWBE'

prime_goals = pd.concat([prime_goals1,prime_goals2])

prime_goals = prime_goals.merge(sbs[['FMS_VENDOR_ID','ReportCategory', 'EthGen']], how = 'left', left_on = 'VendorNumber', right_on = 'FMS_VENDOR_ID')

prime_goals['SizeGroup'] = list(prime_goals.apply(size_group, axis = 1))
