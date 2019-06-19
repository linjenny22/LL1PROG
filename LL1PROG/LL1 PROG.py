import datetime
import pandas as pd
import pyodbc
import sys
import numpy as np
import os
import re
import csv
import PrimeUtil
import SubUtil
import SBSMWBETable
import PrimeGoals
import EBETable
from xlsxwriter.utility import xl_range
import time
import pyarrow.parquet as pq

start = time.monotonic()

#Must Be Date
t = datetime.datetime.now().date()

data_set_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\FY18 MWBE Prog Files\Scripts\Optimized\LL1 Reporting - PRODUCTION\Datasets'

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\PRODUCTION\SBS MWBE'

sbs = SBSMWBETable.sbs

data_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\PRODUCTION\Datasets'
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

start_date = str(date_range_start.month) + '/' + str(date_range_start.day) + '/' + str(date_range_start.year)
end_date = str(date_range_end.month) + '/' + str(date_range_end.day) + '/' + str(date_range_end.year)

###

def fillorigdate(x,y): #row['Original Registration Date'], row['Original Registration Date'], row['RegistrationDate']
    if  x == pd.to_datetime('1970-01-01'):  # Critical Step - Replace Original Registration Date
        return y
    elif  x == '0':
        return y
    else:
        return x

fillorigdate = np.vectorize(fillorigdate)

def mwbe_ll(x): #row['Original Registration Date2']
    if pd.to_datetime(x,format='%Y-%m-%d') >= pd.to_datetime('01012013', format='%m%d%Y', errors='ignore'):
        return 'LL1'
    elif pd.to_datetime(x,format='%Y-%m-%d') >= pd.to_datetime('05152006', format='%m%d%Y', errors='ignore') and pd.to_datetime(x)<= pd.to_datetime('12312012', format='%m%d%Y', errors='ignore'):
        return 'LL129'

mwbe_ll = np.vectorize(mwbe_ll)

def fillmwbe_p(row):
    if row['MWBE_LL_y'] in (0,'None'):
        if row['RegistrationDate'] > pd.to_datetime('01012013', format='%m%d%Y', errors='ignore'):
            return 'LL1'
        elif row['RegistrationDate'] > pd.to_datetime('01012013', format='%m%d%Y', errors='ignore'):
            return 'LL1'
        elif row['RegistrationDate'] >= pd.to_datetime('05152006', format='%m%d%Y', errors='ignore') and row['RegistrationDate'] <= pd.to_datetime('12312012', format='%m%d%Y', errors='ignore'):
            return 'LL129'
    else:
        return row['MWBE_LL_y']

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

def fillmwbe_s(row):
    if pd.to_datetime(row['SubStartDate'])>= pd.to_datetime('20130101', format='%Y%m%d', errors='ignore'):
        return 'LL1'
    elif pd.to_datetime(row['SubStartDate'])>= pd.to_datetime('20060515', format='%Y%m%d', errors='ignore') and row['SubStartDate'] <= pd.to_datetime('20121231', format='Y%m%d%', errors='ignore'):
        return 'LL129'

def agency(x):
    if x == 'DoITT':
        return 'DOITT'
    elif x == 'Law':
        return 'LAW'
    else:
        return x

agency = np.vectorize(agency)

def subindustry_new(row):
    if row['SubIndustry'] == 'Arch and Enginrng':
        return 'Professional Services'
    elif row['SubIndustry'] == u'Architecture/Engineering':
        return 'Professional Services'
    elif row['SubIndustry'] == 'Construction':
        return 'Construction Services'
    elif row['SubIndustry'] == 'Standard Services':
        return 'Standardized Services'
    elif row['SubIndustry'] == 'Other' and row['PrimeIndustry'] == 'Professional Services':
        return 'Professional Services'
    elif row['SubIndustry'] == 'Other' and row['PrimeIndustry'] == 'Standardized Services':
        return 'Standardized Services'
    elif row['SubIndustry'] == 'Other' and row['PrimeIndustry'] == 'Architecture/Engineering':
        return 'Professional Services'
    elif row['SubIndustry'] == 'Other' and row['PrimeIndustry'] == 'Construction Services':
        return 'Construction Services'
    else:
         return row['SubIndustry']

def industry_map(x):
    if x == 'Architecture/Engineering':
        return 'Professional Services'
    else:
        return x

industry_map = np.vectorize(industry_map)

def size_group_subs(x,y,z): #row['SubValue'], row['SubIndustry'], row['SizeGroup']
        if x <= 35000 and y == 'Construction Services':
            return 'Micro Purchase'
        elif x <= 20000:
            return 'Micro Purchase'
        elif x <= 100000 and x> 35000 and y == 'Construction Services':
            return 'Small Purchase'
        elif x <= 100000 and x > 20000:
            return 'Small Purchase'
        else:
            return z

size_group_subs = np.vectorize(size_group_subs)

pu = PrimeUtil.prime_util

su = SubUtil.sub_util

ebe = EBETable.ebe

prime_goals = PrimeGoals.prime_goals

prime_ebe = pu[pu['VendorNumber'].isin(ebe['FMS Vendor Number'])]

#Master Combined Util File
if __name__ == "__main__":

    prime_util = pu

    sub_util = su

    prime_m = prime_util

    prime_m['ReportCategory'] = prime_m['ReportCategory'].str.replace('None','')

    prime_m['SizeGroup'] = size_group(prime_m['ContractValue'], prime_m['Industry'])

    prime_util = prime_m[[u'Agency', u'DOC_CD', u'DOC_DEPT_CD', u'DOC_ID', u'ContractID', u'EPIN', u'RegistrationDate',
         u'ContractValue', u'Industry', u'SizeGroup', u'MWBE_Status', u'ReportCategory', u'EthGen',
         u'MWBE_LL', u'Method', u'VendorName', u'VendorTIN', u'VendorNumber', u'Purpose', u'StartDate',
         u'EndDate', u'REG_FQ', u'REG_FY']]

    prime_util.columns = [u'Agency', u'DOC_CD', u'DOC_DEPT_CD', u'DOC_ID', u'ContractID', u'EPIN',
                          u'RegistrationDate', u'ContractValue', u'Industry', u'SizeGroup', u'MWBE_Status',
                          u'ReportCategory', u'EthGen', u'MWBE_LL', u'Method', u'VendorName', u'VendorTIN',
                          u'VendorNumber', u'Purpose', u'StartDate', u'EndDate', u'REG_FQ', u'REG_FY']

    prime_util = prime_util.drop_duplicates('ContractID')

    prime_util['Agency'] = agency(prime_util['Agency'])
    primes = prime_util
    pw = prime_util

    ###

    sub_util.loc[:,'SubDescr'] = sub_util['SubDescr'].str.lower()

    sub_util.loc[:, 'MWBE_LL'] = sub_util['MWBE_LL'].fillna('0')

    sub_util_1 = sub_util[sub_util['MWBE_LL']=='0']
    sub_util_2 = sub_util[sub_util['MWBE_LL']!='0']

    sub_util_1.loc[:,'SubStartDate'] = pd.to_datetime(sub_util_1['SubStartDate'])

    sub_util_1.loc[:,'MWBE_LL'] = sub_util.apply(fillmwbe_s, axis=1)

    sub_util = pd.concat([sub_util_1,sub_util_2])

    sub_util = sub_util[[u'Agency', u'ContractID', u'Method', u'Industry', u'ContractValue', u'SubIndustry', u'SubValue',
          u'SizeGroup', u'SubStartDate', u'ReportCategory', u'EthGen', u'MWBE_Status', u'MWBE_LL',
          u'RegistrationDate',
          u'SubVendorName', u'SubVendorNumber', u'SubDescr', u'FQuarter', u'SubContractID']]

    sub_util = sub_util[sub_util['ReportCategory'] != str(0)]

    sub_util.loc[:, 'PrimeIndustry'] = sub_util['Industry']

    sub_util.loc[:, 'SizeGroup'] = size_group_subs(sub_util['SubValue'], sub_util['SubIndustry'], sub_util['SizeGroup'])

    subs = sub_util

    subs.loc[:, 'Industry'] = subs.apply(subindustry_new, axis = 1)

    subs['Agency'] = agency(subs['Agency'])

    sw = subs

    merged_data_set = pd.merge(pw[pw['MWBE_Status'] == 'MWBE'], sw, on=['ContractID', 'Agency'])  # Inner Join of MWBE Primes and all Subs. Result should be all subs off of MWBE Primes

    prime_util['ContractID']=prime_util['ContractID'].astype(str)
    sub_util['ContractID']=sub_util['ContractID'].astype(str)

    merged_data_set_total = pd.merge(prime_util, sub_util, how = 'left', on=['ContractID','Agency'])  # All Subs off of All Primes #fewer columns than the next table

    merged_original = pd.merge(primes, subs, on=['ContractID', 'Agency'])  # same as merged_data_set_total except with more columns

    MWBESubs_onMWBEPrimes = merged_data_set[(merged_data_set['MWBE_Status_y'] == 'MWBE')]  # only those MWBE subs off of MWBE Primes

    NonMWBESubs_onMWBEPrimes = merged_original[(merged_original['MWBE_Status_y'] == 'Not MWBE') & (merged_original['MWBE_Status_x'] == 'MWBE')]

    ##
    list = []
    summary_list = []
    ranked_summary_list = []

    for i, j, k in [['Agency', 'Agency', 'Agency'], ['Industry', 'Industry_y', 'Industry_x']]:
        a1 = pw[(pw['MWBE_Status'] == 'MWBE')].groupby([i]).ContractID.count()
        a2 = pw[(pw['MWBE_Status'] == 'MWBE')].groupby([i]).ContractValue.sum()
        a1.name = 'MWBE Primes (#)'
        a2.name = 'MWBE Primes ($)'
        a3 = pw.groupby([i]).ContractValue.sum()
        a3.name = 'Total Primes ($)'
        per = (a2 / a3).fillna(0)
        per.name = 'MWBE of Total (%)'
        a4 = pw.groupby([i]).ContractID.count()
        a4.name = 'Total Primes (#)'
        a5 = subs[(subs['MWBE_Status'] == 'MWBE')].groupby([i]).SubContractID.nunique()
        a5.name = 'MWBE SubContracts (#)'
        a6 = subs[(subs['MWBE_Status'] == 'MWBE')].groupby([i]).SubValue.sum()
        a6.name = 'MWBE SubContracts ($)'
        a7 = subs.groupby([i]).SubContractID.count()
        a7.name = 'Total SubContracts (#)'
        a8 = subs.groupby([i]).SubValue.sum()
        a8.name = 'Total SubContracts ($)'
        per2 = (a6 / a8).fillna(0)
        per2.name = 'MWBE Subcontract (%)'
        a9 = merged_data_set[(merged_data_set['MWBE_Status_y'] == 'MWBE')].groupby([j]).SubContractID.nunique()
        a9.name = 'M/WBE Subs on MWBE Primes (#)'
        a10 = merged_data_set[(merged_data_set['MWBE_Status_y'] == 'MWBE')].groupby([j]).SubValue.sum()
        a10.name = 'M/WBE Subs on MWBE Primes ($)'
        a11 = merged_original[(merged_original['MWBE_Status_y'] == 'Not MWBE') & (
        merged_original['MWBE_Status_x'] == 'MWBE')].groupby([j]).SubContractID.nunique()
        a11.name = 'Non MWBE Subs on MWBE Primes (#)'
        if len(a11) == 0:
            col = np.zeros(shape=(len(a8), 1))
            a11 = pd.DataFrame(col, index=a8.index, columns=['Non MWBE Subs on MWBE Primes (#)'])
        a12 = merged_original[(merged_original['MWBE_Status_y'] == 'Not MWBE') & (
        merged_original['MWBE_Status_x'] == 'MWBE')].groupby(
            [j]).SubValue.sum()  # grouped by INDUSTRY OF THE SUBS.
        a12.name = 'Non MWBE Subs on MWBE Primes ($)'
        if len(a12) == 0:
            col = np.zeros(shape=(len(a8), 1))
            a12 = pd.DataFrame(col, index=a8.index, columns=['Non MWBE Subs on MWBE Primes ($)'])
        a13 = merged_data_set_total.groupby([j]).SubContractID.nunique()
        a13.name = 'All Subs on All Primes (#)'
        a14 = merged_data_set_total.groupby([
                                                j]).SubValue.sum()  # for all subs on all primes, grouped by Prime Industry. (Confident on my definitions because they matched with Jin's.
        a14.name = 'All Subs on All Primes ($)'

        a15_1 = a1
        a15_2 = a5
        a15_3 = -1 * a9
        a15_4 = -1 * a11
        a15 = pd.concat([a15_1, a15_2, a15_3, a15_4], axis=1).fillna(0).sum(axis=1)
        a15.name = 'Total MWBE (#)'

        a16_1 = a2.astype(float)
        a16_2 = a6.astype(float)
        a16_3 = -1 * a10.astype(float)
        a16_4 = -1 * a12.astype(float)

        a16_f2 = pd.concat([a16_1, a16_2], axis=1).fillna(0).sum(axis=1)
        a16_f3 = pd.concat([a16_f2, a16_3], axis=1).fillna(0).sum(axis=1)
        a16_f4 = pd.concat([a16_f3, a16_4], axis=1).fillna(0).sum(axis=1)
        a16_f4.name = 'Total MWBE ($)'  # Can't concat series that are of different lengths. Last two series have fewer agencies as first two.

        a17_1 = pw.groupby([i]).ContractID.count()
        a17_2 = sw.groupby([i]).SubContractID.count()
        a17_3 = -1 * merged_data_set_total.groupby([k]).SubContractID.count()
        a17 = pd.concat([a17_1, a17_2, a17_3], axis=1).fillna(0).sum(axis=1)
        a17.name = 'Total (#)'

        a18_1 = pw.groupby([i]).ContractValue.sum().astype(float)
        a18_2 = sw.groupby([i]).SubValue.sum().astype(float)
        a18 = pd.concat([a18_1, a18_2], axis=1).fillna(0).sum(axis=1)  # ***
        a18_3 = -1 * merged_data_set_total.groupby([k]).SubValue.sum().astype(float)
        a18_f = pd.concat([a18, a18_3], axis=1).fillna(0).sum(axis=1)
        a18_f.name = 'Total ($)'

        per3 = (a16_f4 / a18_f).fillna(0)
        per3.name = 'Total MWBE (%)'

        final = pd.concat(
            [a1, a2, per, a4, a3, a5, a6, per2, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16_f4, per3, a17,
             a18_f], axis=1).fillna(0)

        list.append(final)

    final1 = list[0]

    final1 = pd.concat([final1, pd.DataFrame(final1.sum(axis=0)).T], axis=0)

    final1.index = final1.index[:-1].tolist() + ['Total']

    final1.loc['Total', 'Total MWBE (%)'] = (final1.loc['Total', 'Total MWBE ($)'] / final1.loc[
        'Total', 'Total ($)'])  # THESE ROWS ARE FOR TOTAL PERCENTAGES in Total Row

    final1.loc['Total', 'MWBE of Total (%)'] = round(
        (final1.loc['Total', 'MWBE Primes ($)']) / (final1.loc['Total', 'Total Primes ($)']), 4)

    final1.loc['Total', 'MWBE Subcontract (%)'] = round(
        (final1.loc['Total', 'MWBE SubContracts ($)']) / (final1.loc['Total', 'Total SubContracts ($)']), 4)

    final1.index.name = 'Agency'

    final1 = final1.fillna(0)

    final1 = final1.sort_index(axis=0)

    summary_list.append(final1)

    ############################
    ## RANKED MAIN SUMMARY TABLE
    ############################

    final1_ranked = final1.sort_values(['Total MWBE (%)'], ascending=False)

    final_ranked_without_t = final1_ranked.ix[final1_ranked.index != 'Total', :]

    final_ranked_table = pd.concat([final_ranked_without_t, pd.DataFrame(final1_ranked.loc['Total', :]).T],
                                   axis=0)

    final_ranked_table.index.name = 'Ranked Agency'

    final_ranked_table = final_ranked_table.fillna(0)

    ranked_summary_list.append(final_ranked_table)

    ################################################

    final2 = list[1]

    final2 = final2.loc[['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']]

    final2 = pd.concat([final2, pd.DataFrame(final2.sum(axis=0)).T], axis=0)

    final2.index = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services', 'Total']

    final2.loc['Total', 'Total MWBE (%)'] = (final2.loc['Total', 'Total MWBE ($)']) / (final2.loc['Total', 'Total ($)'])  # Total Percentage Rows
    final2.loc['Total', 'MWBE of Total (%)'] = (final2.loc['Total', 'MWBE Primes ($)']) / (final2.loc['Total', 'Total Primes ($)'])

    final2.loc['Total', 'MWBE Subcontract (%)'] = (final2.loc['Total', 'MWBE SubContracts ($)']) / (final2.loc['Total', 'Total SubContracts ($)'])

    final2.loc['Construction Services', 'Total MWBE (%)'] = (final2.loc['Construction Services', 'Total MWBE ($)']) / (final2.loc['Construction Services', 'Total ($)'])  # THESE ROWS ARE FOR TOTAL PERCENTAGES

    final2.loc['Construction Services', 'MWBE of Total (%)'] = (float(
        final2.loc['Construction Services', 'MWBE Primes ($)'])) / float(
        final2.loc['Construction Services', 'Total Primes ($)'])
    final2.loc['Construction Services', 'MWBE of Total (%)'] = (float(
        final2.loc['Construction Services', 'MWBE Primes ($)'])) / float(
        final2.loc['Construction Services', 'Total Primes ($)'])

    final2.loc['Construction Services', 'MWBE Subcontract (%)'] = (float(
        final2.loc['Construction Services', 'MWBE SubContracts ($)'])) / (float(
        final2.loc['Construction Services', 'Total SubContracts ($)']))

    final2.loc['Professional Services', 'Total MWBE (%)'] = final2.loc[
                                                                'Professional Services', 'Total MWBE ($)'] / \
                                                            final2.loc[
                                                                'Professional Services', 'Total ($)']  # THESE ROWS ARE FOR TOTAL PERCENTAGES

    final2.loc['Professional Services', 'MWBE of Total (%)'] = (float(
        final2.loc['Professional Services', 'MWBE Primes ($)'])) / (float(
        final2.loc['Professional Services', 'Total Primes ($)']))

    final2.loc['Professional Services', 'MWBE Subcontract (%)'] = float(
        final2.loc['Professional Services', 'MWBE SubContracts ($)']) / (float(
        final2.loc['Professional Services', 'Total SubContracts ($)']))

    final2.index.name = 'Industry'

    final2[u'Total (#)'] = final2[u'Total Primes (#)'].astype(float) + final2[u'Total SubContracts (#)'].astype(
        float) - final2[u'All Subs on All Primes (#)'].astype(float)

    final2[u'Total ($)'] = final2[u'Total Primes ($)'].astype(float) + final2[u'Total SubContracts ($)'].astype(
        float) - final2[u'All Subs on All Primes ($)'].astype(float)

    final2 = final2.fillna(0)

    summary_list.append(final2)
    ranked_summary_list.append(final2)

    ###

    c1 = pw[(pw['MWBE_Status'] == 'MWBE')].groupby(['SizeGroup']).ContractID.nunique()
    c1.name = 'MWBE Primes (#)'
    c2 = pw[(pw['MWBE_Status'] == 'MWBE')].groupby(['SizeGroup']).ContractValue.sum()
    c2.name = 'MWBE Primes ($)'
    c3 = pw.groupby(['SizeGroup']).ContractValue.sum()
    c3.name = 'Total Primes ($)'
    per = c2 / c3.fillna(0)
    per.name = 'MWBE of Total (%)'
    c4 = pw.groupby(['SizeGroup']).ContractID.nunique()
    c4.name = 'Total Primes (#)'
    c5 = pd.Series(['     N/A'] * 3)
    c5.name = 'MWBE SubContracts (#)'
    c6 = pd.Series(['           N/A'] * 3)  # Five Size Groups. <=$20K >$100K, <=$1M >$1M, <=$5M >$20K, <=$100K >$5M, <=$25M
    c6.name = 'MWBE SubContracts ($)'
    c7 = pd.Series(['     N/A'] * len(c4))
    c7.name = 'Total SubContracts (#)'
    c8 = pd.Series(['           N/A'] * len(c4))
    c8.name = 'Total SubContracts ($)'

    per2 = pd.Series(['         N/A'] * len(c4))
    per2.name = 'MWBE Subcontract (%)'

    c9 = pd.Series(['     N/A'] * len(c4))
    c9.name = 'M/WBE Subs on MWBE Primes (#)'
    c10 = pd.Series(['          N/A'] * len(c4))
    c10.name = 'M/WBE Subs on MWBE Primes ($)'
    c11 = pd.Series(['     N/A'] * len(c4))
    c11.name = 'Non MWBE Subs on MWBE Primes (#)'
    c12 = pd.Series(['          N/A'] * len(c4))
    c12.name = 'Non MWBE Subs on MWBE Primes ($)'
    c13 = pd.Series(['     N/A'] * len(c4))
    c13.name = 'All Subs on All Primes (#)'
    c14 = pd.Series(['          N/A'] * len(c4))
    c14.name = 'All Subs on All Primes ($)'

    c15_1 = pw[(pw['MWBE_Status'] == 'MWBE')].groupby(['SizeGroup']).ContractID.count()
    c15_2 = sw[(sw['MWBE_Status'] == 'MWBE')].groupby(['SizeGroup']).SubContractID.count()
    c15_3 = -1 * (
        merged_data_set[(merged_data_set['MWBE_Status_y'] == 'MWBE')].groupby(['SizeGroup_y']).ContractID.count())
    c15_4 = -1 * merged_original[(merged_original['MWBE_Status_y'] == 'Not MWBE') & (
        merged_original['MWBE_Status_x'] == 'MWBE')].groupby(['SizeGroup_y']).ContractID.count()
    c15 = pd.concat([c15_1, c15_2, c15_3, c15_4], axis=1).fillna(0).sum(axis=1)
    c15.name = 'Total MWBE (#)'

    c16_1 = pw[(pw['MWBE_Status'] == 'MWBE')].groupby(['SizeGroup']).ContractValue.sum()
    c16_2 = sw[(sw['MWBE_Status'] == 'MWBE')].groupby(['SizeGroup']).SubValue.sum()
    c16_3 = -1 * merged_original[
        (merged_original['MWBE_Status_y'] == 'MWBE') & (merged_original['MWBE_Status_x'] == 'MWBE')].groupby(
        ['SizeGroup_y']).SubValue.sum()
    c16_4 = -1 * merged_original[
        (merged_original['MWBE_Status_y'] == 'Not MWBE') & (
        merged_original['MWBE_Status_x'] == 'MWBE')].groupby(
        ['SizeGroup_y']).SubValue.sum()
    c16 = pd.concat([c16_1, c16_2, c16_3, c16_4], axis=1).fillna(0).sum(axis=1)
    c16.name = 'Total MWBE ($)'

    c17_1 = pw.groupby(['SizeGroup']).ContractID.count()
    c17_2 = sw.groupby(['SizeGroup']).SizeGroup.count()
    c17_3 = -1 * merged_data_set_total.groupby(['SizeGroup_y']).SizeGroup_y.count()
    c17 = pd.concat([c17_1, c17_2, c17_3], axis=1).fillna(0).sum(axis=1)
    c17.name = 'Total (#)'

    c18_1 = pw.groupby(['SizeGroup']).ContractValue.sum()
    c18_2 = sw.groupby(['SizeGroup']).SubValue.sum()
    c18_3 = -1 * merged_data_set_total.groupby(['SizeGroup_y']).SubValue.sum()
    c18 = pd.concat([c18_1, c18_2, c18_3], axis=1).fillna(0).sum(axis=1)
    c18.name = 'Total ($)'

    per3 = c16 / c18.fillna(0)
    per3.name = 'Total MWBE (%)'

    final3 = pd.concat(
        [c1, c2, per, c4, c3, c5, c6, per2, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, per3, c17, c18],axis=1).fillna(0)

    new_row = final3.loc['>$100K, <=$1M', :] + final3.loc['>$1M, <=$5M', :] + final3.loc['>$5M, <=$25M', :] + final3.loc['>$25M', :]
    new_row.name = 'Over $100K'  # Created the Over $100K Category by Adding together 4 smaller categories
    nr = pd.DataFrame(new_row)
    final3 = pd.concat([final3, nr.T])
    try:
        new_row = final3.loc['Micro Purchase',:]  # + final3.loc['<=$20K', :]  # Added more to the Micro Purchase Category by lumping together the under 20K
    except:
        new_row = final3.loc['Micro Purchase', :]
    new_row.name = 'MicroPurchase'
    nr = pd.DataFrame(new_row)
    final3 = pd.concat([final3, nr.T])
    try:
        new_row = final3.loc['Small Purchase',:]  # + final3.loc['>$20K, <=$100K',:]  # Added more to the Micro Purchase Category by lumping together the under 20K HERE FOR HISTORICAL REASONS ONE DAY WILL DELETE
    except:
        new_row = final3.loc['Small Purchase', :]
    new_row.name = 'SmallPurchase'
    nr = pd.DataFrame(new_row)
    final3 = pd.concat([final3, nr.T])

    total_row = 0

    df3 = final3.loc[['MicroPurchase', 'SmallPurchase', 'Over $100K'], :]

    for i in range(len(df3.index.unique())):
        total_row = total_row + df3.loc[df3.index.unique()[i], :]

    total_row.name = 'Total'
    tr = pd.DataFrame(total_row)
    df3 = pd.concat([df3, tr.T])  # putting on the total row

    df3.index = ['Micro Purchase', 'Small Purchase', 'Over $100K', 'Total']

    df3.loc['Total', 'Total MWBE (%)'] = (float(df3.loc['Total', 'Total MWBE ($)'])) / (
        float(df3.loc['Total', 'Total ($)']))  # THESE ROWS ARE FOR TOTAL PERCENTAGES

    df3.loc['Total', 'MWBE of Total (%)'] = (float(df3.loc['Total', 'MWBE Primes ($)'])) / float(
        df3.loc['Total', 'Total Primes ($)'])

    df3.loc['Total', 'MWBE Subcontract (%)'] = '      N/A'

    for m in ['Micro Purchase', 'Small Purchase', 'Over $100K']:

        try:
            df3.loc[m, 'Total MWBE (%)'] = round((df3.loc[m, 'Total MWBE ($)']) / (df3.loc[m, 'Total ($)']), 2)
        except:
            pass
        df3.loc[m, 'MWBE of Total (%)'] = round(
            float(df3.loc[m, 'MWBE Primes ($)']) / float(df3.loc[m, 'Total Primes ($)']), 2)
        df3.loc[m, 'MWBE Subcontract (%)'] = '      N/A'

    df3.index.name = 'Purchase Size'

    df3 = df3[[u'MWBE Primes (#)', u'MWBE Primes ($)', u'MWBE of Total (%)',
               u'Total Primes (#)', u'Total Primes ($)', u'MWBE SubContracts (#)',
               u'MWBE SubContracts ($)', u'MWBE Subcontract (%)',
               u'Total SubContracts (#)', u'Total SubContracts ($)',
               u'M/WBE Subs on MWBE Primes (#)', u'M/WBE Subs on MWBE Primes ($)',
               u'Non MWBE Subs on MWBE Primes (#)',
               u'Non MWBE Subs on MWBE Primes ($)', u'All Subs on All Primes (#)',
               u'All Subs on All Primes ($)', u'MWBE Primes (#)', u'MWBE Primes ($)', u'MWBE of Total (%)',
               u'Total Primes (#)', u'Total Primes ($)']]

    summary_list.append(df3)
    ranked_summary_list.append(df3)

    ####################################################################################################

    d1 = pw[(pw['MWBE_Status'] == 'MWBE')].groupby(['REG_FQ']).ContractValue.sum()
    d2 = pw[(pw['MWBE_Status'] == 'MWBE')].groupby(['REG_FQ']).ContractID.nunique()
    d1.name = 'MWBE Primes ($)'
    d2.name = 'MWBE Primes (#)'
    d3 = pw.groupby(['REG_FQ']).ContractValue.sum()
    d3.name = 'Total Primes ($)'
    per = d1 / d3.fillna(0)
    per.name = 'MWBE of Total (%)'
    d4 = pw.groupby(['REG_FQ']).ContractID.nunique()
    d4.name = 'Total Primes (#)'
    d5 = sw[(sw['MWBE_Status'] == 'MWBE')].groupby(['FQuarter']).SubValue.sum()
    d5.name = 'MWBE SubContracts ($)'
    d6 = sw[(sw['MWBE_Status'] == 'MWBE')].groupby(['FQuarter']).SubContractID.count()
    d6.name = 'MWBE SubContracts (#)'
    d7 = sw.groupby(['FQuarter']).SubValue.sum()
    d7.name = 'Total SubContracts ($)'
    per2 = d5 / d7.fillna(0)
    per2.name = 'MWBE Subcontract (%)'
    d8 = sw.groupby(['FQuarter']).SubContractID.count()
    d8.name = 'Total SubContracts (#)'
    d9 = merged_data_set[(merged_data_set['MWBE_Status_y'] == 'MWBE')].groupby(['FQuarter']).SubValue.sum()
    d9.name = 'M/WBE Subs on MWBE Primes ($)'
    d10 = merged_data_set[(merged_data_set['MWBE_Status_y'] == 'MWBE')].groupby(
        ['FQuarter']).SubContractID.count()
    d10.name = 'M/WBE Subs on MWBE Primes (#)'
    d11 = merged_original[
        (merged_original['MWBE_Status_y'] == 'Not MWBE') & (
        merged_original['MWBE_Status_x'] == 'MWBE')].groupby(
        ['FQuarter']).SubValue.sum()
    d11.name = 'Non MWBE Subs on MWBE Primes ($)'
    d12 = merged_original[
        (merged_original['MWBE_Status_y'] == 'Not MWBE') & (
        merged_original['MWBE_Status_x'] == 'MWBE')].groupby(
        ['FQuarter']).SubContractID.count()
    d12.name = 'Non MWBE Subs on MWBE Primes (#)'
    d13 = merged_data_set_total.groupby(['FQuarter']).SubValue.sum()
    d13.name = 'All Subs on All Primes ($)'
    d14 = merged_data_set_total.groupby(['FQuarter']).SubContractID.count()
    d14.name = 'All Subs on All Primes (#)'

    d15 = pd.concat([d1.astype(float), d5.astype(float), -1 * d9.astype(float), -1 * d11.astype(float)],
                    axis=1).fillna(
        0).sum(axis=1)
    d15.name = 'Total MWBE ($)'

    d16_f2 = pd.concat([d2.astype(float), d6.astype(float)], axis=1).fillna(0).sum(axis=1)
    d16_f3 = pd.concat([d16_f2, -1 * d10.astype(float)], axis=1).fillna(0).sum(axis=1)
    d16_f4 = pd.concat([d16_f3, -1 * d12.astype(float)], axis=1).fillna(0).sum(axis=1)
    d16_f4.name = 'Total MWBE (#)'

    d17_1 = pw.groupby(['REG_FQ']).ContractValue.sum().astype(float) #not working
    d17_2 = sw.groupby(['FQuarter']).SubValue.sum().astype(float)
    d17 = pd.concat([d17_1, d17_2], axis=1).fillna(0).sum(axis=1)
    d17_3 = -1 * merged_data_set_total.groupby(['FQuarter']).SubValue.sum().astype(float)
    d17_f = pd.concat([d17, d17_3], axis=1).fillna(0).sum(axis=1)
    d17_f.name = 'Total ($)'

    per3 = d15 / d17_f.fillna(0)
    per3.name = 'Total MWBE (%)'

    d18_1 = pw.groupby(['REG_FQ']).ContractID.count() #not working
    d18_2 = sw.groupby(['FQuarter']).SubContractID.count()
    d18_3 = -1 * merged_data_set_total.groupby(['FQuarter']).FQuarter.count()
    d18 = pd.concat([d18_1, d18_2, d18_3], axis=1).fillna(0).sum(axis=1)
    d18.name = 'Total (#)'

    final4 = pd.concat(
        [d2, d1, per, d4, d3, d6, d5, per2, d8, d7, d10, d9, d12, d11, d14, d13, d16_f4, d15, per3, d18, d17_f],
        axis=1).fillna(0)

    total_row = 0

    final4_sum = final4.sum()

    tr = pd.DataFrame(final4_sum)
    final4 = pd.concat([final4, tr.T])

    final4.index = final4.index.tolist()[:-1] + ['Total']

    final4.loc['Total', 'Total MWBE (%)'] = final4.loc['Total', 'Total MWBE ($)'] / final4.loc[
        'Total', 'Total ($)']  # THESE ROWS ARE FOR TOTAL PERCENTAGES

    final4.loc['Total', 'MWBE of Total (%)'] = final4.loc['Total', 'MWBE Primes ($)'] / final4.loc[
        'Total', 'Total Primes ($)']

    final4.loc['Total', 'MWBE Subcontract (%)'] = final4.loc['Total', 'MWBE SubContracts ($)'] / final4.loc[
        'Total', 'Total SubContracts ($)']

    final4.index.name = 'Fiscal Quarter'

    final4 = final4.fillna(0)

    summary_list.append(final4)
    ranked_summary_list.append(final4)

    subs_post_master = subs
    primes_post_master = primes

    filepath = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1PROGFY19Q3\Datasets\Outputs\Masters'

    writer = pd.ExcelWriter(filepath + '\\' + r'FY%s %s LL1 and LL129 Replicate_%s.xlsx' % (str(FY)[2:4], FQ, str(t)), engine='xlsxwriter')

    startrow = 5  # row, col index start at 0
    startcol = 1
    row1 = startrow
    row2 = startrow

    workbook = writer.book

    format1 = workbook.add_format({'bold': True})

    header_format = workbook.add_format({'fg_color': '#D9D9D9', 'bold': 1, 'border': 1})
    header_format.set_align('center')
    header_format.set_align('vcenter')
    header_format.set_text_wrap()

    gridlines = workbook.add_format({'border': 1})
    format2 = workbook.add_format({'num_format': '$###,###,###,##0', 'border': 1})
    percentage_signs_1 = workbook.add_format({'num_format': '0%'})
    percentage_signs_2 = workbook.add_format({'num_format': '0.00%'})
    center = workbook.add_format()
    center.set_align('center')
    center.set_align('vcenter')

    for i in summary_list:
        i.to_excel(writer, sheet_name='Summary', startrow=row1, startcol=startcol, header=False,
                   index=True)  # is it possible to put gridlines around the cells at this point?
        worksheet = writer.sheets['Summary']
        worksheet.write('C' + str(row1), '#', header_format)
        worksheet.write('D' + str(row1), '$', header_format)
        worksheet.write('E' + str(row1), '%', header_format)
        worksheet.write('F' + str(row1), '#', header_format)
        worksheet.write('G' + str(row1), '$', header_format)
        worksheet.write('H' + str(row1), '#', header_format)
        worksheet.write('I' + str(row1), '$', header_format)
        worksheet.write('J' + str(row1), '%', header_format)
        worksheet.write('K' + str(row1), '#', header_format)
        worksheet.write('L' + str(row1), '$', header_format)
        worksheet.write('M' + str(row1), '#', header_format)
        worksheet.write('N' + str(row1), '$', header_format)
        worksheet.write('O' + str(row1), '#', header_format)
        worksheet.write('P' + str(row1), '$', header_format)
        worksheet.write('Q' + str(row1), '#', header_format)
        worksheet.write('R' + str(row1), '$', header_format)
        worksheet.write('S' + str(row1), '#', header_format)
        worksheet.write('T' + str(row1), '$', header_format)
        worksheet.write('U' + str(row1), '%', header_format)
        worksheet.write('V' + str(row1), '#', header_format)
        worksheet.write('W' + str(row1), '$', header_format)
        worksheet.conditional_format(xl_range(row1, startcol, row1 + i.shape[0] - 1, startcol + i.shape[1] - 1),
                                     {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': gridlines})
        worksheet.set_row(row1 - 2, 30)
        worksheet.merge_range('B' + str(row1 - 1) + ':' + 'B' + str(row1), i.index.name, header_format)
        worksheet.merge_range('C' + str(row1 - 1) + ':' + 'E' + str(row1 - 1), 'M/WBE Primes', header_format)
        worksheet.merge_range('F' + str(row1 - 1) + ':' + 'G' + str(row1 - 1), 'Total Primes', header_format)
        worksheet.merge_range('H' + str(row1 - 1) + ':' + 'J' + str(row1 - 1), 'M/WBE Subcontracts',
                              header_format)
        worksheet.merge_range('K' + str(row1 - 1) + ':' + 'L' + str(row1 - 1), 'Total Subcontracts',
                              header_format)
        worksheet.merge_range('M' + str(row1 - 1) + ':' + 'N' + str(row1 - 1), 'M/WBE Subs \non MWBE Primes',
                              header_format)
        worksheet.merge_range('O' + str(row1 - 1) + ':' + 'P' + str(row1 - 1),
                              'Non-M/WBE Subs \nfrom M/WBE Primes', header_format)
        worksheet.merge_range('Q' + str(row1 - 1) + ':' + 'R' + str(row1 - 1), 'All Subs on \nAll Primes',
                              header_format)
        worksheet.merge_range('S' + str(row1 - 1) + ':' + 'U' + str(row1 - 1), 'Total MWBE', header_format)
        worksheet.merge_range('V' + str(row1 - 1) + ':' + 'W' + str(row1 - 1), 'Total', header_format)
        worksheet.conditional_format('E' + str(row1) + ':' + 'E' + str(row1 + i.shape[0] - 1),
                                     {'type': 'no_blanks', 'format': percentage_signs_1})
        worksheet.conditional_format('U' + str(row1) + ':' + 'U' + str(row1 + i.shape[0] - 1),
                                     {'type': 'no_blanks', 'format': percentage_signs_1})
        worksheet.conditional_format('J' + str(row1) + ':' + 'J' + str(row1 + i.shape[0] - 1),
                                     {'type': 'no_blanks', 'format': percentage_signs_1})
        worksheet.conditional_format('E' + str(row1 + i.shape[0]),
                                     {'type': 'no_blanks', 'format': percentage_signs_2})
        worksheet.conditional_format('U' + str(row1 + i.shape[0]),
                                     {'type': 'no_blanks', 'format': percentage_signs_2})
        worksheet.conditional_format('J' + str(row1 + i.shape[0]),
                                     {'type': 'no_blanks', 'format': percentage_signs_2})
        row1 += (i.shape[0] + 3)

    worksheet.write('B2', 'Combined Utilization FY' + str(FY)[2:4] + ' ' + str(FQ), format1)
    worksheet.set_column('B:B', 23)

    row3 = startrow

    for i in summary_list:
        for h, j in [['D', 'MWBE Primes ($)'], ['G', 'Total Primes ($)'], ['I', 'MWBE SubContracts ($)'],
                     ['L', 'Total SubContracts ($)'], ['N', 'M/WBE Subs on MWBE Primes ($)'],
                     ['P', 'Non MWBE Subs on MWBE Primes ($)'], ['R', 'All Subs on All Primes ($)'],
                     ['T', 'MWBE Primes ($)'], ['W', 'Total Primes ($)']]:
            try:
                worksheet.set_column(h + str(row3 + 1) + ':' + h + str(row3 + i.shape[0] - 1),
                                     len(str(int(i[j].max()))) + 4.5)
            except:
                pass
            worksheet.conditional_format(h + str(row3 + 1) + ':' + h + str(row3 + i.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': format2})
        row3 += (i.shape[0] + 3)  # this loops through all three dataframes.

    if pw.Agency.nunique() == 35:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['53', '54', '55', '56']:
                worksheet.write(x + y, 'N/A', center)
    elif pw.Agency.nunique() == 34:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['52', '53', '54', '55']:
                worksheet.write(x + y, 'N/A', center)
    elif pw.Agency.nunique() == 33:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['51', '52', '53', '54']:
                worksheet.write(x + y, 'N/A', center)
    elif pw.Agency.nunique() == 32:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['50', '51', '52', '53']:
                worksheet.write(x + y, 'N/A', center)

    startrow = 5
    startcol = 1
    row1 = startrow
    row2 = startrow

    for i in ranked_summary_list:
        i.to_excel(writer, sheet_name='Ranked Summary', startrow=row1, startcol=startcol, header=False,
                   index=True)  # is it possible to put gridlines around the cells at this point?
        worksheet = writer.sheets['Ranked Summary']
        worksheet.write('C' + str(row1), '#', header_format)
        worksheet.write('D' + str(row1), '$', header_format)
        worksheet.write('E' + str(row1), '%', header_format)
        worksheet.write('F' + str(row1), '#', header_format)
        worksheet.write('G' + str(row1), '$', header_format)
        worksheet.write('H' + str(row1), '#', header_format)
        worksheet.write('I' + str(row1), '$', header_format)
        worksheet.write('J' + str(row1), '%', header_format)
        worksheet.write('K' + str(row1), '#', header_format)
        worksheet.write('L' + str(row1), '$', header_format)
        worksheet.write('M' + str(row1), '#', header_format)
        worksheet.write('N' + str(row1), '$', header_format)
        worksheet.write('O' + str(row1), '#', header_format)
        worksheet.write('P' + str(row1), '$', header_format)
        worksheet.write('Q' + str(row1), '#', header_format)
        worksheet.write('R' + str(row1), '$', header_format)
        worksheet.write('S' + str(row1), '#', header_format)
        worksheet.write('T' + str(row1), '$', header_format)
        worksheet.write('U' + str(row1), '%', header_format)
        worksheet.write('V' + str(row1), '#', header_format)
        worksheet.write('W' + str(row1), '$', header_format)
        worksheet.conditional_format(xl_range(row1, startcol, row1 + i.shape[0] - 1, startcol + i.shape[1] - 1),
                                     {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': gridlines})
        worksheet.set_row(row1 - 2, 30)
        worksheet.merge_range('B' + str(row1 - 1) + ':' + 'B' + str(row1), i.index.name, header_format)
        worksheet.merge_range('C' + str(row1 - 1) + ':' + 'E' + str(row1 - 1), 'M/WBE Primes', header_format)
        worksheet.merge_range('F' + str(row1 - 1) + ':' + 'G' + str(row1 - 1), 'Total Primes', header_format)
        worksheet.merge_range('H' + str(row1 - 1) + ':' + 'J' + str(row1 - 1), 'M/WBE Subcontracts',
                              header_format)
        worksheet.merge_range('K' + str(row1 - 1) + ':' + 'L' + str(row1 - 1), 'Total Subcontracts',
                              header_format)
        worksheet.merge_range('M' + str(row1 - 1) + ':' + 'N' + str(row1 - 1), 'M/WBE Subs \non MWBE Primes',
                              header_format)
        worksheet.merge_range('O' + str(row1 - 1) + ':' + 'P' + str(row1 - 1),
                              'Non-M/WBE Subs \nfrom M/WBE Primes',
                              header_format)
        worksheet.merge_range('Q' + str(row1 - 1) + ':' + 'R' + str(row1 - 1), 'All Subs on \nAll Primes',
                              header_format)
        worksheet.merge_range('S' + str(row1 - 1) + ':' + 'U' + str(row1 - 1), 'Total MWBE', header_format)
        worksheet.merge_range('V' + str(row1 - 1) + ':' + 'W' + str(row1 - 1), 'Total', header_format)
        worksheet.conditional_format('U' + str(row1) + ':' + 'U' + str(row1 + i.shape[0] - 1),
                                     {'type': 'no_blanks', 'format': percentage_signs_1})
        worksheet.conditional_format('E' + str(row1) + ':' + 'E' + str(row1 + i.shape[0] - 1),
                                     {'type': 'no_blanks', 'format': percentage_signs_1})
        worksheet.conditional_format('J' + str(row1) + ':' + 'J' + str(row1 + i.shape[0] - 1),
                                     {'type': 'no_blanks', 'format': percentage_signs_1})
        worksheet.conditional_format('U' + str(row1 + i.shape[0]),
                                     {'type': 'no_blanks', 'format': percentage_signs_2})
        worksheet.conditional_format('E' + str(row1 + i.shape[0]),
                                     {'type': 'no_blanks', 'format': percentage_signs_2})
        worksheet.conditional_format('J' + str(row1 + i.shape[0]),
                                     {'type': 'no_blanks', 'format': percentage_signs_2})
        row1 += (i.shape[0] + 3)

    worksheet.write('B2', 'Combined Utilization FY' + str(FY)[2:4] + ' ' + str(FQ), format1)

    worksheet.set_column('B:B', 23)

    row3 = startrow

    for i in ranked_summary_list:
        for h, j in [['D', 'MWBE Primes ($)'], ['G', 'Total Primes ($)'], ['I', 'MWBE SubContracts ($)'],
                     ['L', 'Total SubContracts ($)'], ['N', 'M/WBE Subs on MWBE Primes ($)'],
                     ['P', 'Non MWBE Subs on MWBE Primes ($)'], ['R', 'All Subs on All Primes ($)'],
                     ['T', 'Total MWBE ($)'], ['W', 'Total ($)']]:
            try:
                worksheet.set_column(h + str(row3 + 1) + ':' + h + str(row3 + i.shape[0] - 1),
                                     len(str(int(i[j].max()))) + 4.5)
            except:
                pass
                # worksheet.set_column(h + str(row3 + 1) + ':' + h + str(row3 + i.shape[0] - 1), i[j].map(lambda x: len(x)).max() + 6, center)
            worksheet.conditional_format(h + str(row3 + 1) + ':' + h + str(row3 + i.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': format2})
        row3 += (i.shape[0] + 3)

    if pw.Agency.nunique() == 35:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['53', '54', '55', '56']:
                worksheet.write(x + y, 'N/A', center)
    elif pw.Agency.nunique() == 36:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['54', '55', '56', '57']:
                worksheet.write(x + y, 'N/A', center)
    elif pw.Agency.nunique() == 34:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['52', '53', '54', '55']:
                worksheet.write(x + y, 'N/A', center)
    elif pw.Agency.nunique() == 33:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['51', '52', '53', '54']:
                worksheet.write(x + y, 'N/A', center)
    elif pw.Agency.nunique() == 32:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['50', '51', '52', '53']:
                worksheet.write(x + y, 'N/A', center)

    alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
                'T', 'U',
                'V', 'W', 'X', 'Y', 'Z', 'AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK', 'AL',
                'AM',
                'AN', 'AO', 'AP', 'AQ', 'AR', 'AS', 'AT', 'AU', 'AV', 'AW', 'AX', 'AY', 'AZ', 'BA', 'BB', 'BC',
                'BD',
                'BE', 'BF']

    startrow = 4
    startcol = 1
    row1 = startrow

    for i in sorted(pw.Agency.unique().tolist()):
        primes_ = primes[primes['Agency'] == i]
        subs_ = subs[subs['Agency'] == i]
        pw_ = pw[pw['Agency'] == i]
        sw_ = sw[sw['Agency'] == i]
        pwm = pw_[(pw_['MWBE_Status'] == 'MWBE')]
        mds = pd.merge(pwm, sw_, on='ContractID')
        mo = pd.merge(primes_, subs_, on='ContractID')
        mdst = pd.merge(pw_, sw_, on='ContractID')

        b1 = round(pw_[(pw_['MWBE_Status'] == 'MWBE')].ContractValue.sum(),
                   0)  # how to round all values in a df
        b2 = pw_[(pw_['MWBE_Status'] == 'MWBE')].ContractID.nunique()
        b3 = round(pw_.ContractValue.sum(), 0)
        b4 = pw_.ContractID.nunique()
        b5 = round(sw_[(sw_['MWBE_Status'] == 'MWBE')].SubValue.sum(), 0)
        b6 = sw_[(sw_['MWBE_Status'] == 'MWBE')].index.nunique()
        b7 = round(sw_.SubValue.sum(), 0)
        b8 = sw_.index.nunique()
        b9 = round(mds[(mds['MWBE_Status_y'] == 'MWBE')].SubValue.sum(), 0)
        b10 = mds[(mds['MWBE_Status_y'] == 'MWBE')].index.nunique()
        b11 = round(mo[(mo['MWBE_Status_y'] == 'Not MWBE') & (mo['MWBE_Status_x'] == 'MWBE')].SubValue.sum(), 0)
        b12 = mo[(mo['MWBE_Status_y'] == 'Not MWBE') & (mo['MWBE_Status_x'] == 'MWBE')].index.nunique()
        b13 = round(mdst.SubValue.sum(), 0)
        b14 = mdst.index.nunique()

        counts = []
        counts.append([b2, b6, b10, b12, b4, b8, b14])
        counts_df = pd.DataFrame(counts,
                                 columns=['MWBE Primes', 'MWBE SubContracts', 'M/WBE Subs on MWBE Primes',
                                          'Non MWBE Subs on MWBE Primes', 'Total Primes', 'Total SubContracts',
                                          'All Subs on All Primes'])

        values = []
        values.append([b1, b5, b9, b11, b3, b7, b13])
        values_df = pd.DataFrame(values,
                                 columns=['MWBE Primes', 'MWBE SubContracts', 'M/WBE Subs on MWBE Primes',
                                          'Non MWBE Subs on MWBE Primes', 'Total Primes', 'Total SubContracts',
                                          'All Subs on All Primes'])

        df = pd.concat([counts_df.T, values_df.T], axis=1)
        df.columns = ['Counts', 'Values']

        sum1 = df.loc[['MWBE Primes', 'MWBE SubContracts'], :].sum() - df.loc[['M/WBE Subs on MWBE Primes',
                                                                               'Non MWBE Subs on MWBE Primes'],
                                                                       :].sum()
        sum2 = df.loc[['Total Primes', 'Total SubContracts']].sum() - df.loc[['All Subs on All Primes']].sum()

        df.loc['Total MWBE', 'Counts'] = sum1.Counts
        df.loc['Total MWBE', 'Values'] = sum1.Values

        df.loc['Total Contracts', 'Counts'] = sum2.Counts
        df.loc['Total Contracts', 'Values'] = sum2.Values

        df.loc['Final M/WBE Utilization Rate', 'Values'] = str((int(sum1.Values) / int(sum2.Values)) * 100)[0:5] + ' %'

        df_final = df.ix[['MWBE Primes',
                          'MWBE SubContracts',
                          'M/WBE Subs on MWBE Primes',
                          'Non MWBE Subs on MWBE Primes',
                          'Total MWBE',
                          'Total Primes',
                          'Total SubContracts',
                          'All Subs on All Primes',
                          'Total Contracts',
                          'Final M/WBE Utilization Rate'], :]

        df_final.index.name = 'Type of Contract'

        df_final.to_excel(writer, sheet_name='Agency Breakdown', startcol=startcol, startrow=row1, index=True,
                          header=True)

        workbook = writer.book

        worksheet = writer.sheets['Agency Breakdown']

        worksheet.set_column('B:B', 36.5)
        worksheet.set_column('C:C', 8)
        worksheet.set_column('D:D', 23.5)

        merge_format = workbook.add_format(
            {'bold': 1, 'align': 'center', 'valign': 'vcenter', 'border': 2, 'fg_color': '#BFBFBF'})
        merge_format2 = workbook.add_format({'bold': 1, 'align': 'center', 'valign': 'vcenter', 'border': 2})
        header = workbook.add_format({'bold': 1, 'align': 'center', 'valign': 'vcenter', 'border': 2})
        header2 = workbook.add_format({'bold': 1, 'bg_color': '#BFBFBF'})
        header3 = workbook.add_format({'bold': 1, 'bg_color': '#D8E4BC'})
        header4 = workbook.add_format({'bold': 1, 'bg_color': '#FABF8F'})
        gridlines = workbook.add_format({'border': 2})
        bold = workbook.add_format({'bold': 1})
        dollar_signs = workbook.add_format({'num_format': '$###,###,###,##0', 'border': 2})
        percentage_signs = workbook.add_format({'num_format': '0.00%'})

        worksheet.write('B' + str(row1 - 1), str(i), bold)
        worksheet.merge_range('B' + str(row1) + ':D' + str(row1),
                              'FY %s Local Law 1 and Local Law 129 M/WBE Utilization Primes and Subs' % (str(FY)),
                              merge_format)
        worksheet.conditional_format('B' + str(row1 + 1) + ':' + 'D' + str(row1 + 1),
                                     {'type': 'no_blanks', 'format': header2})
        worksheet.conditional_format('B' + str(row1 + 1) + ':' + 'B' + str(row1 + df_final.shape[0] + 1),
                                     {'type': 'no_blanks', 'format': gridlines})
        worksheet.conditional_format('C' + str(row1 + 1) + ':' + 'D' + str(row1 + 1 + df_final.shape[0]),
                                     {'type': 'no_blanks', 'format': header})
        worksheet.conditional_format('C' + str(row1 + 1) + ':' + 'D' + str(row1 + 1 + df_final.shape[0]),
                                     {'type': 'no_blanks', 'format': header})
        worksheet.conditional_format('C' + str(row1 + 2) + ':' + 'C' + str(row1 + df_final.shape[0]),
                                     {'type': 'no_blanks', 'format': gridlines})
        worksheet.conditional_format('D' + str(row1 + 2) + ':' + 'D' + str(row1 + df_final.shape[0]),
                                     {'type': 'no_blanks', 'format': dollar_signs})
        worksheet.conditional_format('U' + str(row1) + ':' + 'U' + str(row1 + df_final.shape[0]),
                                     {'type': 'no_blanks', 'format': percentage_signs})
        worksheet.conditional_format(
            'B' + str(row1 + df_final.shape[0] + 1) + ':' + 'D' + str(row1 + df_final.shape[0] + 1),
            {'type': 'no_blanks', 'format': header2})

        worksheet.conditional_format('B' + str(row1 + 2), {'type': 'no_blanks', 'format': header3})
        worksheet.conditional_format('B' + str(row1 + 3), {'type': 'no_blanks', 'format': header3})
        worksheet.conditional_format('B' + str(row1 + 4), {'type': 'no_blanks', 'format': header4})
        worksheet.conditional_format('B' + str(row1 + 5), {'type': 'no_blanks', 'format': header4})
        worksheet.conditional_format('B' + str(row1 + 7), {'type': 'no_blanks', 'format': header3})
        worksheet.conditional_format('B' + str(row1 + 8), {'type': 'no_blanks', 'format': header3})
        worksheet.conditional_format('B' + str(row1 + 9), {'type': 'no_blanks', 'format': header4})

        row1 += df_final.shape[0]

        worksheet.merge_range('C' + str(row1 + 1) + ':D' + str(row1 + 1), df.loc['Final M/WBE Utilization Rate', 'Values'], merge_format2)

        row1 += 4

    startrow = 0
    startcol = 0

    primes.to_excel(writer, sheet_name='Primes', startrow=startrow, startcol=startcol, header=True,
                    index=False)  # is it possible to put gridlines around the cells at this point?

    worksheet1 = writer.sheets['Primes']

    for a in range(primes.shape[1]):
        worksheet1.set_column(alphabet[a] + ':' + alphabet[a],
                              max(primes.iloc[:, a].map(str).map(len).max() + 4,
                                  len(primes.columns.tolist()[a]) + 4))

    startrow = 0
    startcol = 0

    sub_util.to_excel(writer, sheet_name='Subs', startrow=startrow, startcol=startcol, header=True,
                      index=False)

    worksheet2 = writer.sheets['Subs']

    for b in range(subs.shape[1]):
        worksheet2.set_column(alphabet[b] + ':' + alphabet[b],
                              max(subs.iloc[:, b].map(str).map(len).max() + 4,
                                  len(subs.columns.tolist()[b]) + 4))

    #################

    startrow = 0
    startcol = 0
    row1 = startrow

    MWBESubs_onMWBEPrimes.to_excel(writer, sheet_name='MWBE on MWBE', startcol=startcol, startrow=row1,
                                   index=False,
                                   header=True)

    worksheet3 = writer.sheets['MWBE on MWBE']
    text = 'Key: \n \n _x variables from primes data. \n \n _y variables from subs data.'
    options = {'width': 280, 'height': 130, 'fill': {'color': '#DAEEF7'}}
    worksheet3.insert_textbox(2, 1, text, options)

    for c in range(MWBESubs_onMWBEPrimes.shape[1]):
        worksheet3.set_column(alphabet[c] + ':' + alphabet[c],
                              max(MWBESubs_onMWBEPrimes.iloc[:, c].map(str).map(len).max() + 4,
                                  len(MWBESubs_onMWBEPrimes.columns.tolist()[c]) + 4))

    startrow = 0
    startcol = 0
    row1 = startrow

    NonMWBESubs_onMWBEPrimes.to_excel(writer, sheet_name='NonMWBE on MWBE', startcol=startcol, startrow=row1,
                                      index=False, header=True)

    worksheet4 = writer.sheets['NonMWBE on MWBE']
    text = 'Key: \n \n _x variables from primes data. \n \n _y variables from subs data.'
    options = {'width': 280, 'height': 130, 'fill': {'color': '#DAEEF7'}}
    worksheet4.insert_textbox(2, 1, text, options)

    for d in range(NonMWBESubs_onMWBEPrimes.shape[1]):
        worksheet4.set_column(alphabet[d] + ':' + alphabet[d],
                              max(NonMWBESubs_onMWBEPrimes.iloc[:, d].map(str).map(len).max() + 4,
                                  len(NonMWBESubs_onMWBEPrimes.columns.tolist()[d]) + 4))

    startrow = 0
    startcol = 0
    row1 = startrow

    merged_data_set_total = merged_data_set_total[~merged_data_set_total['SubValue'].isnull()]

    merged_data_set_total.to_excel(writer, sheet_name='Sub on Prime', startcol=startcol, startrow=row1,
                                   index=False,
                                   header=True)

    worksheet5 = writer.sheets['Sub on Prime']
    text = 'Key: \n \n _x variables from primes data. \n \n _y variables from subs data.'
    options = {'width': 280, 'height': 130, 'fill': {'color': '#DAEEF7'}}
    worksheet5.insert_textbox(2, 1, text, options)

    for e in range(merged_data_set_total.shape[1]):
        worksheet5.set_column(alphabet[e] + ':' + alphabet[e],
                              max(merged_data_set_total.iloc[:, e].map(str).map(len).max() + 4,
                                  len(merged_data_set_total.columns.tolist()[e]) + 4))

    writer.save()

    # filepath = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\01 - Reporting\01 - LL1 and LL129\==Data==\FY%s\FY%s %s' % (
    #     str(FY)[2:4], str(FY)[2:4], FQ)
    # try:
    #     os.mkdir(filepath)
    # except:
    #     pass

    # sh.move(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\FY18 MWBE Prog Files\Scripts\Optimized\LL1 Reporting - PRODUCTION\FY%s %s LL1 and LL129 Replicate_%s_.xlsx' % (
    #         str(FY)[2:4], FQ, str(today.date())), filepath + '\FY%s %s LL1 and LL129 Replicate_%s_.xlsx' % (
    #         str(FY)[2:4], FQ, str(today.date())))

    writer = pd.ExcelWriter(filepath + '//' + r'FY%s %s LL1 and LL129 Replicate OMWBE_%s.xlsx' % (str(FY)[2:4], FQ, str(t)), engine='xlsxwriter')
    startrow = 5  # row, col index start at 0
    startcol = 1
    row2 = startrow
    row1 = startrow

    workbook = writer.book

    format1 = workbook.add_format({'bold': True})

    header_format = workbook.add_format({'fg_color': '#D9D9D9', 'bold': 1, 'border': 1})
    header_format.set_align('center')
    header_format.set_align('vcenter')
    header_format.set_text_wrap()

    gridlines = workbook.add_format({'border': 1})
    format2 = workbook.add_format({'num_format': '$###,###,###,##0', 'border': 1})
    percentage_signs_1 = workbook.add_format({'num_format': '0%'})
    percentage_signs_2 = workbook.add_format({'num_format': '0.00%'})
    center = workbook.add_format()
    center.set_align('center')
    center.set_align('vcenter')

    for i in summary_list:
        i.to_excel(writer, sheet_name='1. Summary', startrow=row1, startcol=startcol, header=False,
                   index=True)  # is it possible to put gridlines around the cells at this point?
        worksheet = writer.sheets['1. Summary']
        worksheet.write('C' + str(row1), '#', header_format)
        worksheet.write('D' + str(row1), '$', header_format)
        worksheet.write('E' + str(row1), '%', header_format)
        worksheet.write('F' + str(row1), '#', header_format)
        worksheet.write('G' + str(row1), '$', header_format)
        worksheet.write('H' + str(row1), '#', header_format)
        worksheet.write('I' + str(row1), '$', header_format)
        worksheet.write('J' + str(row1), '%', header_format)
        worksheet.write('K' + str(row1), '#', header_format)
        worksheet.write('L' + str(row1), '$', header_format)
        worksheet.write('M' + str(row1), '#', header_format)
        worksheet.write('N' + str(row1), '$', header_format)
        worksheet.write('O' + str(row1), '#', header_format)
        worksheet.write('P' + str(row1), '$', header_format)
        worksheet.write('Q' + str(row1), '#', header_format)
        worksheet.write('R' + str(row1), '$', header_format)
        worksheet.write('S' + str(row1), '#', header_format)
        worksheet.write('T' + str(row1), '$', header_format)
        worksheet.write('U' + str(row1), '%', header_format)
        worksheet.write('V' + str(row1), '#', header_format)
        worksheet.write('W' + str(row1), '$', header_format)
        worksheet.conditional_format(xl_range(row1, startcol, row1 + i.shape[0] - 1, startcol + i.shape[1] - 1),
                                     {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': gridlines})
        worksheet.set_row(row1 - 2, 30)
        worksheet.merge_range('B' + str(row1 - 1) + ':' + 'B' + str(row1), i.index.name, header_format)
        worksheet.merge_range('C' + str(row1 - 1) + ':' + 'E' + str(row1 - 1), 'M/WBE Primes', header_format)
        worksheet.merge_range('F' + str(row1 - 1) + ':' + 'G' + str(row1 - 1), 'Total Primes', header_format)
        worksheet.merge_range('H' + str(row1 - 1) + ':' + 'J' + str(row1 - 1), 'M/WBE Subcontracts', header_format)
        worksheet.merge_range('K' + str(row1 - 1) + ':' + 'L' + str(row1 - 1), 'Total Subcontracts', header_format)
        worksheet.merge_range('M' + str(row1 - 1) + ':' + 'N' + str(row1 - 1), 'M/WBE Subs \non MWBE Primes',
                              header_format)
        worksheet.merge_range('O' + str(row1 - 1) + ':' + 'P' + str(row1 - 1),
                              'Non-M/WBE Subs \nfrom M/WBE Primes', header_format)
        worksheet.merge_range('Q' + str(row1 - 1) + ':' + 'R' + str(row1 - 1), 'All Subs on \nAll Primes',
                              header_format)
        worksheet.merge_range('S' + str(row1 - 1) + ':' + 'U' + str(row1 - 1), 'Total MWBE', header_format)
        worksheet.merge_range('V' + str(row1 - 1) + ':' + 'W' + str(row1 - 1), 'Total', header_format)
        worksheet.conditional_format('E' + str(row1) + ':' + 'E' + str(row1 + i.shape[0] - 1),
                                     {'type': 'no_blanks', 'format': percentage_signs_1})
        worksheet.conditional_format('U' + str(row1) + ':' + 'U' + str(row1 + i.shape[0] - 1),
                                     {'type': 'no_blanks', 'format': percentage_signs_1})
        worksheet.conditional_format('J' + str(row1) + ':' + 'J' + str(row1 + i.shape[0] - 1),
                                     {'type': 'no_blanks', 'format': percentage_signs_1})
        worksheet.conditional_format('E' + str(row1 + i.shape[0]), {'type': 'no_blanks', 'format': percentage_signs_2})
        worksheet.conditional_format('U' + str(row1 + i.shape[0]), {'type': 'no_blanks', 'format': percentage_signs_2})
        worksheet.conditional_format('J' + str(row1 + i.shape[0]), {'type': 'no_blanks', 'format': percentage_signs_2})
        row1 += (i.shape[0] + 3)

    worksheet.write('B2', 'Combined Utilization FY' + str(FY)[2:4] + ' ' + str(FQ), format1)

    worksheet.set_column('B:B', 23)

    row3 = startrow

    for i in summary_list:
        for h, j in [['D', 'MWBE Primes ($)'], ['G', 'Total Primes ($)'], ['I', 'MWBE SubContracts ($)'],
                     ['L', 'Total SubContracts ($)'], ['N', 'M/WBE Subs on MWBE Primes ($)'],
                     ['P', 'Non MWBE Subs on MWBE Primes ($)'], ['R', 'All Subs on All Primes ($)'],
                     ['T', 'MWBE Primes ($)'], ['W', 'Total Primes ($)']]:
            try:
                worksheet.set_column(h + str(row3 + 1) + ':' + h + str(row3 + i.shape[0] - 1),
                                     len(str(int(i[j].max()))) + 4.5)
            except:
                pass
            worksheet.conditional_format(h + str(row3 + 1) + ':' + h + str(row3 + i.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': format2})
        row3 += (i.shape[0] + 3)  # this loops through all three dataframes.

    if pw.Agency.nunique() == 35:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['53', '54', '55', '56']:
                worksheet.write(x + y, 'N/A', center)
    elif pw.Agency.nunique() == 34:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['52', '53', '54', '55']:
                worksheet.write(x + y, 'N/A', center)
    elif pw.Agency.nunique() == 33:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['51', '52', '53', '54']:
                worksheet.write(x + y, 'N/A', center)
    elif pw.Agency.nunique() == 32:
        for x in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']:
            for y in ['50', '51', '52', '53']:
                worksheet.write(x + y, 'N/A', center)

    startrow = 0
    startcol = 0

    primes.columns = primes.columns = ['Agency', 'DOC_CD', 'DOC_DEPT_CD', 'DOC_ID', 'ContractID', 'EPIN',
                                       'RegistrationDate', 'ContractValue', 'Industry', 'SizeGroup', 'MWBE_Status',
                                       'ReportCategory', 'EthGen',
                                       'MWBE_LL',
                                       'Method',
                                       'VendorName',
                                       'VendorTIN',
                                       'VendorNumber',
                                       'Purpose',
                                       'StartDate',
                                       'EndDate',
                                       'REG_FQ',
                                       'REG_FY']

    primes.to_excel(writer, sheet_name='2. Primes Data', startrow=startrow, startcol=startcol, header=True,
                    index=False)  # is it possible to put gridlines around the cells at this point?

    worksheet1 = writer.sheets['2. Primes Data']

    alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
                'V', 'W', 'X', 'Y', 'Z', 'AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK', 'AL', 'AM',
                'AN', 'AO', 'AP', 'AQ', 'AR', 'AS', 'AT', 'AU', 'AV', 'AW', 'AX', 'AY', 'AZ', 'BA', 'BB', 'BC', 'BD',
                'BE', 'BF']

    for a in range(primes.shape[1]):
        worksheet1.set_column(alphabet[a] + ':' + alphabet[a],
                              max(primes.iloc[:, a].map(str).map(len).max() + 4, len(primes.columns.tolist()[a]) + 4))

    startrow = 0
    startcol = 0

    subs = subs[['Agency', 'ContractID', 'Method', 'PrimeIndustry', 'ContractValue', 'SubValue', 'SubStartDate',
                 'ReportCategory', 'EthGen', 'RegistrationDate', 'SubVendorName', 'SubVendorNumber', 'SubDescr',
                 'MWBE_Status', 'FQuarter', 'SubIndustry']]
    subs.columns = ['Agency', 'ContractID', 'Method', 'Industry', 'PrimeContractValue', 'SubValue', 'SubStartDate',
                    'ReportCategory', 'EthGen', 'RegistrationDate', 'SubVendorName', 'SubVendorNumber', 'SubDescr',
                    'MWBE_Status', 'FQuarter', 'SubIndustry']

    subs.to_excel(writer, sheet_name='3. Subs Data', startrow=startrow, startcol=startcol, header=True,
                  index=False)

    worksheet2 = writer.sheets['3. Subs Data']

    for b in range(subs.shape[1]):
        worksheet2.set_column(alphabet[b] + ':' + alphabet[b],
                              max(subs.iloc[:, b].map(str).map(len).max() + 4, len(subs.columns.tolist()[b]) + 4))

    worksheet3 = workbook.add_worksheet('4. Data Dictionary')

    large = workbook.add_format({'font_name': 'Calibri', 'font_size': '16'})
    italics = workbook.add_format({'font_name': 'Calibri', 'font_size': '11'})
    italics.set_align('vcenter')
    italics.set_align('center')
    italics.set_italic()
    reg = workbook.add_format({'bold': 0, 'font_name': 'Calibri', 'font_size': '11'})
    reg_bold = workbook.add_format({'bold': 1, 'font_name': 'Calibri', 'font_size': '11'})
    reg_bold.set_align('vcenter')
    reg_bold.set_align('center')

    worksheet3.set_column('B:B', 21)
    worksheet3.set_column('C:C', 58)
    worksheet3.set_column('D:D', 18)
    worksheet3.set_column('E:E', 18)
    worksheet3.set_column('F:F', 28)
    worksheet3.set_column('G:G', 59)
    worksheet3.set_column('H:H', 18)

    worksheet3.write('B2', 'Primes Data Dictionary', large)
    worksheet3.write('F2', 'Subs Data Dictionary', large)
    worksheet3.write('B3', 'Field', italics)
    worksheet3.write('C3', 'Description', italics)
    worksheet3.write('D3', 'Source', italics)
    worksheet3.write('F3', 'Field', italics)
    worksheet3.write('G3', 'Description', italics)
    worksheet3.write('H3', 'Source', italics)

    worksheet3.write('F4', 'Agency', reg_bold)
    worksheet3.write('F5', 'ContractID', reg_bold)
    worksheet3.write('F6', 'Method', reg_bold)
    worksheet3.write('F7', 'PrimeIndustry', reg_bold)
    worksheet3.write('F8', 'PrimeContractValue', reg_bold)
    worksheet3.write('F9', 'SubValue', reg_bold)
    worksheet3.write('F10', 'SubStartDate', reg_bold)
    worksheet3.write('F11', 'ReportCategory', reg_bold)
    worksheet3.write('F12', 'RegistrationDate', reg_bold)
    worksheet3.write('F13', 'SubVendorName', reg_bold)
    worksheet3.write('F14', 'SubVendorNumber', reg_bold)
    worksheet3.write('F15', 'SubDescr', reg_bold)
    worksheet3.write('F16', 'MWBE_Status', reg_bold)
    worksheet3.write('F17', 'FQuarter', reg_bold)
    worksheet3.write('F18', 'SubIndustry', reg_bold)

    worksheet3.write('G4', 'Agency Name', reg)
    worksheet3.write('G5', 'Prime Contract ID', reg)
    worksheet3.write('G6', 'Prime Contract Award Method', reg)
    worksheet3.write('G7', 'Industry of Prime Contract', reg)
    worksheet3.write('G8', 'Original Contract Value of Prime Contract', reg)
    worksheet3.write('G9', 'Contract Value of subcontract', reg)
    worksheet3.write('G10', 'Start date of subcontract', reg)
    worksheet3.write('G11', 'A combination of ethnicity and MWBE type (MBE/WBE/MWBE)', reg)
    worksheet3.write('G12', 'Registration Date of Prime Contract', reg)
    worksheet3.write('G13', 'Legal name of sub vendor', reg)
    worksheet3.write('G14', 'FMS ID of sub vendor', reg)
    worksheet3.write('G15', 'Description of sub contract scope', reg)
    worksheet3.write('G16', 'Indicates whether awarded firm is an M/WBE or not', reg)
    worksheet3.write('G17', 'The Fiscal Quarter of sub start date', reg)
    worksheet3.write('G18', 'Industry of the sub contract', reg)

    worksheet3.write('H4', 'FMS', reg)
    worksheet3.write('H5', 'FMS/MOCS', reg)
    worksheet3.write('H6', 'FMS', reg)
    worksheet3.write('H7', 'FMS/MOCS', reg)
    worksheet3.write('H8', 'FMS/MOCS', reg)
    worksheet3.write('H9', 'FMS', reg)
    worksheet3.write('H10', 'FMS', reg)
    worksheet3.write('H11', 'SBS/MOCS', reg)
    worksheet3.write('H12', 'FMS', reg)
    worksheet3.write('H13', 'FMS', reg)
    worksheet3.write('H14', 'FMS', reg)
    worksheet3.write('H15', 'FMS', reg)
    worksheet3.write('H16', 'SBS/MOCS', reg)
    worksheet3.write('H17', 'FMS/MOCS', reg)
    worksheet3.write('H18', 'FMS/MOCS', reg)

    worksheet3.write('B4', 'Agency', reg_bold)
    worksheet3.write('B5', 'DOC_CD', reg_bold)
    worksheet3.write('B6', 'DOC_DEPT_CD', reg_bold)
    worksheet3.write('B7', 'DOC_ID', reg_bold)
    worksheet3.write('B8', 'ContractID', reg_bold)
    worksheet3.write('B9', 'EPIN', reg_bold)
    worksheet3.write('B10', 'RegistrationDate', reg_bold)
    worksheet3.write('B11', 'ContractValue', reg_bold)
    worksheet3.write('B12', 'Industry', reg_bold)
    worksheet3.write('B13', 'SizeGroup', reg_bold)
    worksheet3.write('B14', 'MWBE_Status', reg_bold)
    worksheet3.write('B15', 'ReportCategory', reg_bold)
    worksheet3.write('B16', 'EthGen', reg_bold)
    worksheet3.write('B17', 'MWBE_LL', reg_bold)
    worksheet3.write('B18', 'Method', reg_bold)
    worksheet3.write('B19', 'VendorName', reg_bold)
    worksheet3.write('B20', 'VendorTIN', reg_bold)
    worksheet3.write('B21', 'VendorNumber', reg_bold)
    worksheet3.write('B22', 'Purpose', reg_bold)
    worksheet3.write('B23', 'StartDate', reg_bold)
    worksheet3.write('B24', 'EndDate', reg_bold)
    worksheet3.write('B25', 'REG_FQ', reg_bold)
    worksheet3.write('B26', 'REG_FY', reg_bold)

    worksheet3.write('C4', 'Agency Name', reg)
    worksheet3.write('C5', 'Document Code', reg)
    worksheet3.write('C6', 'Document Department Code', reg)
    worksheet3.write('C7', 'Document ID', reg)
    worksheet3.write('C8', 'DOC_CD + DOC_DEPT_CD + DOC_ID', reg)
    worksheet3.write('C9', 'Electronic tracking number', reg)
    worksheet3.write('C10', 'Date Contract Registered', reg)
    worksheet3.write('C11', 'Original Contract Value', reg)
    worksheet3.write('C12', 'Industry of Prime Contract', reg)
    worksheet3.write('C13', 'Size Grouping of Contract Value', reg)
    worksheet3.write('C14', 'Indicates whether awarded firm is an M/WBE or not', reg)
    worksheet3.write('C15', 'A combination of ethnicity and MWBE type (MBE/WBE/MWBE)', reg)
    worksheet3.write('C16', 'Ethnicity category provided by SBS', reg)
    worksheet3.write('C17', 'Indicates whether prime contract subject to LL1 or LL129', reg)
    worksheet3.write('C18', 'Award Method of Contract', reg)
    worksheet3.write('C19', 'Legal Name of Vendor', reg)
    worksheet3.write('C20', 'Tax Identification Number of Vendor', reg)
    worksheet3.write('C21', 'FMS_ID', reg)
    worksheet3.write('C22', 'Description of contract scope', reg)
    worksheet3.write('C23', 'Original Start Date of Contract', reg)
    worksheet3.write('C24', 'Original End Date of Contract', reg)
    worksheet3.write('C25', 'The Fiscal Quarter of registration', reg)
    worksheet3.write('C26', 'The Fiscal Year of registration', reg)

    worksheet3.write('D4', 'FMS', reg)
    worksheet3.write('D5', 'FMS', reg)
    worksheet3.write('D6', 'FMS', reg)
    worksheet3.write('D7', 'FMS', reg)
    worksheet3.write('D8', 'MOCS', reg)
    worksheet3.write('D9', 'FMS', reg)
    worksheet3.write('D10', 'FMS/MOCS', reg)
    worksheet3.write('D11', 'FMS/MOCS', reg)
    worksheet3.write('D12', 'SBS/MOCS', reg)
    worksheet3.write('D13', 'SBS/MOCS', reg)
    worksheet3.write('D14', 'SBS/MOCS', reg)
    worksheet3.write('D15', 'SBS/MOCS', reg)
    worksheet3.write('D16', 'FMS/MOCS', reg)
    worksheet3.write('D17', 'FMS/MOCS', reg)
    worksheet3.write('D18', 'FMS', reg)
    worksheet3.write('D19', 'FMS', reg)
    worksheet3.write('D20', 'FMS', reg)
    worksheet3.write('D21', 'FMS/MOCS', reg)
    worksheet3.write('D22', 'FMS/MOCS', reg)
    worksheet3.write('D23', 'FMS/MOCS', reg)
    worksheet3.write('D24', 'FMS/MOCS', reg)
    worksheet3.write('D25', 'FMS/MOCS', reg)
    worksheet3.write('D26', 'FMS/MOCS', reg)

    writer.save()

#Directors Handout
if __name__ == "__main__":

     prime_util = pu

     prime_util = prime_util[prime_util['Agency'] != u'OMB']

     prime_util['Agency'] = prime_util['Agency'].str.replace('DoITT', 'DOITT')

     prime_util['RegistrationDate'] = pd.to_datetime(prime_util['RegistrationDate'])

     prime_util = prime_util.drop_duplicates('ContractID')

     ###

     prime_util['Agency'] = agency(prime_util['Agency'])
     subs = subs_post_master
     sub_util = subs_post_master
     pw = prime_util[['Agency', 'ContractID', 'RegistrationDate', 'ContractValue', 'MWBE_Status', 'Industry', 'SizeGroup', 'REG_FQ', 'ReportCategory', 'EthGen']]
     sg = pd.DataFrame(pw.SizeGroup.unique())  # Size Groups in Prime Data
     ind = pd.DataFrame(pw.Industry.unique())  # Distinct Industries in Prime Data

     MECE_SizeGroups = pd.DataFrame(['>$1M, <=$5M', 'Micro Purchase', '>$100K, <=$1M', 'Small Purchase', '>$5M, <=$25M',
                                     '>$25M'])  # Define 6 Categories of Size Groups of Interest
     MECE_Industries = pd.DataFrame(['Standardized Services', 'Goods', 'Professional Services',
                                     'Construction Services'])  # Define 4 Categories of Industries
     sw = subs

     sw['SubIndustry2'] = subs['Industry']
     # sub_util['SubIndustry2'] = subs
     sw['Agency'] = agency(sw['Agency'])
     pw['Agency'] = agency(pw['Agency'])

     if set(sg) == set(MECE_SizeGroups) & set(ind) == set(MECE_Industries):  # testing to see if the unique size groups and industries (6 and 4) Size Groups and industries.
        industry = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']

        pw_ = pw
        sw_ = sw
        pwm = pw_[(pw_['MWBE_Status'] == 'MWBE')]
        mds = pd.merge(pwm, sw_, on='ContractID')
        mo = pd.merge(prime_util, subs, on='ContractID')
        mdst = pd.merge(pw_, sw_, on='ContractID')

        b1 = round(pw_[(pw_['MWBE_Status'] == 'MWBE')].ContractValue.sum(), 0)  # how to round all values in a df
        b2 = pw_[(pw_['MWBE_Status'] == 'MWBE')].ContractID.nunique()
        b3 = round(pw_.ContractValue.sum(), 0)
        b4 = pw_.ContractID.nunique()
        b5 = round(sw_[(sw_['MWBE_Status'] == 'MWBE')].SubValue.sum(), 0)
        b6 = sw_[(sw_['MWBE_Status'] == 'MWBE')].SubContractID.nunique()
        b7 = round(sub_util['SubValue'].sum(), 0)
        b8 = sub_util.shape[0]
        b9 = round(mds[(mds['MWBE_Status_y'] == 'MWBE')].SubValue.sum(), 0)
        b10 = mds[(mds['MWBE_Status_y'] == 'MWBE')].SubContractID.nunique()
        b11 = round(mo[(mo['MWBE_Status_y'] == 'Not MWBE') & (mo['MWBE_Status_x'] == 'MWBE')].SubValue.sum(), 0)
        b12 = mo[(mo['MWBE_Status_y'] == 'Not MWBE') & (mo['MWBE_Status_x'] == 'MWBE')].SubContractID.nunique()
        b13 = round(mdst.SubValue.sum(), 0)
        b14 = mdst.SubContractID.nunique()

        counts = []
        counts.append([b2, b6, b10, b12, b4, b8, b14])
        counts_df = pd.DataFrame(counts, columns=['MWBE Primes', 'MWBE SubContracts', 'M/WBE Subs on MWBE Primes',
                                                  'Non MWBE Subs on MWBE Primes', 'Total Primes', 'Total SubContracts',
                                                  'All Subs on All Primes'])

        values = []
        values.append([b1, b5, b9, b11, b3, b7, b13])
        values_df = pd.DataFrame(values, columns=['MWBE Primes', 'MWBE SubContracts', 'M/WBE Subs on MWBE Primes',
                                                  'Non MWBE Subs on MWBE Primes', 'Total Primes', 'Total SubContracts',
                                                  'All Subs on All Primes'])

        df = pd.concat([counts_df.T, values_df.T], axis=1)
        df.columns = ['Counts', 'Values']

        sum1_cw = df.loc[['MWBE Primes', 'MWBE SubContracts'], :].sum() - df.loc[['M/WBE Subs on MWBE Primes',
                                                                                  'Non MWBE Subs on MWBE Primes'],
                                                                          :].sum()
        sum2_cw = df.loc[['Total Primes', 'Total SubContracts']].sum() - df.loc[['All Subs on All Primes']].sum()

        df.loc['Total MWBE', 'Counts'] = sum1_cw.Counts
        df.loc['Total MWBE', 'Values'] = sum1_cw.Values

        df.loc['Total Contracts', 'Counts'] = sum2_cw.Counts
        df.loc['Total Contracts', 'Values'] = sum2_cw.Values

        df.loc['Final M/WBE Utilization Rate', 'Values'] = str(int(sum1_cw.Values) / int(sum2_cw.Values))[0:5] + ' %'

        df_final_city_wide = df.ix[['MWBE Primes',
                                    'MWBE SubContracts',
                                    'M/WBE Subs on MWBE Primes',
                                    'Non MWBE Subs on MWBE Primes',
                                    'Total MWBE',
                                    'Total Primes',
                                    'Total SubContracts',
                                    'All Subs on All Primes',
                                    'Total Contracts',
                                    'Final M/WBE Utilization Rate'], :]

        df_final_city_wide.index.name = 'Citywide'

        pwm = pw[pw['MWBE_Status'] == 'MWBE']
        mds = pd.merge(pwm, sw, on='ContractID')
        mo = pd.merge(prime_util, subs, on='ContractID')
        mdst = pd.merge(pw, sw, on='ContractID')

        b1 = round(pw[(pw['MWBE_Status'] == 'MWBE')].ContractValue.sum(), 0)  # how to round all values in a df
        b2 = pw[(pw['MWBE_Status'] == 'MWBE')].ContractID.nunique()
        b3 = round(pw.ContractValue.sum(), 0)
        b4 = pw.ContractID.nunique()
        b5 = round(sw[(sw['MWBE_Status'] == 'MWBE')].SubValue.sum(), 0)
        b6 = sw[(sw['MWBE_Status'] == 'MWBE')].SubContractID.nunique()
        b7 = round(sw.SubValue.sum(), 0)
        b8 = sw.SubContractID.nunique()
        b9 = round(mds[(mds['MWBE_Status_y'] == 'MWBE')].SubValue.sum(), 0)
        b10 = mds[(mds['MWBE_Status_y'] == 'MWBE')].SubContractID.nunique()
        b11 = round(mo[(mo['MWBE_Status_y'] == 'Not MWBE') & (mo['MWBE_Status_x'] == 'MWBE')].SubValue.sum(), 0)
        b12 = mo[(mo['MWBE_Status_y'] == 'Not MWBE') & (mo['MWBE_Status_x'] == 'MWBE')].SubContractID.nunique()
        b13 = round(mdst.SubValue.sum(), 0)
        b14 = mdst.SubContractID.nunique()

        counts = []
        counts.append([b2, b6, b10, b12, b4, b8, b14])
        counts_df = pd.DataFrame(counts, columns=['MWBE Primes', 'MWBE SubContracts', 'M/WBE Subs on MWBE Primes',
                                                  'Non MWBE Subs on MWBE Primes', 'Total Primes', 'Total SubContracts',
                                                  'All Subs on All Primes'])

        values = []
        values.append([b1, b5, b9, b11, b3, b7, b13])
        values_df = pd.DataFrame(values, columns=['MWBE Primes', 'MWBE SubContracts', 'M/WBE Subs on MWBE Primes',
                                                  'Non MWBE Subs on MWBE Primes', 'Total Primes', 'Total SubContracts',
                                                  'All Subs on All Primes'])

        df = pd.concat([counts_df.T, values_df.T], axis=1)
        df.columns = ['Counts', 'Values']

        sum1_cw = df.loc[['MWBE Primes', 'MWBE SubContracts'], :].sum() - df.loc[['M/WBE Subs on MWBE Primes',
                                                                                  'Non MWBE Subs on MWBE Primes'],
                                                                          :].sum()

        sum2_cw = df.loc[['Total Primes', 'Total SubContracts']].sum() - df.loc[['All Subs on All Primes']].sum()

        df.loc['Total MWBE', 'Counts'] = sum1_cw.Counts
        df.loc['Total MWBE', 'Values'] = sum1_cw.Values

        df.loc['Total Contracts', 'Counts'] = sum2_cw.Counts
        df.loc['Total Contracts', 'Values'] = sum2_cw.Values

        df.loc['Final M/WBE Utilization Rate', 'Values'] = str(int(sum1_cw.Values) / int(sum2_cw.Values)*100)[0:5] + ' %'

        df_final_city_wide = df.ix[['MWBE Primes',
                                    'MWBE SubContracts',
                                    'M/WBE Subs on MWBE Primes',
                                    'Non MWBE Subs on MWBE Primes',
                                    'Total MWBE',
                                    'Total Primes',
                                    'Total SubContracts',
                                    'All Subs on All Primes',
                                    'Total Contracts',
                                    'Final M/WBE Utilization Rate'], :].fillna('N/A')

        df_final_city_wide.index.name = 'City Wide'

        temp = sw[sw['MWBE_Status'] == 'MWBE'].groupby(['Agency'])['SubContractID'].nunique()

        for i in sorted(pw['Agency'].unique()): # sorted(pw['Agency'].unique())
            primes_ = prime_util[prime_util['Agency'] == i]
            subs_ = subs[subs['Agency'] == i]
            pw_ = pw[pw['Agency'] == i]
            ab_ = sw[sw['Agency'] == i]
            pwm = pw_[(pw_['MWBE_Status'] == 'MWBE')]
            mds = pd.merge(pwm, sw_, on='ContractID')
            mo = pd.merge(primes_, subs_, on='ContractID')
            mdst = pd.merge(pw_, ab_, on='ContractID')
            prime_util_a = prime_util[prime_util['Agency'] == i]
            sub_util_a = sub_util[sub_util['Agency'] == i]
            sub_util_a = sub_util_a[
                [u'Agency', u'ContractID', u'Method', u'Industry', u'ContractValue', u'SubIndustry', u'SubIndustry',
                 u'SubValue', u'SizeGroup', u'SubStartDate', u'ReportCategory', u'MWBE_Status',
                 u'RegistrationDate', u'SubVendorName', u'SubVendorNumber', u'SubDescr', u'FQuarter', u'SubContractID']]
            b1 = pw_[(pw_['MWBE_Status'] == 'MWBE')].ContractValue.sum()  # how to round all values in a df
            b2 = pw_[(pw_['MWBE_Status'] == 'MWBE')].ContractID.nunique()
            b3 = pw_.ContractValue.sum()
            b4 = pw_.ContractID.nunique()
            b5 = ab_[(ab_['MWBE_Status'] == 'MWBE')].SubValue.sum()
            b6 = ab_[(ab_['MWBE_Status'] == 'MWBE')].SubContractID.nunique()
            b7 = ab_.SubValue.sum()
            b8 = ab_.SubContractID.nunique()
            b9 = mds[(mds['MWBE_Status_y'] == 'MWBE')].SubValue.sum()
            b10 = mds[(mds['MWBE_Status_y'] == 'MWBE')].SubContractID.nunique()
            b11 = mo[(mo['MWBE_Status_y'] == 'Not MWBE') & (mo['MWBE_Status_x'] == 'MWBE')].SubValue.sum()
            b12 = mo[(mo['MWBE_Status_y'] == 'Not MWBE') & (mo['MWBE_Status_x'] == 'MWBE')].SubContractID.nunique()
            b13 = mdst.SubValue.sum()
            b14 = mdst.SubContractID.nunique()

            counts = []
            counts.append([b2, b6, b10, b12, b4, b8, b14])
            counts_df = pd.DataFrame(counts, columns=['MWBE Primes', 'MWBE SubContracts', 'M/WBE Subs on MWBE Primes',
                                                      'Non MWBE Subs on MWBE Primes', 'Total Primes',
                                                      'Total SubContracts',
                                                      'All Subs on All Primes'])

            values = []
            values.append([b1, b5, b9, b11, b3, b7, b13])
            values_df = pd.DataFrame(values, columns=['MWBE Primes', 'MWBE SubContracts', 'M/WBE Subs on MWBE Primes',
                                                      'Non MWBE Subs on MWBE Primes', 'Total Primes',
                                                      'Total SubContracts',
                                                      'All Subs on All Primes'])

            df = pd.concat([counts_df.T, values_df.T], axis=1)
            df.columns = ['Counts', 'Values']
            df['Values'] = df['Values'].astype(float)

            sum1 = df.loc[['MWBE Primes', 'MWBE SubContracts'], :].sum() - df.loc[['M/WBE Subs on MWBE Primes', 'Non MWBE Subs on MWBE Primes'], :].sum()

            sum2 = df.loc[['Total Primes', 'Total SubContracts']].sum() - df.loc[['All Subs on All Primes']].sum()

            df.loc['Total MWBE', 'Counts'] = sum1.Counts
            df.loc['Total MWBE', 'Values'] = sum1.Values

            df.loc['Total Contracts', 'Counts'] = sum2.Counts
            df.loc['Total Contracts', 'Values'] = sum2.Values

            df.loc['Final M/WBE Utilization Rate', 'Values'] = str((int(sum1.Values) / int(sum2.Values)) * 100)[0:5] + ' %'

            df_final = df.ix[['MWBE Primes',
                              'MWBE SubContracts',
                              'M/WBE Subs on MWBE Primes',
                              'Non MWBE Subs on MWBE Primes',
                              'Total MWBE',
                              'Total Primes',
                              'Total SubContracts',
                              'All Subs on All Primes',
                              'Total Contracts',
                              'Final M/WBE Utilization Rate'], :]

            df_final.index.name = i

            ###########################
            # Primes Summary Dataframe
            ###########################

            a1_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a1_1.name = 'MBE Black (#)'

            a1_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
                ['Industry', 'SizeGroup']).ContractValue.sum()
            a1_2.name = 'MBE Black ($)'

            a2_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a2_1.name = 'MBE Asian American (#)'

            a2_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
                ['Industry', 'SizeGroup']).ContractValue.sum()
            a2_2.name = 'MBE Asian American ($)'

            a3_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a3_1.name = 'MBE Hispanic American (#)'

            a3_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).ContractValue.sum()
            a3_2.name = 'MBE Hispanic American ($)'

            a4_1 = pw_[pw_['ReportCategory'].isin(
                ['Male-Owned MBE - Black', 'WBE - Black', 'Male-Owned MBE - Asian', 'WBE - Asian',
                 'Male-Owned MBE - Hispanic',
                 'WBE - Hispanic'])].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a4_1.name = 'MBE Total (#)'

            a4_2 = pw_[pw_['ReportCategory'].isin(
                ['Male-Owned MBE - Black', 'WBE - Black', 'Male-Owned MBE - Asian', 'WBE - Asian',
                 'Male-Owned MBE - Hispanic',
                 'WBE - Hispanic'])].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a4_2.name = 'MBE Total ($)'

            a5_1 = pw_[pw_['ReportCategory'] == 'WBE - Black'].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a5_1.name = 'WBE Black (#)'
            a5_2 = pw_[pw_['ReportCategory'] == 'WBE - Black'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a5_2.name = 'WBE Black ($)'

            a6_1 = pw_[pw_['ReportCategory'] == 'WBE - Asian'].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a6_1.name = 'WBE Asian (#)'
            a6_2 = pw_[pw_['ReportCategory'] == 'WBE - Asian'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a6_2.name = 'WBE Asian ($)'

            a7_1 = pw_[pw_['ReportCategory'] == 'WBE - Hispanic'].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a7_1.name = 'WBE Hispanic (#)'
            a7_2 = pw_[pw_['ReportCategory'] == 'WBE - Hispanic'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a7_2.name = 'WBE Hispanic ($)'

            # WBE Caucasian

            a8_1 = pw_[pw_['ReportCategory'].isin(['WBE - Caucasian Woman'])].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a8_1.name = 'WBE - Caucasian (#)'
            a8_2 = pw_[pw_['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
                ['Industry', 'SizeGroup']).ContractValue.sum()
            a8_2.name = 'WBE - Caucasian ($)'

            # WBE Total

            a9_1 = pw_[pw_['ReportCategory'].isin(
                ['WBE - Black', 'WBE - Caucasian Woman', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a9_1.name = 'WBE Total (#)'
            a9_2 = pw_[pw_['ReportCategory'].isin(
                ['WBE - Black', 'WBE - Caucasian Woman', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).ContractValue.sum()
            a9_2.name = 'WBE Total ($)'

            # Non Certified

            a10_1 = pw_[pw_['MWBE_Status'] == 'Not MWBE'].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a10_1.name = 'Non-Certified (#)'
            a10_2 = pw_[pw_['MWBE_Status'] == 'Not MWBE'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a10_2.name = 'Non-Certified ($)'

            # MBE and WBE

            a11_1 = pw_[pw_['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a11_1.name = 'Both MBE and WBE (#)'
            a11_2 = pw_[pw_['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).ContractValue.sum()
            a11_2.name = 'Both MBE and WBE ($)'

            # Total M/WBE

            a12_1 = pw_[pw_['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a12_1.name = 'Total M/WBE (#)'

            a12_2 = pw_[pw_['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a12_2.name = 'Total M/WBE ($)'

            a13_1 = pw_.groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a13_1.name = 'Total ($)'

            a13_2 = pw_.groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a13_2.name = 'Total (#)'

            a14_3 = pw_.groupby(['Industry', 'SizeGroup']).ContractID.nunique() #spaceholder column
            a14_3.name = 'Total MWBE (%)'

            df = pd.concat(
                [a1_1, a1_2, a2_1, a2_2, a3_1, a3_2, a5_1, a5_2, a6_1, a6_2, a7_1, a7_2, a8_1, a8_2, a10_1, a10_2,
                 a11_1, a11_2, a12_1, a12_2, a14_3, a13_2, a13_1], axis=1)

            df = df.fillna(0)

            empty = []

            size_group_dict = {'Micro Purchase': 1, 'Small Purchase': 2, '>$100K, <=$1M': 3, '>$1M, <=$5M': 4,
                               '>$5M, <=$25M': 5, '>$25M': 6}

            index_order = {'Total': 'A', 'Micro Purchase': 'B', 'Small Purchase': 'C', '>$100K, <=$1M': 'D',
                           '>$1M, <=$5M': 'E', '>$5M, <=$25M': 'F', '>$25M': 'G'}

            size_groups = ['Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M', '>$5M, <=$25M', '>$25M']

            for j in industry:

                index_dict = {'A': str(j), 'B': 'Micro Purchase', 'C': 'Small Purchase', 'D': '>$100K, <=$1M',
                              'E': '>$1M, <=$5M', 'F': '>$5M, <=$25M', 'G': '>$25M'}

                try:
                    df_industry = df.loc[(j, size_groups), :]  # sliced it from the original

                    industry_total_row = df_industry.sum()

                    industry_total_row = pd.DataFrame(industry_total_row)

                    industry_total_row = industry_total_row.T

                    df_zeros = pd.DataFrame(np.zeros((6 - df_industry.shape[0], df_industry.shape[1])),
                                            columns=df_industry.columns, index=[(j, y) for y in size_groups if
                                                                                y not in [x[1] for x in
                                                                                          df_industry.index if
                                                                                          isinstance(x, tuple)]])

                    df_industry = pd.concat([df_industry, df_zeros])

                    df_industry = pd.concat([industry_total_row, df_industry], axis=0)

                    df_industry.index.values[0] = tuple([str(j), 'Total'])

                    df_industry.index = [index_order.get(df_industry.index.values[x][1]) for x in
                                         range(len(df_industry.index.values))]  # Map industries to Letters

                    df_industry = df_industry.sort_index(ascending=True)  # Sort by Alphabetical Order

                    df_industry.index = [index_dict.get(df_industry.index.values[x]) for x in
                                         range(
                                             len(df_industry.index))]  # Replace Letters with Original Size Group Labels

                    empty.append(df_industry)

                except:
                    df_industry = pd.DataFrame(np.zeros((7, df.shape[1])), columns=df.columns,
                                               index=[j, 'Micro Purchase', 'Small Purchase', '>$100K, <=$1M',
                                                      '>$1M, <=$5M',
                                                      '>$5M, <=$25M', '>$25M'])
                    empty.append(df_industry)

            final = pd.concat(empty)

            final = final.fillna(0)

            final.name = 'Industry and Size Group'

            try:
                final['Total MWBE (%)'] = final['Total M/WBE ($)'].astype(float) / final['Total ($)'].astype(float)
            except(ZeroDivisionError):
                pass

            final = final.fillna(0)

            ###############

            total_portion = [pd.DataFrame(final.loc[x].astype(float).sum(), columns=[x]).T for x in size_groups]

            total_portion = pd.concat(total_portion)

            total_row = pd.DataFrame(total_portion.sum(), columns=['Total']).T

            total_portion = pd.concat([total_row, total_portion])

            total_portion['Total MWBE (%)'] = total_portion['Total M/WBE ($)'] / total_portion['Total ($)']

            total_portion['Total M/WBE (#)'] = total_portion['MBE Black (#)'] + total_portion['MBE Asian American (#)'] + total_portion['MBE Hispanic American (#)'] + total_portion['WBE - Caucasian (#)']

            total_portion['Total M/WBE ($)'] = total_portion['MBE Black ($)'] + total_portion['MBE Asian American ($)'] + total_portion['MBE Hispanic American ($)'] + total_portion['WBE - Caucasian ($)']

            total_portion = total_portion.fillna(0)

            overall1 = pd.concat([final, total_portion])

            ##############################

            if i in sw['Agency'].unique():

                # Black American
                a1_1 = ab_[ab_['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].pivot_table(
                    index='SizeGroup', columns='SubIndustry2', values='SubContractID', fill_value=0,
                    aggfunc='nunique').unstack().to_frame().rename(columns={0: 'MBE Black (#)'})
                a1_2 = ab_[ab_['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].pivot_table(
                    index='SizeGroup', columns='SubIndustry2', values='SubValue', fill_value=0,
                    aggfunc='sum').unstack().to_frame().rename(columns={0: 'MBE Black ($)'})

                a2_1 = ab_[ab_['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].pivot_table(
                    index='SizeGroup', columns='SubIndustry2', values='SubContractID', fill_value=0,
                    aggfunc='nunique').unstack().to_frame().rename(columns={0: 'MBE Asian American (#)'})
                a2_2 = ab_[ab_['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].pivot_table(
                    index='SizeGroup', columns='SubIndustry2', values='SubValue', fill_value=0,
                    aggfunc='sum').unstack().to_frame().rename(columns={0: 'MBE Asian American ($)'})

                a3_1 = ab_[ab_['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].pivot_table(
                    index='SizeGroup', columns='SubIndustry2', values='SubContractID', fill_value=0,
                    aggfunc='nunique').unstack().to_frame().rename(columns={0: 'MBE Hispanic American (#)'})
                a3_2 = ab_[ab_['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].pivot_table(
                    index='SizeGroup', columns='SubIndustry2', values='SubValue', fill_value=0,
                    aggfunc='sum').unstack().to_frame().rename(columns={0: 'MBE Hispanic American ($)'})

                a4_1 = ab_[ab_['ReportCategory'].isin(
                    ['Male-Owned MBE - Black', 'WBE - Black', 'Male-Owned MBE - Asian', 'WBE - Asian',
                     'Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].pivot_table(
                    index='SizeGroup', columns='SubIndustry2', values='SubContractID', fill_value=0,
                    aggfunc='nunique').unstack().to_frame().rename(columns={0: 'MBE Total (#)'})

                a4_2 = ab_[ab_['ReportCategory'].isin(
                    ['Male-Owned MBE - Black', 'WBE - Black', 'Male-Owned MBE - Asian', 'WBE - Asian',
                     'Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].pivot_table(index='SizeGroup',
                                                                                  columns='SubIndustry2',
                                                                                  values='SubValue', fill_value=0,
                                                                                  aggfunc='sum').unstack().to_frame().rename(
                    columns={0: 'MBE Total ($)'})

                a5_1 = ab_[ab_['ReportCategory'].isin(['WBE - Black'])].pivot_table(index='SizeGroup',
                                                                                    columns='SubIndustry2',
                                                                                    values='SubContractID',
                                                                                    fill_value=0,
                                                                                    aggfunc='nunique').unstack().to_frame().rename(
                    columns={0: 'WBE Black (#)'})

                a5_2 = ab_[ab_['ReportCategory'].isin(['WBE - Black'])].pivot_table(index='SizeGroup',
                                                                                    columns='SubIndustry2',
                                                                                    values='SubValue', fill_value=0,
                                                                                    aggfunc='sum').unstack().to_frame().rename(
                    columns={0: 'WBE Black ($)'})

                a6_1 = ab_[ab_['ReportCategory'].isin(['WBE - Asian'])].pivot_table(index='SizeGroup',
                                                                                    columns='SubIndustry2',
                                                                                    values='SubContractID',
                                                                                    fill_value=0,
                                                                                    aggfunc='nunique').unstack().to_frame().rename(
                    columns={0: 'WBE Asian (#)'})

                a6_2 = ab_[ab_['ReportCategory'].isin(['WBE - Asian'])].pivot_table(index='SizeGroup',
                                                                                    columns='SubIndustry2',
                                                                                    values='SubValue', fill_value=0,
                                                                                    aggfunc='sum').unstack().to_frame().rename(
                    columns={0: 'WBE Asian ($)'})

                a7_1 = ab_[ab_['ReportCategory'].isin(['WBE - Hispanic'])].pivot_table(index='SizeGroup',
                                                                                       columns='SubIndustry2',
                                                                                       values='SubContractID',
                                                                                       fill_value=0,
                                                                                       aggfunc='nunique').unstack().to_frame().rename(
                    columns={0: 'WBE Hispanic (#)'})

                a7_2 = ab_[ab_['ReportCategory'].isin(['WBE - Hispanic'])].pivot_table(index='SizeGroup',
                                                                                       columns='SubIndustry2',
                                                                                       values='SubValue', fill_value=0,
                                                                                       aggfunc='sum').unstack().to_frame().rename(
                    columns={0: 'WBE Hispanic ($)'})

                a8_1 = ab_[ab_['ReportCategory'].isin(['WBE - Caucasian Woman'])].pivot_table(index='SizeGroup',
                                                                                              columns='SubIndustry2',
                                                                                              values='SubContractID',
                                                                                              fill_value=0,
                                                                                              aggfunc='nunique').unstack().to_frame().rename(
                    columns={0: 'WBE - Caucasian Woman (#)'})

                a8_2 = ab_[ab_['ReportCategory'].isin(['WBE - Caucasian Woman'])].pivot_table(index='SizeGroup',
                                                                                              columns='SubIndustry2',
                                                                                              values='SubValue',
                                                                                              fill_value=0,
                                                                                              aggfunc='sum').unstack().to_frame().rename(
                    columns={0: 'WBE - Caucasian Woman ($)'})

                a9_1 = ab_[ab_['ReportCategory'].isin(
                    ['WBE - Black', 'WBE - Asian', 'WBE - Hispanic', 'WBE - Caucasian Woman'])].pivot_table(
                    index='SizeGroup', columns='SubIndustry2', values='SubContractID', fill_value=0,
                    aggfunc='nunique').unstack().to_frame().rename(columns={0: 'WBE - Total (#)'})

                a9_2 = ab_[ab_['ReportCategory'].isin(
                    ['WBE - Black', 'WBE - Asian', 'WBE - Hispanic', 'WBE - Caucasian Woman'])].pivot_table(
                    index='SizeGroup',
                    columns='SubIndustry2',
                    values='SubValue',
                    fill_value=0,
                    aggfunc='sum').unstack().to_frame().rename(columns={0: 'WBE - Total ($)'})

                a10_1 = ab_[ab_['MWBE_Status'].isin(['Not MWBE'])].pivot_table(
                    index='SizeGroup',
                    columns='SubIndustry2',
                    values='SubContractID',
                    fill_value=0,
                    aggfunc='nunique').unstack().to_frame().rename(columns={0: 'Non-Certified (#)'})

                a10_2 = ab_[ab_['MWBE_Status'].isin(['Not MWBE'])].pivot_table(
                    index='SizeGroup', columns='SubIndustry2', values='SubValue', fill_value=0,
                    aggfunc='sum').unstack().to_frame().rename(columns={0: 'Non-Certified ($)'})

                a11_1 = ab_[ab_['ReportCategory'].isin(
                    ['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].pivot_table(
                    index='SizeGroup',
                    columns='SubIndustry2',
                    values='SubContractID',
                    fill_value=0,
                    aggfunc='nunique').unstack().to_frame().rename(columns={0: 'Both MBE and WBE (#)'})

                a11_2 = ab_[ab_['ReportCategory'].isin(
                    ['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].pivot_table(
                    index='SizeGroup', columns='SubIndustry2', values='SubValue', fill_value=0,
                    aggfunc='sum').unstack().to_frame().rename(columns={0: 'Both MBE and WBE ($)'})

                a12_1 = ab_[(ab_['MWBE_Status'] == 'MWBE')].pivot_table(
                    index='SizeGroup',
                    columns='SubIndustry2',
                    values='SubContractID',
                    fill_value=0,
                    aggfunc='count').unstack().to_frame().rename(columns={0: 'Total M/WBE (#)'})

                a12_2 = ab_[(ab_['MWBE_Status'] == 'MWBE')].pivot_table(index='SizeGroup', columns='SubIndustry2',
                                                                        values='SubValue', fill_value=0,
                                                                        aggfunc='sum').unstack().to_frame().rename(
                    columns={0: 'Total M/WBE ($)'})

                a13_1 = ab_[ab_['Agency'] == i].pivot_table(index='SizeGroup', columns='SubIndustry2',
                                                            values='SubValue',
                                                            fill_value=0,
                                                            aggfunc='count').unstack().to_frame().rename(
                    columns={0: 'Total (#)'})

                a13_2 = ab_[ab_['Agency'] == i].pivot_table(index='SizeGroup', columns='SubIndustry2',
                                                            values='SubValue',
                                                            fill_value=0,
                                                            aggfunc='sum').unstack().to_frame().rename(
                    columns={0: 'Total ($)'})

                df = pd.concat(
                    [a1_1, a1_2, a2_1, a2_2, a3_1, a3_2, a5_1, a5_2, a6_1, a6_2, a7_1, a7_2, a8_1, a8_2, a10_1, a10_2,
                     a11_1, a11_2, a12_1, a12_2, a13_1, a13_2], axis=1)

                try:
                    df['Total MWBE (%)'] = df['Total M/WBE ($)'] / df['Total ($)']
                except:
                    df['Total MWBE (%)'] = pd.Series(np.zeros(len(df['Total M/WBE ($)'])))

                arrays = [
                    ['Construction Services', 'Construction Services', 'Construction Services', 'Construction Services',
                     'Construction Services', 'Construction Services', 'Goods', 'Goods', 'Goods', 'Goods', 'Goods',
                     'Goods',
                     'Professional Services', 'Professional Services', 'Professional Services', 'Professional Services',
                     'Professional Services', 'Professional Services', 'Standardized Services', 'Standardized Services',
                     'Standardized Services', 'Standardized Services', 'Standardized Services',
                     'Standardized Services'],
                    ['Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M', '>$5M, <=$25M', '>$25M',
                     'Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M', '>$5M, <=$25M', '>$25M',
                     'Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M', '>$5M, <=$25M', '>$25M',
                     'Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M', '>$5M, <=$25M', '>$25M']]

                index = pd.MultiIndex.from_tuples([x for x in zip(arrays[0],arrays[1])], names=['SubIndustry', 'SizeGroup'])

                df = pd.DataFrame(df, index=index)

                df = df.fillna(0)  # main df

                df = df[['MBE Black (#)', 'MBE Black ($)', 'MBE Asian American (#)',
                         'MBE Asian American ($)', 'MBE Hispanic American (#)',
                         'MBE Hispanic American ($)',
                         'WBE Black (#)', 'WBE Black ($)', 'WBE Asian (#)', 'WBE Asian ($)',
                         'WBE Hispanic (#)', 'WBE Hispanic ($)', 'WBE - Caucasian Woman (#)',
                         'WBE - Caucasian Woman ($)',
                         'Non-Certified (#)', 'Non-Certified ($)', 'Both MBE and WBE (#)',
                         'Both MBE and WBE ($)', 'Total M/WBE (#)', 'Total M/WBE ($)', 'Total MWBE (%)',
                         'Total (#)', 'Total ($)']]

                construction_total = pd.DataFrame(df.loc['Construction Services'].sum()).T
                construction_total.index = ['Construction Services']

                goods_total = pd.DataFrame(df.loc['Goods'].sum()).T
                goods_total.index = ['Goods']

                prof_total = pd.DataFrame(df.loc['Professional Services'].sum()).T
                prof_total.index = ['Professional Services']

                standardized_total = pd.DataFrame(df.loc['Standardized Services'].sum()).T
                standardized_total.index = ['Standardized Services']

                top_portion = pd.concat(
                    [construction_total, df.loc['Construction Services'], goods_total, df.loc['Goods'], prof_total,
                     df.loc['Professional Services'], standardized_total, df.loc['Standardized Services']])

                top_portion['Total MWBE (%)'] = top_portion['Total M/WBE ($)'].astype(float) / top_portion[
                    'Total ($)'].astype(float)

                top_portion = top_portion.fillna(0)

                size_groups = ['Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M', '>$5M, <=$25M',
                               '>$25M']

                total_portion = [pd.DataFrame(top_portion.loc[x].astype(float).sum(), columns=[x]).T for x in
                                 size_groups]

                total_portion = pd.concat(total_portion)

                total_row = pd.DataFrame(total_portion.sum(), columns=['Total']).T

                total_portion = pd.concat([total_row, total_portion])

                total_portion['Total MWBE (%)'] = total_portion['Total M/WBE ($)'] / total_portion['Total ($)']

                total_portion['Total M/WBE (#)'] = total_portion['MBE Black (#)'] + total_portion['MBE Asian American (#)'] + total_portion['MBE Hispanic American (#)'] + total_portion['WBE - Caucasian Woman (#)']

                total_portion['Total M/WBE ($)'] = total_portion['MBE Black ($)'] + total_portion['MBE Asian American ($)'] + total_portion['MBE Hispanic American ($)'] + total_portion['WBE - Caucasian Woman ($)']

                total_portion = total_portion.fillna(0)

                subs_final = pd.concat([top_portion, total_portion])

            filepath = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Directors Handout'

            startrow = 5
            startcol = 1

            writer = pd.ExcelWriter(filepath + '\\' + 'Directors Meeting' + '_Agency Handout_' + str(i) + '.xlsx',
                                    engine='xlsxwriter')

            df_final.to_excel(writer, sheet_name='1. Combined Primes and Subs', startrow=startrow, startcol=startcol,
                              header=False, index=True)

            startrow = 5
            startcol = 5

            df_final_city_wide.to_excel(writer, sheet_name='1. Combined Primes and Subs', startrow=startrow,
                                        startcol=startcol,
                                        header=False, index=True)

            workbook = writer.book
            worksheet = writer.sheets['1. Combined Primes and Subs']

            worksheet.set_column('B:B', 45)
            worksheet.set_column('F:F', 45)

            merge_format = workbook.add_format({
                'bold': 1,
                'border': 1,
                'align': 'center',
                'valign': 'vcenter',
                'bg_color': '#BFBFBF'})

            merge_format2 = workbook.add_format({
                'bold': 1,
                'align': 'center',
                'valign': 'vcenter',
                'bg_color': '#BFBFBF'})

            agency_table_green = workbook.add_format({
                'bold': 1,
                'border': 1,
                'align': 'left',
                'valign': 'vcenter',
                'fg_color': '#D8E4BC'})

            agency_table_orange = workbook.add_format({
                'bold': 1,
                'border': 1,
                'align': 'left',
                'valign': 'vcenter',
                'fg_color': '#FABF8F'})

            agency_table_grey = workbook.add_format({
                'bold': 1,
                'border': 1,
                'align': 'left',
                'valign': 'vcenter',
                'fg_color': '#BFBFBF'})

            agency_table_white = workbook.add_format({
                'bold': 1,
                'border': 1,
                'align': 'left',
                'valign': 'vcenter',
                'fg_color': '#FFFFFF'})

            format10 = workbook.add_format({'bold': 1,
                                            'border': 1,
                                            'align': 'center',
                                            'size': 11,
                                            'bg_color': '#FDE9D9'})

            format10_ = workbook.add_format({'bold': 1,
                                             'align': 'center',
                                             'size': 11,
                                             'bg_color': '#FDE9D9'})

            format11 = workbook.add_format({'bold': 1,
                                            'border': 1,
                                            'align': 'center',
                                            'size': 11,
                                            'bg_color': '#FFCC99'})

            format11_ = workbook.add_format({'bold': 1,
                                             'align': 'center',
                                             'size': 11,
                                             'bg_color': '#FFCC99'})

            dollar_signs = workbook.add_format({'num_format': '$###,###,###,##0'})

            worksheet.write('B5', 'Type of Contracts', merge_format)
            worksheet.write('C5', 'Count', merge_format)
            worksheet.write('D5', 'Value', merge_format)

            worksheet.write('B6', 'Includes M/WBE Primes', agency_table_green)
            worksheet.write('B7', 'Includes M/WBE Subcontracts', agency_table_green)
            worksheet.write('B8', 'Excludes M/WBE Subs on MWBE Primes', agency_table_orange)
            worksheet.write('B9', 'Excludes Non-M/WBE Subs from M/WBE Primes', agency_table_orange)
            worksheet.write('B10', 'Total M/WBE', agency_table_white)  # change
            worksheet.write('B11', 'Includes Total Primes', agency_table_green)
            worksheet.write('B12', 'Includes Total Subcontracts', agency_table_green)
            worksheet.write('B13', 'Excludes All Subs on All Primes', agency_table_orange)
            worksheet.write('B14', 'Total Contracts', agency_table_white)  # change
            worksheet.write('B15', 'Final M/WBE Utilization Rate', agency_table_grey)

            worksheet.write('F5', 'Type of Contracts', merge_format)
            worksheet.write('G5', 'Count', merge_format)
            worksheet.write('H5', 'Value', merge_format)

            worksheet.write('F6', 'Includes M/WBE Primes', agency_table_green)
            worksheet.write('F7', 'Includes M/WBE Subcontracts', agency_table_green)
            worksheet.write('F8', 'Excludes M/WBE Subs on MWBE Primes', agency_table_orange)
            worksheet.write('F9', 'Excludes Non-M/WBE Subs from M/WBE Primes', agency_table_orange)
            worksheet.write('F10', 'Total M/WBE', agency_table_white)  # change
            worksheet.write('F11', 'Includes Total Primes', agency_table_green)
            worksheet.write('F12', 'Includes Total Subcontracts', agency_table_green)
            worksheet.write('F13', 'Excludes All Subs on All Primes', agency_table_orange)
            worksheet.write('F14', 'Total Contracts', agency_table_white)  # change
            worksheet.write('F15', 'Final M/WBE Utilization Rate', agency_table_grey)

            format1 = workbook.add_format()
            format1.set_align('right')

            format2 = workbook.add_format()
            format2.set_bold()

            format3 = workbook.add_format()
            format3.set_align('center')
            format3.set_bold()

            format4 = workbook.add_format()
            format4.set_align('left')
            format4.set_bold()

            format5 = workbook.add_format({'bold': 1,
                                           'border': 1,
                                           'align': 'center',
                                           'valign': 'vcenter'})

            gridlines = workbook.add_format({'border': 1})

            worksheet.conditional_format('C6:D14', {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': gridlines})

            worksheet.conditional_format('G6:H14', {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': gridlines})

            worksheet.merge_range('C15:D15', df_final.loc['Final M/WBE Utilization Rate', 'Values'] , format5)

            # worksheet.merge_range('G15:H15', str(round(int(sum1_cw.Values) / int(sum2_cw.Values),2) *100)[0:5] + ' %', format5)

            worksheet.merge_range('G15:H15', '24.08'+ ' %', format5)

            worksheet.set_row(15, 17)

            worksheet.set_column('G:G', 14)
            worksheet.set_column('H:H', 16, format1)
            worksheet.set_column('I:I', 14, format1)
            worksheet.set_column('C:C', 14, format1)
            worksheet.set_column('D:D', 14, format1)

            worksheet.write(1, 1, str(i), format4)
            worksheet.write(3, 1,
                            'FY %s %s Local Law 1 and Local Law 129 M/WBE Utilization Primes and Subs' % (str(FY), FQ),
                            format4)

            worksheet.write(1, 5, 'Citywide', format4)
            worksheet.write(3, 5,
                            'FY %s %s Local Law 1 and Local Law 129 M/WBE Utilization Primes and Subs' % (str(FY), FQ),
                            format4)

            worksheet.conditional_format('D' + str(startrow + 1) + ':' + 'D' + str(startrow + 1 + df_final.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': dollar_signs})

            worksheet.conditional_format('H' + str(startrow + 1) + ':' + 'H' + str(startrow + 1 + df_final.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': dollar_signs})

            ############################################

            startrow = 5
            startcol = 1

            overall1.to_excel(writer, sheet_name='2. Primes Summary Table', startrow=startrow, startcol=startcol,
                              header=False, index=True)

            workbook = writer.book

            worksheet = writer.sheets['2. Primes Summary Table']

            merge_format = workbook.add_format(
                {'bold': 1, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'fg_color': '#daeef3'})
            merge_format.set_text_wrap()
            merge_format.set_font_size(11)

            merge_format2 = workbook.add_format(
                {'bold': 1, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'fg_color': '#16365C'})
            merge_format2.set_text_wrap()
            merge_format2.set_font_size(11)
            merge_format2.set_font_color('white')

            format4 = workbook.add_format({'bold': 1,
                                           'align': 'center',
                                           'size': 11})
            format4.set_bg_color('#FDE9D9')
            format4.set_right(1)
            format4.set_left(1)
            format4.set_top(1)
            format4.set_bottom(1)

            # Format 5
            format5 = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

            format5.set_bg_color('#FFCC99')
            format5.set_right(1)
            format5.set_left(1)
            format5.set_top(1)
            format5.set_bottom(1)

            # Format 6
            format6 = workbook.add_format({'fg_color': '#FDE9D9', 'bold': 1, 'size': 10})
            format6.set_bottom(1)
            format6.set_top(1)

            # Format 7
            format7 = workbook.add_format({'bold': 1,
                                           'align': 'center',
                                           'size': 11})

            format7.set_bg_color('#FDE9D9')

            format8 = workbook.add_format({'bold': 1,
                                           'align': 'center', 'size': 11})

            format8.set_bg_color('#FFCC99')

            format10 = workbook.add_format({'bold': 1,
                                            'border': 1,
                                            'align': 'center',
                                            'size': 11,
                                            'bg_color': '#FDE9D9'})

            format10_ = workbook.add_format({'bold': 1,
                                             'align': 'center',
                                             'size': 11,
                                             'bg_color': '#FDE9D9'})

            format11 = workbook.add_format({'bold': 1,
                                            'border': 1,
                                            'align': 'center',
                                            'size': 11,
                                            'bg_color': '#FFCC99'})

            format11_ = workbook.add_format({'bold': 1,
                                             'align': 'center',
                                             'size': 11,
                                             'bg_color': '#FFCC99'})

            format12 = workbook.add_format({'num_format': '$###,###,###,##0', 'border': 1})

            industry_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

            industry_index.set_bg_color('#FDE9D9')
            industry_index.set_right(1)
            industry_index.set_left(1)
            industry_index.set_top(1)
            industry_index.set_bottom(1)

            agency_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

            agency_index.set_bg_color('#f4b084')
            agency_index.set_right(1)
            agency_index.set_left(1)
            agency_index.set_top(1)
            agency_index.set_bottom(1)

            agency_body = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

            agency_body.set_bg_color('#f4b084')
            agency_body.set_right(1)
            agency_body.set_left(1)
            agency_body.set_top(1)
            agency_body.set_bottom(1)

            industry_body = workbook.add_format({'bold': 1,
                                                 'align': 'center',
                                                 'size': 11})

            industry_body.set_bg_color('#FDE9D9')

            size_group_body = workbook.add_format({'align': 'right', 'size': 11})
            size_group_body.set_right(1)
            size_group_body.set_left(1)
            size_group_body.set_top(1)
            size_group_body.set_bottom(1)

            size_group_index = workbook.add_format({'bold': 1, 'align': 'left', 'size': 11})
            size_group_index.set_right(1)
            size_group_index.set_left(1)
            size_group_index.set_top(1)
            size_group_index.set_bottom(1)

            total_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

            total_index.set_bg_color('#e26b0a')
            total_index.set_right(1)
            total_index.set_left(1)
            total_index.set_top(1)
            total_index.set_bottom(1)

            total_body = workbook.add_format({'bold': 1,
                                              'align': 'center',
                                              'size': 11})

            total_body.set_bg_color('#e26b0a')

            gridlines = workbook.add_format({'border': 1})

            worksheet.set_row(1, 17.5)
            worksheet.set_row(2, 22.5)
            worksheet.set_row(3, 22.5)
            worksheet.set_row(4, 20)

            worksheet.merge_range('B2:Y2',
                                  'Table A - FY %s Quarter %s, Prime Contracts Subject to M/WBE Program - Disaggregated by Industry' % (
                                  str(FY), str(FQ)[1]), merge_format2)
            worksheet.merge_range('B3:B5', 'Industry and Size Group', merge_format)

            percentage_signs = workbook.add_format({'num_format': '0%'})

            worksheet.merge_range('C3:H3', 'MBE', merge_format)
            worksheet.merge_range('I3:P3', 'WBE', merge_format)
            worksheet.merge_range('C4:D4', 'Black', merge_format)
            worksheet.merge_range('E4:F4', 'Asian', merge_format)
            worksheet.merge_range('G4:H4', 'Hispanic', merge_format)
            worksheet.merge_range('I4:J4', 'Black', merge_format)
            worksheet.merge_range('K4:L4', 'Asian', merge_format)
            worksheet.merge_range('M4:N4', 'Hispanic', merge_format)
            worksheet.merge_range('O4:P4', 'Caucasian', merge_format)
            worksheet.merge_range('Q3:R4', 'Non-Certified', merge_format)
            worksheet.merge_range('S3:T4', 'Certified as Both MBE & WBE', merge_format)
            worksheet.merge_range('U3:W4', 'Total M/WBE', merge_format)  # minority women only
            worksheet.merge_range('X3:Y4', 'Total', merge_format)

            worksheet.set_column('D:D', 11)
            worksheet.set_column('H:H', 11)
            worksheet.set_column('F:F', 12)
            worksheet.set_column('N:N', 12)
            worksheet.set_column('R:R', 13)
            worksheet.set_column('T:T', 14)
            worksheet.set_column('P:P', 12)
            worksheet.set_column('V:V', 12)
            worksheet.set_column('U:U', 6)
            worksheet.set_column('X:X', 6)
            worksheet.set_column('Y:Y', 12)

            worksheet.write('C' + str(startrow), '#', merge_format)
            worksheet.write('D' + str(startrow), '$', merge_format)
            worksheet.write('E' + str(startrow), '#', merge_format)
            worksheet.write('F' + str(startrow), '$', merge_format)
            worksheet.write('G' + str(startrow), '#', merge_format)
            worksheet.write('H' + str(startrow), '$', merge_format)
            worksheet.write('I' + str(startrow), '#', merge_format)
            worksheet.write('J' + str(startrow), '$', merge_format)
            worksheet.write('K' + str(startrow), '#', merge_format)
            worksheet.write('L' + str(startrow), '$', merge_format)
            worksheet.write('M' + str(startrow), '#', merge_format)
            worksheet.write('N' + str(startrow), '$', merge_format)
            worksheet.write('O' + str(startrow), '#', merge_format)
            worksheet.write('P' + str(startrow), '$', merge_format)
            worksheet.write('Q' + str(startrow), '#', merge_format)
            worksheet.write('R' + str(startrow), '$', merge_format)
            worksheet.write('S' + str(startrow), '#', merge_format)
            worksheet.write('T' + str(startrow), '$', merge_format)
            worksheet.write('U' + str(startrow), '#', merge_format)
            worksheet.write('V' + str(startrow), '$', merge_format)
            worksheet.write('W' + str(startrow), '%', merge_format)
            worksheet.write('X' + str(startrow), '#', merge_format)
            worksheet.write('Y' + str(startrow), '$', merge_format)

            worksheet.set_column('B:B', 22.5)

            dollar_columns = ['D', 'F', 'H', 'J', 'L', 'N', 'P', 'R', 'T', 'V', 'Y']

            for h in dollar_columns:
                worksheet.conditional_format(h + str(startrow) + ':' + h + str(startrow + overall1.shape[0]),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0,
                                              'format': format12})  # Gives Dollar Signs

            for h in ['C', 'E', 'G', 'I', 'K', 'M', 'O', 'Q', 'S', 'U', 'W', 'X']:
                worksheet.conditional_format(h + str(startrow) + ':' + h + str(startrow + overall1.shape[0]),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0,
                                              'format': gridlines})  # Gives Gridlines

            for i in range(len(overall1.index)):  # Coloring for Industry and Total Rows
                if overall1.index[i] in industry:
                    worksheet.conditional_format(str('C' + str(i + 6) + ':' + 'Y' + str(i + 6)),
                                                 {'type': 'cell', 'criteria': '>=', 'value': 0,
                                                  'format': industry_body})
                    worksheet.write('B' + str(i + 6), overall1.index.tolist()[i], industry_index)
                elif overall1.index[i] in ['Total']:
                    worksheet.conditional_format(str('C' + str(i + 6) + ':' + 'Y' + str(i + 6)),
                                                 {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': total_body})
                    worksheet.write('B' + str(i + 6), overall1.index.tolist()[i], total_index)
                elif overall1.index[i] in prime_util.Agency.unique():
                    worksheet.conditional_format(str('C' + str(i + 6) + ':' + 'Y' + str(i + 6)),
                                                 {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': agency_body})
                    worksheet.write('B' + str(i + 6), overall1.index.tolist()[i], agency_index)
                elif overall1.index[i] in [key for key in size_group_dict]:
                    worksheet.conditional_format(str('C' + str(i + 6) + ':' + 'Y' + str(i + 6)),
                                                 {'type': 'cell', 'criteria': '>=', 'value': 0,
                                                  'format': size_group_body})
                    worksheet.write('B' + str(i + 6), overall1.index.tolist()[i], size_group_index)

            worksheet.set_column('W:W', 5, percentage_signs)  # Percentage Signs
            worksheet.set_column('H:H', 14)
            worksheet.set_column('Y:Y', 14)

            ##################################################################
            # Printing Subs Table
            ##################################################################

            try:

                if subs_final is not None:

                    subs_final.to_excel(writer, sheet_name='3. Subs Summary Table', startrow=startrow,
                                        startcol=startcol, header=False, index=True)

                    worksheet = writer.sheets['3. Subs Summary Table']

                    worksheet.set_row(1, 17.5)
                    worksheet.set_row(2, 26)
                    worksheet.set_row(3, 22)  # about 1 point higher here than in Excel.
                    worksheet.set_row(4, 20)

                    worksheet.merge_range('B2:Y2',
                                          'Table B - FY %s Quarter %s, Sub Contracts Subject to M/WBE Program - Disaggregated by Industry' % (
                                          str(FY), str(FQ)[1]), merge_format2)
                    worksheet.merge_range('B3:B5', 'Industry and Size Group', merge_format)
                    worksheet.merge_range('C3:H3', 'MBE', merge_format)
                    worksheet.merge_range('I3:P3', 'WBE', merge_format)
                    worksheet.merge_range('C4:D4', 'Black', merge_format)
                    worksheet.merge_range('E4:F4', 'Asian', merge_format)
                    worksheet.merge_range('G4:H4', 'Hispanic', merge_format)
                    worksheet.merge_range('I4:J4', 'Black', merge_format)
                    worksheet.merge_range('K4:L4', 'Asian', merge_format)
                    worksheet.merge_range('M4:N4', 'Hispanic', merge_format)
                    worksheet.merge_range('O4:P4', 'Caucasian', merge_format)
                    worksheet.merge_range('Q3:R4', 'Non-Certified', merge_format)
                    worksheet.merge_range('S3:T4', 'Certified as Both MBE & WBE', merge_format)
                    worksheet.merge_range('U3:W4', 'Total M/WBE', merge_format)  # minority women only
                    worksheet.merge_range('X3:Y4', 'Total', merge_format)

                    worksheet.write('C' + str(startrow), '#', merge_format)
                    worksheet.write('D' + str(startrow), '$', merge_format)
                    worksheet.write('E' + str(startrow), '#', merge_format)
                    worksheet.write('F' + str(startrow), '$', merge_format)
                    worksheet.write('G' + str(startrow), '#', merge_format)
                    worksheet.write('H' + str(startrow), '$', merge_format)
                    worksheet.write('I' + str(startrow), '#', merge_format)
                    worksheet.write('J' + str(startrow), '$', merge_format)
                    worksheet.write('K' + str(startrow), '#', merge_format)
                    worksheet.write('L' + str(startrow), '$', merge_format)
                    worksheet.write('M' + str(startrow), '#', merge_format)
                    worksheet.write('N' + str(startrow), '$', merge_format)
                    worksheet.write('O' + str(startrow), '#', merge_format)
                    worksheet.write('P' + str(startrow), '$', merge_format)
                    worksheet.write('Q' + str(startrow), '#', merge_format)
                    worksheet.write('R' + str(startrow), '$', merge_format)
                    worksheet.write('S' + str(startrow), '#', merge_format)
                    worksheet.write('T' + str(startrow), '$', merge_format)
                    worksheet.write('U' + str(startrow), '#', merge_format)
                    worksheet.write('V' + str(startrow), '$', merge_format)
                    worksheet.write('W' + str(startrow), '%', merge_format)
                    worksheet.write('X' + str(startrow), '#', merge_format)
                    worksheet.write('Y' + str(startrow), '$', merge_format)

                    worksheet.set_column('D:D', 11)
                    worksheet.set_column('F:F', 11)
                    worksheet.set_column('H:H', 11)
                    worksheet.set_column('N:N', 12)
                    worksheet.set_column('P:P', 11)
                    worksheet.set_column('R:R', 13)
                    worksheet.set_column('T:T', 12)
                    worksheet.set_column('V:V', 12)
                    worksheet.set_column('U:U', 6)
                    worksheet.set_column('X:X', 6)
                    worksheet.set_column('V:V', 12)
                    worksheet.set_column('Y:Y', 12)
                    worksheet.set_column('B:B', 24)

                    for h in ['D', 'F', 'H', 'J', 'L', 'N', 'P', 'R', 'T', 'V', 'Y']:
                        worksheet.conditional_format(h + str(startrow) + ':' + h + str(startrow + subs_final.shape[0]),
                                                     {'type': 'cell', 'criteria': '>=', 'value': 0,
                                                      'format': format12})  # Gives Dollar Signs

                    for h in ['C', 'E', 'G', 'I', 'K', 'M', 'O', 'Q', 'S', 'U', 'W', 'X']:
                        worksheet.conditional_format(h + str(startrow) + ':' + h + str(startrow + subs_final.shape[0]),
                                                     {'type': 'cell', 'criteria': '>=', 'value': 0,
                                                      'format': gridlines})  # Gives Gridlines

                    worksheet.set_column('W:W', 5, percentage_signs)  # Percentage Signs

                    for i in range(len(subs_final.index)):
                        if subs_final.index[i] in industry:
                            worksheet.conditional_format(str('C' + str(i + 6) + ':' + 'Y' + str(i + 6)),
                                                         {'type': 'cell', 'criteria': '>=', 'value': 0,
                                                          'format': industry_body})
                            worksheet.write('B' + str(i + 6), subs_final.index.tolist()[i], industry_index)
                        elif subs_final.index[i] in ['Total']:
                            worksheet.conditional_format(str('C' + str(i + 6) + ':' + 'Y' + str(i + 6)),
                                                         {'type': 'cell', 'criteria': '>=', 'value': 0,
                                                          'format': total_body})
                            worksheet.write('B' + str(i + 6), subs_final.index.tolist()[i], total_index)
                        elif subs_final.index[i] in prime_util.Agency.unique():
                            worksheet.conditional_format(str('C' + str(i + 6) + ':' + 'Y' + str(i + 6)),
                                                         {'type': 'cell', 'criteria': '>=', 'value': 0,
                                                          'format': agency_body})
                            worksheet.write('B' + str(i + 6), subs_final.index.tolist()[i], agency_index)
                        elif subs_final.index[i] in [key for key in size_group_dict]:
                            worksheet.conditional_format(str('C' + str(i + 6) + ':' + 'Y' + str(i + 6)),
                                                         {'type': 'cell', 'criteria': '>=', 'value': 0,
                                                          'format': size_group_body})
                            worksheet.write('B' + str(i + 6), subs_final.index.tolist()[i], size_group_index)

                    worksheet.set_column('W:W', 5, percentage_signs)  # Percentage Signs
            except:
                pass
            #######################################################################################################################################################################
            # Printing Tab 4
            #######################################################################################################################################################################

            prime_util_a.to_excel(writer, sheet_name='4. Prime Contract Data', startrow=1, startcol=1, header=True,
                                  index=False)

            worksheet = writer.sheets['4. Prime Contract Data']

            worksheet.set_column('B:B', 13)
            worksheet.set_column('C:C', 14)
            worksheet.set_column('D:D', 15)
            worksheet.set_column('E:E', 14)
            worksheet.set_column('F:F', 22)
            worksheet.set_column('G:G', 15)
            worksheet.set_column('H:H', 17)
            worksheet.set_column('I:I', 14)
            worksheet.set_column('J:J', 25)
            worksheet.set_column('K:K', 18)
            worksheet.set_column('L:L', 18)
            worksheet.set_column('M:M', 45)
            worksheet.set_column('N:N', 115)
            worksheet.set_column('O:O', 21)
            worksheet.set_column('P:P', 19)
            worksheet.set_column('Q:Q', 23)
            worksheet.set_column('R:R', 23)
            worksheet.set_column('S:S', 20)
            worksheet.set_column('T:T', 32)
            worksheet.set_column('U:U', 23)
            worksheet.set_column('V:V', 21)
            worksheet.set_column('W:W', 16)
            worksheet.set_column('X:X', 15)
            worksheet.set_column('Y:Y', 20)
            worksheet.set_column('Z:Z', 18)
            worksheet.set_column('AA:AA', 15)
            worksheet.set_column('AB:AB', 19)
            worksheet.set_column('AC:AC', 13)
            worksheet.set_column('AD:AD', 17)
            worksheet.set_column('AE:AE', 15)
            worksheet.set_column('AF:AF', 17)
            worksheet.set_column('AG:AG', 20)
            worksheet.set_column('AH:AH', 28)
            worksheet.set_column('AI:AI', 9)
            worksheet.set_column('AK:AK', 17)
            worksheet.set_column('AL:AL', 17)
            worksheet.set_column('AM:AM', 15)
            worksheet.set_column('AN:AN', 17)
            worksheet.set_column('AO:AO', 20)
            worksheet.set_column('AP:AP', 28)
            worksheet.set_column('AQ:AQ', 12)
            worksheet.set_column('AR:AR', 16)

            #####################
            # Printing Tab 5
            #####################

            if ab_.shape[0] != 0:

                ab_.to_excel(writer, sheet_name='5. Sub Contract Data', startrow=1, startcol=1, header=True,
                             index=False)

                worksheet = writer.sheets['5. Sub Contract Data']

                worksheet.set_column('B:B', 8)
                worksheet.set_column('C:C', 20)
                worksheet.set_column('D:D', 24)
                worksheet.set_column('E:E', 25)
                worksheet.set_column('F:F', 26)
                worksheet.set_column('G:G', 22)
                worksheet.set_column('H:H', 25)
                worksheet.set_column('I:I', 26)
                worksheet.set_column('J:J', 20)
                worksheet.set_column('K:K', 29)
                worksheet.set_column('L:L', 19)
                worksheet.set_column('M:M', 19)
                worksheet.set_column('N:N', 15)
                worksheet.set_column('O:O', 23)
                worksheet.set_column('P:P', 44)
                worksheet.set_column('Q:Q', 24)
                worksheet.set_column('R:R', 22)
                worksheet.set_column('S:S', 16)
                worksheet.set_column('T:T', 66)
                worksheet.set_column('U:U', 28)
                worksheet.set_column('V:V', 25)
            else:
                pass

            writer.save()

            try:
                del subs_final
            except:
                pass

            try:
                del ab_
            except:
                pass

#Compliance Report Appendices A and B
if __name__ == "__main__":

    prime_util = primes_post_master

    try:
        prime_ebe = prime_util[prime_util['VendorNumber'].isin(ebe['FMS Vendor Number'])]
    except:
        pass
        primes = prime_util

    sub_util = subs_post_master

    sub_util = sub_util[sub_util['ReportCategory'] != str(0)]

    sub_util = sub_util.drop_duplicates()

    empty = []
    shape = []
    empty2 = []
    agency_total_list = []

    sg = pd.DataFrame(prime_util.SizeGroup.unique())
    ind = pd.DataFrame(prime_util.Industry.unique())

    MECE_SizeGroups = pd.DataFrame(['>$1M, <=$5M', 'Micro Purchase', '>$100K, <=$1M', 'Small Purchase', '>$5M, <=$25M', '>$25M'])
    MECE_Industries = pd.DataFrame(['Standardized Services', 'Goods', 'Professional Services', 'Construction Services'])

    if set(sg) == set(MECE_SizeGroups) & set(ind) == set(MECE_Industries):

        industry = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']

        # MBE - Black
        a1_1 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
            ['Industry', 'SizeGroup']).ContractID.nunique()
        a1_1.name = 'MBE Black (#)'
        a1_2 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
            ['Industry', 'SizeGroup']).ContractValue.sum()
        a1_2.name = 'MBE Black ($)'

        # MBE - Asian
        a2_1 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
            ['Industry', 'SizeGroup']).ContractID.nunique()
        a2_1.name = 'MBE Asian (#)'
        a2_2 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
            ['Industry', 'SizeGroup']).ContractValue.sum()
        a2_2.name = 'MBE Asian ($)'

        # MBE - Hispanic
        a3_1 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
            ['Industry', 'SizeGroup']).ContractID.nunique()
        a3_1.name = 'MBE Hispanic (#)'
        a3_2 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
            ['Industry', 'SizeGroup']).ContractValue.sum()
        a3_2.name = 'MBE Hispanic ($)'

        # WBE - Black
        a4_1 = primes[primes['ReportCategory'] == 'WBE - Black'].groupby(
            ['Industry', 'SizeGroup']).ContractID.nunique()
        a4_1.name = 'WBE Black (#)'
        a4_2 = primes[primes['ReportCategory'] == 'WBE - Black'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
        a4_2.name = 'WBE Black ($)'

        # WBE - Asian
        a5_1 = primes[primes['ReportCategory'] == 'WBE - Asian'].groupby(
            ['Industry', 'SizeGroup']).ContractID.nunique()
        a5_1.name = 'WBE Asian (#)'
        a5_2 = primes[primes['ReportCategory'] == 'WBE - Asian'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
        a5_2.name = 'WBE Asian ($)'

        # WBE Hispanic
        a6_1 = primes[primes['ReportCategory'] == 'WBE - Hispanic'].groupby(
            ['Industry', 'SizeGroup']).ContractID.nunique()
        a6_1.name = 'WBE Hispanic (#)'
        a6_2 = primes[primes['ReportCategory'] == 'WBE - Hispanic'].groupby(
            ['Industry', 'SizeGroup']).ContractValue.sum()
        a6_2.name = 'WBE Hispanic ($)'

        # # WBE Caucasian
        a7_1 = primes[primes['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
            ['Industry', 'SizeGroup']).ContractID.nunique()
        a7_1.name = 'WBE - Caucasian (#)'
        a7_2 = primes[primes['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
            ['Industry', 'SizeGroup']).ContractValue.sum()
        a7_2.name = 'WBE - Caucasian ($)'

        # Non Certified
        a8_1 = primes[primes['ReportCategory'].isnull()].groupby(
            ['Industry', 'SizeGroup']).ContractID.nunique()
        a8_1.name = 'Non-Certified (#)'

        a8_2 = primes[primes['ReportCategory'].isnull()].groupby(
            ['Industry', 'SizeGroup']).ContractValue.sum()
        a8_2.name = 'Non-Certified ($)'

        # MBE and WBE
        a9_1 = primes[primes['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
            ['Industry', 'SizeGroup']).ContractID.count()
        a9_1.name = 'Certified as Both MBE and WBE (#)'

        a9_2 = primes[primes['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
            ['Industry', 'SizeGroup']).ContractValue.sum()
        a9_2.name = 'Certified as Both MBE and WBE ($)'

        # Total M/WBE
        a10_1 = (((a1_1.fillna(0) + a2_1.fillna(0)).fillna(0) + a3_1.fillna(0)).fillna(0) + a7_1).fillna(0)
        a10_1.name = 'Total M/WBE (#)'
        a10_2 = primes[primes['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
        a10_2.name = 'Total M/WBE ($)'

        # Total
        a11_1 = primes.groupby(['Industry', 'SizeGroup']).ContractValue.sum()
        a11_1.name = 'Total ($)'
        a11_2 = primes.groupby(['Industry', 'SizeGroup']).ContractID.nunique()
        a11_2.name = 'Total (#)'
        #
        a10_3 = a10_2 / a11_1
        a10_3.name = 'Total MWBE (%)'

        copy = a10_3

        try:
            col1 = prime_ebe.groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            col1.name = 'EBE (#)'
        except:
            col1 = copy
            col1.iloc[:] = 0
            col1.name = 'EBE (#)'

        try:
            col2 = prime_ebe.groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            col2.name = 'EBE ($)'
        except:
            col2 = copy
            col2.iloc[:] = 0
            col2.name = 'EBE ($)'

        a10_3 = a10_2 / a11_1
        a10_3.name = 'Total MWBE (%)'

        df = pd.concat([a1_1, a1_2, a2_1, a2_2, a3_1, a3_2, a4_1, a4_2, a5_1, a5_2, a6_1, a6_2, a7_1, a7_2, a8_1, a8_2, col1, col2, a9_1, a9_2, a10_1, a10_2, a10_3, a11_2, a11_1], axis=1)

        df.columns = [u'MBE Black (#)', u'MBE Black ($)', u'MBE Asian (#)', u'MBE Asian ($)', u'MBE Hispanic (#)', u'MBE Hispanic ($)', u'WBE Black (#)', u'WBE Black ($)', u'WBE Asian (#)', u'WBE Asian ($)',u'WBE Hispanic (#)', u'WBE Hispanic ($)', u'WBE - Caucasian (#)',u'WBE - Caucasian ($)', u'Non-Certified (#)', u'Non-Certified ($)',u'EBE (#)', u'EBE ($)', u'Certified as Both MBE and WBE (#)',u'Certified as Both MBE and WBE ($)', u'Total M/WBE (#)',u'Total M/WBE ($)', u'Total MWBE (%)', u'Total (#)', u'Total ($)']

        df = df.fillna(0)

        df_columns = df.columns

        size_group_dict = {'Micro Purchase': 1, 'Small Purchase': 2, '>$100K, <=$1M': 3, '>$1M, <=$5M': 4,
                           '>$5M, <=$25M': 5,
                           '>$25M': 6}  # This data frame has all the info, but the order is WRONG and labels are WRONG

        for j in industry:
            if j in df.index.levels[0].tolist():  # df is a multi index level
                index_order = {'Total': 'A', 'Micro Purchase': 'B', 'Small Purchase': 'C', '>$100K, <=$1M': 'D',
                               '>$1M, <=$5M': 'E', '>$5M, <=$25M': 'F', '>$25M': 'G'}

                index_dict = {'A': str(j), 'B': 'Micro Purchase', 'C': 'Small Purchase', 'D': '>$100K, <=$1M',
                              'E': '>$1M, <=$5M', 'F': '>$5M, <=$25M', 'G': '>$25M'}

                df_industry = df.loc[(j, df.index.levels[1].tolist()), :]  # sliced it from the original
                industry_total_row = df_industry.sum()
                industry_total_row = pd.DataFrame(industry_total_row)
                industry_total_row = industry_total_row.T

                df_zeros = pd.DataFrame(np.zeros((6 - df_industry.shape[0], df_industry.shape[1])),
                                        columns=df_industry.columns, index=[(j, y) for y in
                                                                            ['Micro Purchase', 'Small Purchase',
                                                                             '>$100K, <=$1M',
                                                                             '>$1M, <=$5M', '>$5M, <=$25M', '>$25M'] if
                                                                            y not in [x[1] for x in df_industry.index if
                                                                                      isinstance(x, tuple)]])

                df_industry = pd.concat([df_industry, df_zeros])

                df_industry = pd.concat([industry_total_row, df_industry], axis=0)

                df_industry.index.values[0] = tuple([str(j), 'Total'])

                df_industry.index = [index_order.get(df_industry.index.values[x][1]) for x in
                                     range(len(df_industry.index.values))]

                df_industry = df_industry.sort_index(ascending=True)

                industry_shape = df_industry.shape

                shape.append(industry_shape)

                df_industry.index = [index_dict.get(df_industry.index.values[x]) for x in range(len(df_industry.index))]

                empty.append(df_industry)

            else:
                if j not in df.index.levels[0].unique().tolist():
                    df_industry = pd.DataFrame(np.zeros((7, df.shape[1])), columns=df.columns,
                                               index=[j, 'Micro Purchase', 'Small Purchase', '>$100K, <=$1M',
                                                      '>$1M, <=$5M',
                                                      '>$5M, <=$25M', '>$25M'])
                empty.append(df_industry)

        final = pd.concat(empty)

        empty = []

        final.name = 'Industry and Size Group'

        final['Total MWBE (%)'] = final['Total M/WBE ($)'].astype(float) / final['Total ($)'].astype(float)

        final = final.fillna(0)  # The Order and Labels are Correct, but there is No Total SubSection

        index_order = {'Total': 'A', 'Micro Purchase': 'B', 'Small Purchase': 'C', '>$100K, <=$1M': 'D',
                       '>$1M, <=$5M': 'E', '>$5M, <=$25M': 'F', '>$25M': 'G'}

        list = []
        index_list = []

        try:
            micro = pd.DataFrame(final.loc['Micro Purchase', :].sum()).T
            list.append(micro)
            index_list.append('Micro Purchase')
        except (KeyError):
            pass
        try:
            small = pd.DataFrame(final.loc['Small Purchase', :].sum()).T
            list.append(small)
            index_list.append('Small Purchase')
        except (KeyError):
            pass
        try:
            level1 = pd.DataFrame(final.loc['>$100K, <=$1M', :].astype(float).sum()).T
            list.append(level1)
            index_list.append('>$100K, <=$1M')
        except (KeyError):
            pass
        try:
            level2 = pd.DataFrame(final.loc['>$1M, <=$5M', :].astype(float).sum()).T
            list.append(level2)
            index_list.append('>$1M, <=$5M')
        except (KeyError):
            pass
        try:
            level3 = pd.DataFrame(final.loc['>$5M, <=$25M', :].astype(float).sum()).T
            list.append(level3)
            index_list.append('>$5M, <=$25M')
        except (KeyError):
            pass
        try:
            level4 = pd.DataFrame(final.loc['>$25M', :].astype(float).sum()).T
            list.append(level4)
            index_list.append('>$25M')
        except (KeyError):
            pass

        df = pd.concat(list)

        df.index = index_list

        tot = pd.DataFrame(df.sum()).T
        tot.index = ['Total']

        total_portion = pd.concat([tot, df])

        total_portion['Total MWBE (%)'] = total_portion['Total M/WBE ($)'] / total_portion['Total ($)']

        primes_industry_summary = pd.concat([final, total_portion])

        primes_industry_summary['Total M/WBE (#)'] = primes_industry_summary['MBE Black (#)'] + primes_industry_summary[
            'MBE Asian (#)'] + primes_industry_summary['MBE Hispanic (#)'] + primes_industry_summary[
                                                         'WBE - Caucasian (#)']

        primes_industry_summary['Certified as Both MBE and WBE (#)'] = primes_industry_summary['WBE Black (#)'] + \
                                                                       primes_industry_summary['WBE Asian (#)'] + \
                                                                       primes_industry_summary['WBE Hispanic (#)']

        primes_industry_summary['Total M/WBE ($)'] = primes_industry_summary['MBE Black ($)'] + primes_industry_summary[
            'MBE Asian ($)'] + primes_industry_summary['MBE Hispanic ($)'] + primes_industry_summary[
                                                         'WBE - Caucasian ($)']

        primes_industry_summary['Certified as Both MBE and WBE ($)'] = primes_industry_summary['WBE Black ($)'] + \
                                                                       primes_industry_summary['WBE Asian ($)'] + \
                                                                       primes_industry_summary['WBE Hispanic ($)']

        ##############################################################
        #
        empty = []
        shape = []
        empty2 = []
        agency_total_list = []

        for i in sorted(primes['Agency'].unique()):

            pw_ = primes[primes['Agency'] == i]
            pw_ebe = prime_ebe[prime_ebe['Agency'] == i]

            if pw_.shape[0] > 0:

                industry = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']

                # MBE - Black
                a1_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
                    ['Industry', 'SizeGroup']).ContractID.nunique()
                a1_1.name = 'MBE Black (#)'
                a1_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
                    ['Industry', 'SizeGroup']).ContractValue.sum()
                a1_2.name = 'MBE Black ($)'

                # MBE - Asian
                a2_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
                    ['Industry', 'SizeGroup']).ContractID.nunique()
                a2_1.name = 'MBE Asian (#)'
                a2_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
                    ['Industry', 'SizeGroup']).ContractValue.sum()
                a2_2.name = 'MBE Asian ($)'

                # MBE - Hispanic American
                a3_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
                    ['Industry', 'SizeGroup']).ContractID.nunique()
                a3_1.name = 'MBE Hispanic (#)'
                a3_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
                    ['Industry', 'SizeGroup']).ContractValue.sum()
                a3_2.name = 'MBE Hispanic ($)'

                # WBE - Black
                a4_1 = pw_[pw_['ReportCategory'] == 'WBE - Black'].groupby(
                    ['Industry', 'SizeGroup']).ContractID.nunique()
                a4_1.name = 'WBE Black (#)'
                a4_2 = pw_[pw_['ReportCategory'] == 'WBE - Black'].groupby(
                    ['Industry', 'SizeGroup']).ContractValue.sum()
                a4_2.name = 'WBE Black ($)'

                # WBE - Asian
                a5_1 = pw_[pw_['ReportCategory'] == 'WBE - Asian'].groupby(
                    ['Industry', 'SizeGroup']).ContractID.nunique()
                a5_1.name = 'WBE Asian (#)'
                a5_2 = pw_[pw_['ReportCategory'] == 'WBE - Asian'].groupby(
                    ['Industry', 'SizeGroup']).ContractValue.sum()
                a5_2.name = 'WBE Asian ($)'

                # WBE Hispanic
                a6_1 = pw_[pw_['ReportCategory'] == 'WBE - Hispanic'].groupby(
                    ['Industry', 'SizeGroup']).ContractID.nunique()
                a6_1.name = 'WBE Hispanic (#)'
                a6_2 = pw_[pw_['ReportCategory'] == 'WBE - Hispanic'].groupby(
                    ['Industry', 'SizeGroup']).ContractValue.sum()
                a6_2.name = 'WBE Hispanic ($)'

                # WBE Caucasian
                a7_1 = pw_[pw_['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
                    ['Industry', 'SizeGroup']).ContractID.nunique()
                a7_1.name = 'WBE - Caucasian (#)'
                a7_2 = pw_[pw_['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
                    ['Industry', 'SizeGroup']).ContractValue.sum()
                a7_2.name = 'WBE - Caucasian ($)'

                # Non Certified
                a8_1 = pw_[pw_['ReportCategory'].isnull()].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
                a8_1.name = 'Non-Certified (#)'
                a8_2 = pw_[pw_['ReportCategory'].isnull()].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
                a8_2.name = 'Non-Certified ($)'

                # MBE and WBE
                a9_1 = ((a4_1 + a5_1).fillna(0) + a6_1).fillna(0)
                a9_1.name = 'Certified as Both MBE and WBE (#)'
                a9_2 = pw_[pw_['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
                    ['Industry', 'SizeGroup']).ContractValue.sum()
                a9_2.name = 'Certified as Both MBE and WBE ($)'

                # Total M/WBE
                a10_1 = (((a1_1.fillna(0) + a2_1.fillna(0)).fillna(0) + a3_1.fillna(0)).fillna(0) + a7_1).fillna(0)
                a10_1.name = 'Total M/WBE (#)'
                a10_2 = pw_[pw_['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
                a10_2.name = 'Total M/WBE ($)'

                # Total
                a11_1 = pw_.groupby(['Industry', 'SizeGroup']).ContractValue.sum()
                a11_1.name = 'Total ($)'
                a11_2 = pw_.groupby(['Industry', 'SizeGroup']).ContractID.nunique()
                a11_2.name = 'Total (#)'

                try:
                    a10_3 = a10_2 / a11_1
                    a10_3.name = 'Total MWBE (%)'
                except:
                    col = np.zeros(shape=(len(a11_1), 1))

                col = np.zeros(shape=(len(a11_1), 1))

                try:
                    col1 = pw_ebe.groupby(['Industry', 'SizeGroup']).ContractID.nunique()
                    col1.name = 'EBE (#)'
                except:
                    col1 = a10_3
                    col1.iloc[:] = 0
                    col1.name = 'EBE (#)'
                    pass

                try:
                    col2 = pw_ebe.groupby(['Industry', 'SizeGroup']).ContractValue.sum()
                    col2.name = 'EBE ($)'
                except:
                    col2 = a10_3
                    col2.iloc[:] = 0
                    col2.name = 'EBE ($)'

                try:
                    a10_3 = a10_2 / a11_1
                    a10_3.name = 'Total MWBE (%)'
                except:
                    col = np.zeros(shape=(len(a11_1), 1))

                df = pd.concat([a1_1, a1_2, a2_1, a2_2, a3_1, a3_2, a4_1, a4_2, a5_1, a5_2, a6_1, a6_2, a7_1, a7_2, a8_1, a8_2, col1, col2, a9_1, a9_2, a10_1, a10_2, a10_3, a11_2, a11_1], axis=1)

                df.columns = [u'MBE Black (#)', u'MBE Black ($)', u'MBE Asian (#)', u'MBE Asian ($)',
                              u'MBE Hispanic (#)', u'MBE Hispanic ($)', u'WBE Black (#)', u'WBE Black ($)',
                              u'WBE Asian (#)', u'WBE Asian ($)', u'WBE Hispanic (#)', u'WBE Hispanic ($)',
                              u'WBE - Caucasian (#)', u'WBE - Caucasian ($)', u'Non-Certified (#)',
                              u'Non-Certified ($)', u'EBE (#)', u'EBE ($)', u'Certified as Both MBE and WBE (#)',
                              u'Certified as Both MBE and WBE ($)', u'Total M/WBE (#)', u'Total M/WBE ($)',
                              u'Total MWBE (%)', u'Total (#)', u'Total ($)']

                df = df.fillna(0)

                size_group_dict = {'Micro Purchase': 1, 'Small Purchase': 2, '>$100K, <=$1M': 3, '>$1M, <=$5M': 4,
                                   '>$5M, <=$25M': 5,
                                   '>$25M': 6}  # This data frame has all the info, but the order is WRONG and labels are WRONG

                for j in industry:  # df is a multi index level
                    if j in df.index.levels[0].unique().tolist():
                        index_order = {'Total': 'A', 'Micro Purchase': 'B', 'Small Purchase': 'C', '>$100K, <=$1M': 'D',
                                       '>$1M, <=$5M': 'E', '>$5M, <=$25M': 'F', '>$25M': 'G'}

                        index_dict = {'A': str(j), 'B': 'Micro Purchase', 'C': 'Small Purchase', 'D': '>$100K, <=$1M',
                                      'E': '>$1M, <=$5M', 'F': '>$5M, <=$25M', 'G': '>$25M'}

                        df_industry = df.loc[(j, df.index.levels[1].tolist()), :]  # sliced it from the original
                        industry_total_row = df_industry.sum()
                        industry_total_row = pd.DataFrame(industry_total_row)
                        industry_total_row = industry_total_row.T

                        df_zeros = pd.DataFrame(np.zeros((6 - df_industry.shape[0], df_industry.shape[1])),
                                                columns=df_industry.columns, index=[(j, y) for y in
                                                                                    ['Micro Purchase', 'Small Purchase',
                                                                                     '>$100K, <=$1M',
                                                                                     '>$1M, <=$5M', '>$5M, <=$25M',
                                                                                     '>$25M']
                                                                                    if
                                                                                    y not in [x[1] for x in
                                                                                              df_industry.index if
                                                                                              isinstance(x, tuple)]])

                        df_industry = pd.concat([df_industry, df_zeros])

                        df_industry = pd.concat([industry_total_row, df_industry], axis=0)

                        df_industry.index.values[0] = tuple([str(j), 'Total'])

                        df_industry.index = [index_order.get(df_industry.index.values[x][1]) for x in
                                             range(len(df_industry.index.values))]

                        df_industry = df_industry.sort_index(ascending=True)

                        industry_shape = df_industry.shape

                        shape.append(industry_shape)

                        df_industry.index = [index_dict.get(df_industry.index.values[x]) for x in
                                             range(len(df_industry.index))]

                        empty.append(df_industry)

                    else:
                        if j not in df.index.levels[0].unique().tolist():
                            df_industry = pd.DataFrame(np.zeros((7, df.shape[1])), columns=df.columns,
                                                       index=[j, 'Micro Purchase', 'Small Purchase', '>$100K, <=$1M',
                                                              '>$1M, <=$5M', '>$5M, <=$25M', '>$25M'])
                            empty.append(df_industry)

                final = pd.concat(empty)

                empty = []

                final.name = 'Industry and Size Group'

                final['Total MWBE (%)'] = final['Total M/WBE ($)'].astype(float) / final['Total ($)'].astype(float)

                final = final.fillna(0)  # The Order and Labels are Correct, but there is No Total SubSection

            total = []

            for z in industry:
                if z in df.index.levels[0].unique().tolist():
                    total.append(final.loc[z])

            df_test = pd.concat(total, axis=1)

            tot = pd.DataFrame(df_test.sum(axis=1)).T

            tot.index = [str(i)]

            tot['Total MWBE (%)'] = tot['Total M/WBE ($)'] / tot['Total ($)']

            agency_total_list.append(tot)

            overall1 = pd.concat([tot, final])

            empty2.append(overall1)

        tab2_final = pd.concat(empty2)

        agency_total = pd.concat(agency_total_list).sum()

        agency_total.name = 'Total'

        bottom_total_row = pd.DataFrame(agency_total).T

        bottom_total_row['Total MWBE (%)'] = bottom_total_row['Total M/WBE ($)'] / bottom_total_row['Total ($)']

        tab2_final = pd.concat([tab2_final, bottom_total_row])

        tab2_final['Total M/WBE (#)'] = tab2_final['MBE Black (#)'] + tab2_final['MBE Asian (#)'] + tab2_final[
            'MBE Hispanic (#)'] + tab2_final['WBE - Caucasian (#)']

        tab2_final['Certified as Both MBE and WBE (#)'] = tab2_final['WBE Black (#)'] + tab2_final['WBE Asian (#)'] + \
                                                          tab2_final['WBE Hispanic (#)']

        tab2_final['Total M/WBE ($)'] = tab2_final['MBE Black ($)'].astype(float) + tab2_final['MBE Asian ($)'].astype(
            float) + tab2_final[
                                            'MBE Hispanic ($)'].astype(float) + tab2_final[
                                            'WBE - Caucasian ($)'].astype(float)

        tab2_final['Certified as Both MBE and WBE ($)'] = tab2_final['WBE Black ($)'].astype(float) + tab2_final[
            'WBE Asian ($)'].astype(float) + tab2_final['WBE Hispanic ($)'].astype(float)

        path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Compliance Appendices'

        writer = pd.ExcelWriter(path + '\\' + r'Table A and B.Prime Contract MWBE Utilization FY%s %s %s.xlsx' % (str(FY)[2:4], FQ, str(t)), engine='xlsxwriter')

        workbook = writer.book

        startrow = 6
        startcol = 1

        primes_industry_summary.to_excel(writer, sheet_name='Table A - Primes Ind. Summary', startrow=startrow,
                                         startcol=startcol, index=True, header=False)

        worksheet = writer.sheets['Table A - Primes Ind. Summary']

        worksheet.set_row(2, 18)

        merge_format = workbook.add_format({
            'bold': 1,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': '#daeef3'})

        merge_format.set_text_wrap()
        merge_format.set_font_size(11)

        merge_format1 = workbook.add_format({
            'bold': 1,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': '#daeef3', 'size': 11})

        merge_format2 = workbook.add_format({
            'bold': 1,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': '#16365C', 'size': 11})

        merge_format2.set_font_color('white')

        size_group_body = workbook.add_format({'align': 'right', 'size': 11})
        size_group_body.set_right(1)
        size_group_body.set_left(1)
        size_group_body.set_top(1)
        size_group_body.set_bottom(1)

        size_group_index = workbook.add_format({'bold': 1, 'align': 'left', 'size': 11})
        size_group_index.set_right(1)
        size_group_index.set_left(1)
        size_group_index.set_top(1)
        size_group_index.set_bottom(1)

        industry_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

        industry_index.set_bg_color('#FDE9D9')
        industry_index.set_right(1)
        industry_index.set_left(1)
        industry_index.set_top(1)
        industry_index.set_bottom(1)

        industry_body = workbook.add_format({'bold': 1,
                                             'align': 'center',
                                             'size': 11})

        industry_body.set_bg_color('#FDE9D9')

        total_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

        total_index.set_bg_color('#e26b0a')
        total_index.set_right(1)
        total_index.set_left(1)
        total_index.set_top(1)
        total_index.set_bottom(1)

        total_body = workbook.add_format({'bold': 1,
                                          'align': 'center',
                                          'size': 11})

        total_body.set_bg_color('#e26b0a')

        format5 = workbook.add_format({'bold': 1,
                                       'align': 'center', 'size': 11})

        format5.set_bg_color('#FFCC99')
        format5.set_right(1)
        format5.set_left(1)
        format5.set_top(1)
        format5.set_bottom(1)

        format6 = workbook.add_format({'fg_color': '#FDE9D9', 'bold': 1, 'size': 11})
        format6.set_bottom(1)
        format6.set_top(1)

        format7 = workbook.add_format({'size': 11})

        format8 = workbook.add_format({'bold': 1,
                                       'size': 11})
        format8.set_bg_color('#E26B0A')

        format9 = workbook.add_format({'bold': 1,
                                       'align': 'center',
                                       'size': 11})
        format9.set_border(0)

        format10 = workbook.add_format({'bold': 1,
                                        'align': 'center',
                                        'size': 11})

        format10.set_bg_color('#FDE9D9')

        format11 = workbook.add_format({'bold': 1,
                                        'align': 'center', 'size': 11})

        format11.set_bg_color('#FFCC99')

        format12 = workbook.add_format({'bold': 1,
                                        'align': 'center',
                                        'size': 11})

        format12.set_bg_color('#FFCC99')
        format12.set_right(1)
        format12.set_left(1)
        format12.set_top(1)
        format12.set_bottom(1)

        format12_ = workbook.add_format({'bold': 1,
                                         'align': 'center',
                                         'size': 11})

        format12_.set_bg_color('#FFCC99')

        dollar_signs = workbook.add_format({'num_format': '$###,###,###,##0'})
        percentage_signs = workbook.add_format({'num_format': '0%'})
        gridlines = workbook.add_format({'border': 1})

        worksheet.merge_range('B3:AA3','Table A - FY%s Quarter %s Prime Contracts Subject to M/WBE Program - Disaggregated by Industry' % (str(FY)[2:4], FQ[1]), merge_format2)
        worksheet.merge_range('C4:H4', 'MBE', merge_format)
        worksheet.merge_range('B4:B6', 'Industry and Size Group', merge_format)
        worksheet.merge_range('I4:P4', 'WBE', merge_format)
        worksheet.merge_range('C5:D5', 'Black', merge_format)
        worksheet.merge_range('E5:F5', 'Asian', merge_format)
        worksheet.merge_range('G5:H5', 'Hispanic', merge_format)
        worksheet.merge_range('I5:J5', 'Black', merge_format)
        worksheet.merge_range('K5:L5', 'Asian', merge_format)
        worksheet.merge_range('M5:N5', 'Hispanic', merge_format)
        worksheet.merge_range('O5:P5', 'Caucasian', merge_format)
        worksheet.merge_range('Q4:R5', 'Non-certified', merge_format)
        worksheet.merge_range('S4:T5', 'EBE', merge_format)
        worksheet.merge_range('U4:V5', 'Certified as Both MBE and WBE', merge_format)
        worksheet.merge_range('W4:Y5', 'Total M/WBE', merge_format)
        worksheet.merge_range('Z4:AA5', 'Total', merge_format)

        worksheet.write('C6', '#', merge_format)
        worksheet.write('D6', '$', merge_format)
        worksheet.write('E6', '#', merge_format)
        worksheet.write('F6', '$', merge_format)
        worksheet.write('G6', '#', merge_format)
        worksheet.write('H6', '$', merge_format)
        worksheet.write('I6', '#', merge_format)
        worksheet.write('J6', '$', merge_format)
        worksheet.write('K6', '#', merge_format)
        worksheet.write('L6', '$', merge_format)
        worksheet.write('M6', '#', merge_format)
        worksheet.write('N6', '$', merge_format)
        worksheet.write('O6', '#', merge_format)
        worksheet.write('P6', '$', merge_format)
        worksheet.write('Q6', '#', merge_format)
        worksheet.write('R6', '$', merge_format)
        worksheet.write('S6', '#', merge_format)
        worksheet.write('T6', '$', merge_format)
        worksheet.write('U6', '#', merge_format)
        worksheet.write('V6', '$', merge_format)
        worksheet.write('W6', '#', merge_format)
        worksheet.write('X6', '$', merge_format)
        worksheet.write('Y6', '%', merge_format)
        worksheet.write('Z6', '#', merge_format)
        worksheet.write('AA6', '$', merge_format)

        worksheet.set_column('B:B', 24)

        industry = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']

        for i in [primes_industry_summary]:
            for h, j in [['D', 'MBE Black ($)'], ['F', 'MBE Asian ($)'], ['H', 'MBE Hispanic ($)'],
                         ['J', 'WBE Black ($)'], ['L', 'WBE Asian ($)'], ['N', 'WBE Hispanic ($)'],
                         ['P', 'WBE - Caucasian ($)'], ['R', 'Non-Certified ($)'], ['T', 'EBE ($)'],
                         ['V', 'Certified as Both MBE and WBE ($)'], ['X', 'Total M/WBE ($)'], ['AA', 'Total ($)']]:
                worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1), len(str(i[j].max())) + 3)
                worksheet.conditional_format(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0]),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': dollar_signs})
            for h, j in [['C', 'MBE Black (#)'], ['E', 'MBE Asian (#)'], ['G', 'MBE Hispanic (#)'],
                         ['I', 'WBE Black (#)'], ['K', 'WBE Asian (#)'], ['M', 'WBE Hispanic (#)'],
                         ['O', 'WBE - Caucasian (#)'], ['Q', 'Non-Certified (#)'], ['S', 'EBE (#)'],
                         ['U', 'Certified as Both MBE and WBE (#)'], ['W', 'Total M/WBE (#)'], ['Z', 'Total (#)']]:
                worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1),
                                     len(str(i[j].max())) + 2.0)
            for k, l in [['Y', 'Total MWBE (%)']]:
                worksheet.set_column(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0] - 1),
                                     len(str(int(i[l].max()))) + 3.5)
                worksheet.conditional_format(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0]),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': percentage_signs})

        for i in range(len(primes_industry_summary.index)):
            if primes_industry_summary.index[i] in industry:
                worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': industry_body})
                worksheet.write('B' + str(i + 7), primes_industry_summary.index.tolist()[i], industry_index)
            elif primes_industry_summary.index[i] in ['Total']:
                worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': total_body})
                worksheet.write('B' + str(i + 7), primes_industry_summary.index.tolist()[i], total_index)
            elif primes_industry_summary.index[i] in primes.Agency.unique():
                worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': industry_body})
                worksheet.write('B' + str(i + 7), primes_industry_summary.index.tolist()[i], industry_index)
            elif primes_industry_summary.index[i] in [key for key in size_group_dict]:
                worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': size_group_body})
                worksheet.write('B' + str(i + 7), primes_industry_summary.index.tolist()[i], size_group_index)

        worksheet.conditional_format('C' + str(startrow + 1) + ':AA' + str(startrow + primes_industry_summary.shape[0]),
                                     {'type': 'no_errors', 'format': gridlines})

        ########################################################

        tab2_final.to_excel(writer, sheet_name='Table B - Primes by Agency', startrow=startrow, startcol=startcol,
                            index=True, header=False)

        worksheet = writer.sheets['Table B - Primes by Agency']

        worksheet.set_row(2, 18)

        merge_format = workbook.add_format({
            'bold': 1,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': '#daeef3'})

        merge_format.set_text_wrap()
        merge_format.set_font_size(11)

        merge_format1 = workbook.add_format({
            'bold': 1,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': '#daeef3', 'size': 11})

        merge_format2 = workbook.add_format({
            'bold': 1,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': '#16365C', 'size': 11})

        merge_format2.set_font_color('white')

        size_group_body = workbook.add_format({'align': 'right', 'size': 11})
        size_group_body.set_right(1)
        size_group_body.set_left(1)
        size_group_body.set_top(1)
        size_group_body.set_bottom(1)

        size_group_index = workbook.add_format({'bold': 1, 'align': 'left', 'size': 11})
        size_group_index.set_right(1)
        size_group_index.set_left(1)
        size_group_index.set_top(1)
        size_group_index.set_bottom(1)

        industry_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

        industry_index.set_bg_color('#FDE9D9')
        industry_index.set_right(1)
        industry_index.set_left(1)
        industry_index.set_top(1)
        industry_index.set_bottom(1)

        industry_body = workbook.add_format({'bold': 1,
                                             'align': 'center',
                                             'size': 11})

        industry_body.set_bg_color('#FDE9D9')

        agency_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

        agency_index.set_bg_color('#FFCC99')
        agency_index.set_right(1)
        agency_index.set_left(1)
        agency_index.set_top(1)
        agency_index.set_bottom(1)

        agency_body = workbook.add_format({'bold': 1,
                                           'align': 'center',
                                           'size': 11})

        agency_body.set_bg_color('#FFCC99')

        total_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

        total_index.set_bg_color('#e26b0a')
        total_index.set_right(1)
        total_index.set_left(1)
        total_index.set_top(1)
        total_index.set_bottom(1)

        total_body = workbook.add_format({'bold': 1,
                                          'align': 'center',
                                          'size': 11})

        total_body.set_bg_color('#e26b0a')

        format5 = workbook.add_format({'bold': 1,
                                       'align': 'center', 'size': 11})

        format5.set_bg_color('#FFCC99')
        format5.set_right(1)
        format5.set_left(1)
        format5.set_top(1)
        format5.set_bottom(1)

        format6 = workbook.add_format({'fg_color': '#FDE9D9', 'bold': 1, 'size': 11})
        format6.set_bottom(1)
        format6.set_top(1)

        format7 = workbook.add_format({'size': 11})

        format8 = workbook.add_format({'bold': 1,
                                       'size': 11})
        format8.set_bg_color('#E26B0A')

        format9 = workbook.add_format({'bold': 1,
                                       'align': 'center',
                                       'size': 11})
        format9.set_border(0)

        format10 = workbook.add_format({'bold': 1,
                                        'align': 'center',
                                        'size': 11})

        format10.set_bg_color('#FDE9D9')

        format11 = workbook.add_format({'bold': 1,
                                        'align': 'center', 'size': 11})

        format11.set_bg_color('#FFCC99')

        format12 = workbook.add_format({'bold': 1,
                                        'align': 'center',
                                        'size': 11})

        format12.set_bg_color('#FFCC99')
        format12.set_right(1)
        format12.set_left(1)
        format12.set_top(1)
        format12.set_bottom(1)

        format12_ = workbook.add_format({'bold': 1,
                                         'align': 'center',
                                         'size': 11})

        format12_.set_bg_color('#FFCC99')

        dollar_signs = workbook.add_format({'num_format': '$###,###,###,##0'})
        percentage_signs = workbook.add_format({'num_format': '0%'})
        gridlines = workbook.add_format({'border': 1})

        worksheet.merge_range('B3:AA3', 'Table B - FY%s Quarter %s' % (str(FY)[2:4], FQ[1]) + ' Prime Contracts Subject to M/WBE Program - Disaggregated by Agency', merge_format2)
        worksheet.merge_range('C4:H4', 'MBE', merge_format)
        worksheet.merge_range('B4:B6', 'Industry and Size Group', merge_format)
        worksheet.merge_range('I4:P4', 'WBE', merge_format)
        worksheet.merge_range('C5:D5', 'Black', merge_format)
        worksheet.merge_range('E5:F5', 'Asian', merge_format)
        worksheet.merge_range('G5:H5', 'Hispanic', merge_format)
        worksheet.merge_range('I5:J5', 'Black', merge_format)
        worksheet.merge_range('K5:L5', 'Asian', merge_format)
        worksheet.merge_range('M5:N5', 'Hispanic', merge_format)
        worksheet.merge_range('O5:P5', 'Caucasian', merge_format)
        worksheet.merge_range('Q4:R5', 'Non-certified', merge_format)
        worksheet.merge_range('S4:T5', 'EBE', merge_format)
        worksheet.merge_range('U4:V5', 'Certified as Both MBE and WBE', merge_format)
        worksheet.merge_range('W4:Y5', 'Total M/WBE', merge_format)
        worksheet.merge_range('Z4:AA5', 'Total', merge_format)

        worksheet.write('C6', '#', merge_format)
        worksheet.write('D6', '$', merge_format)
        worksheet.write('E6', '#', merge_format)
        worksheet.write('F6', '$', merge_format)
        worksheet.write('G6', '#', merge_format)
        worksheet.write('H6', '$', merge_format)
        worksheet.write('I6', '#', merge_format)
        worksheet.write('J6', '$', merge_format)
        worksheet.write('K6', '#', merge_format)
        worksheet.write('L6', '$', merge_format)
        worksheet.write('M6', '#', merge_format)
        worksheet.write('N6', '$', merge_format)
        worksheet.write('O6', '#', merge_format)
        worksheet.write('P6', '$', merge_format)
        worksheet.write('Q6', '#', merge_format)
        worksheet.write('R6', '$', merge_format)
        worksheet.write('S6', '#', merge_format)
        worksheet.write('T6', '$', merge_format)
        worksheet.write('U6', '#', merge_format)
        worksheet.write('V6', '$', merge_format)
        worksheet.write('W6', '#', merge_format)
        worksheet.write('X6', '$', merge_format)
        worksheet.write('Y6', '%', merge_format)
        worksheet.write('Z6', '#', merge_format)
        worksheet.write('AA6', '$', merge_format)

        worksheet.set_column('B:B', 24)

        for i in [tab2_final]:
            for h, j in [['D', 'MBE Black ($)'], ['F', 'MBE Asian ($)'], ['H', 'MBE Hispanic ($)'],
                         ['J', 'WBE Black ($)'], ['L', 'WBE Asian ($)'], ['N', 'WBE Hispanic ($)'],
                         ['P', 'WBE - Caucasian ($)'], ['R', 'Non-Certified ($)'], ['T', 'EBE ($)'],
                         ['V', 'Certified as Both MBE and WBE ($)'], ['X', 'Total M/WBE ($)'], ['AA', 'Total ($)']]:
                worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1),
                                     len(str(i[j].max())) + 5)
                worksheet.conditional_format(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0]),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': dollar_signs})
            for h, j in [['C', 'MBE Black (#)'], ['E', 'MBE Asian (#)'], ['G', 'MBE Hispanic (#)'],
                         ['I', 'WBE Black (#)'], ['K', 'WBE Asian (#)'], ['M', 'WBE Hispanic (#)'],
                         ['O', 'WBE - Caucasian (#)'], ['Q', 'Non-Certified (#)'], ['S', 'EBE (#)'],
                         ['U', 'Certified as Both MBE and WBE (#)'], ['W', 'Total M/WBE (#)'], ['Z', 'Total (#)']]:
                worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1),
                                     len(str(int(i[j].max()))) + 2.5)
            for k, l in [['Y', 'Total MWBE (%)']]:
                worksheet.set_column(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0] - 1),
                                     len(str(int(i[l].max()))) + 4.5)
                worksheet.conditional_format(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0]),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': percentage_signs})

        for i in range(len(tab2_final.index)):
            if tab2_final.index[i] in industry:
                worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': industry_body})
                worksheet.write('B' + str(i + 7), tab2_final.index.tolist()[i], industry_index)
            elif tab2_final.index[i] in ['Total']:
                worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': total_body})
                worksheet.write('B' + str(i + 7), tab2_final.index.tolist()[i], total_index)
            elif tab2_final.index[i] in primes.Agency.unique():
                worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': agency_body})
                worksheet.write('B' + str(i + 7), tab2_final.index.tolist()[i], agency_index)
            elif tab2_final.index[i] in [key for key in size_group_dict]:
                worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                             {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': size_group_body})
                worksheet.write('B' + str(i + 7), tab2_final.index.tolist()[i], size_group_index)

        worksheet.conditional_format('C' + str(startrow + 1) + ':AA' + str(startrow + tab2_final.shape[0]),
                                     {'type': 'no_errors', 'format': gridlines})

        primes_post_master.to_excel(writer, sheet_name='Prime Data', startrow=0, startcol=0, index=False, header=True)

        workbook = writer.book
        worksheet = writer.sheets['Prime Data']

        worksheet.set_column('A:A', 13)
        worksheet.set_column('B:B', 12)
        worksheet.set_column('C:C', 18)
        worksheet.set_column('D:D', 18)
        worksheet.set_column('E:E', 20)
        worksheet.set_column('F:F', 20)
        worksheet.set_column('G:G', 22)
        worksheet.set_column('H:H', 18)
        worksheet.set_column('I:I', 26)
        worksheet.set_column('J:J', 22)
        worksheet.set_column('K:K', 19)
        worksheet.set_column('L:L', 31)
        worksheet.set_column('M:M', 28)
        worksheet.set_column('N:N', 22)
        worksheet.set_column('O:O', 25)
        worksheet.set_column('P:P', 63)
        worksheet.set_column('Q:Q', 27)
        worksheet.set_column('R:R', 18)
        worksheet.set_column('S:S', 131)
        worksheet.set_column('T:T', 21)
        worksheet.set_column('U:U', 21)
        worksheet.set_column('V:V', 19)
        worksheet.set_column('W:W', 20)

        writer.save()

#Compliance Report Appendices C and D
if __name__ == "__main__":

    prime_goals['Industry'] = industry_map(prime_goals['Industry'])

    prime_goals = prime_goals.drop_duplicates(['ContractID', 'ContractValue', 'VendorName', 'VendorNumber'])

    try:
        primeg_ebe = prime_goals[prime_goals['VendorNumber'].isin(ebe['FMS Vendor Number'])]
        primeg_ebe = primeg_ebe.drop_duplicates()
    except:
        pass

    primes = prime_goals

    ##################################

    industry = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']

    a1_1 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
    ['Industry', 'SizeGroup']).ContractID.nunique()
    a1_1.name = 'MBE Black (#)'

    a1_2 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
    ['Industry', 'SizeGroup']).ContractValue.sum()
    a1_2.name = 'MBE Black ($)'

    a2_1 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
    ['Industry', 'SizeGroup']).ContractID.nunique()
    a2_1.name = 'MBE Asian American (#)'
    a2_2 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
    ['Industry', 'SizeGroup']).ContractValue.sum()
    a2_2.name = 'MBE Asian American ($)'

    a3_1 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
    ['Industry', 'SizeGroup']).ContractID.nunique()
    a3_1.name = 'MBE Hispanic American (#)'
    a3_2 = primes[primes['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
    ['Industry', 'SizeGroup']).ContractValue.sum()
    a3_2.name = 'MBE Hispanic American ($)'

    a4_1 = primes[primes['ReportCategory'] == 'WBE - Black'].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
    a4_1.name = 'WBE Black (#)'
    a4_2 = primes[primes['ReportCategory'] == 'WBE - Black'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
    a4_2.name = 'WBE Black ($)'

    a5_1 = primes[primes['ReportCategory'] == 'WBE - Asian'].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
    a5_1.name = 'WBE Asian (#)'
    a5_2 = primes[primes['ReportCategory'] == 'WBE - Asian'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
    a5_2.name = 'WBE Asian ($)'

    a6_1 = primes[primes['ReportCategory'] == 'WBE - Hispanic'].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
    a6_1.name = 'WBE Hispanic (#)'
    a6_2 = primes[primes['ReportCategory'] == 'WBE - Hispanic'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
    a6_2.name = 'WBE Hispanic ($)'

    a7_1 = primes[primes['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
    ['Industry', 'SizeGroup']).ContractID.nunique()
    a7_1.name = 'WBE - Caucasian (#)'
    a7_2 = primes[primes['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
    ['Industry', 'SizeGroup']).ContractValue.sum()
    a7_2.name = 'WBE - Caucasian ($)'

    a8_1 = primes[primes['ReportCategory'].isnull()].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
    a8_1.name = 'Non-Certified (#)'
    a8_2 = primes[primes['ReportCategory'].isnull()].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
    a8_2.name = 'Non-Certified ($)'

    a9_1 = primes[primes['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
    ['Industry', 'SizeGroup']).ContractID.nunique()
    a9_1.name = 'Certified as Both MBE and WBE (#)'
    a9_2 = primes[primes['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
    ['Industry', 'SizeGroup']).ContractValue.sum()
    a9_2.name = 'Certified as Both MBE and WBE ($)'

    a10_1 = primes[primes['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
    a10_1.name = 'Total M/WBE (#)'
    a10_2 = primes[primes['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
    a10_2.name = 'Total M/WBE ($)'

    a11_1 = primes.groupby(['Industry', 'SizeGroup']).ContractValue.sum()
    a11_1.name = 'Total ($)'
    a11_2 = primes.groupby(['Industry', 'SizeGroup']).ContractID.nunique()
    a11_2.name = 'Total (#)'

    a10_3 = primes.groupby(['Industry', 'SizeGroup']).ContractID.nunique() #SPACEHOLDER DISREGARD
    a10_3.name = 'Total MWBE (%)'

    try:
        col1 = primeg_ebe.groupby(['Industry', 'SizeGroup']).ContractValue.sum()
        col1.name = 'EBE ($)'
    except:
        col1 = pd.DataFrame(col, index=a11_1.index, columns=['EBE ($)'])

    try:
        col2 = primeg_ebe.groupby(['Industry', 'SizeGroup']).ContractID.nunique()
        col2.name = 'EBE (#)'
    except:
        col2 = pd.DataFrame(col, index=a11_1.index, columns=['EBE ($)'])

    df = pd.concat([a1_1, a1_2, a2_1, a2_2, a3_1, a3_2, a4_1, a4_2, a5_1, a5_2, a6_1, a6_2, a7_1, a7_2, a8_1, a8_2, col1, col2, a9_1, a9_2, a10_1, a10_2, a10_3, a11_2, a11_1], axis=1)

    df = df.fillna(0)

    empty = []

    size_group_dict = {'Micro Purchase': 1, 'Small Purchase': 2, '>$100K, <=$1M': 3, '>$1M, <=$5M': 4,
                       '>$5M, <=$25M': 5,
                       '>$25M': 6}  # This data frame has all the info, but the order is WRONG and labels are WRONG

    shape = []

    for j in industry:
        if j in df.index.levels[0].unique().tolist():
            index_order = {'Total': 'A', 'Micro Purchase': 'B', 'Small Purchase': 'C', '>$100K, <=$1M': 'D',
                           '>$1M, <=$5M': 'E', '>$5M, <=$25M': 'F', '>$25M': 'G'}

            index_dict = {'A': str(j), 'B': 'Micro Purchase', 'C': 'Small Purchase', 'D': '>$100K, <=$1M',
                          'E': '>$1M, <=$5M', 'F': '>$5M, <=$25M', 'G': '>$25M'}

            df_industry = df.loc[(
                j, ['Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M', '>$5M, <=$25M', '>$25M']),
                          :]  # sliced it from the original
            industry_total_row = df_industry.sum()
            industry_total_row = pd.DataFrame(industry_total_row)
            industry_total_row = industry_total_row.T

            df_zeros = pd.DataFrame(np.zeros((6 - df_industry.shape[0], df_industry.shape[1])),
                                    columns=df_industry.columns, index=[(j, y) for y in
                                                                        ['Micro Purchase', 'Small Purchase',
                                                                         '>$100K, <=$1M',
                                                                         '>$1M, <=$5M', '>$5M, <=$25M', '>$25M']
                                                                        if
                                                                        y not in [x[1] for x in
                                                                                  df_industry.index if
                                                                                  isinstance(x, tuple)]])

            df_industry = pd.concat([df_industry, df_zeros])

            df_industry = pd.concat([industry_total_row, df_industry], axis=0)

            df_industry.index.values[0] = tuple([str(j), 'Total'])

            df_industry.index = [index_order.get(df_industry.index.values[x][1]) for x in
                                 range(len(df_industry.index.values))]

            df_industry = df_industry.sort_index(ascending=True)

            industry_shape = df_industry.shape

            shape.append(industry_shape)

            df_industry.index = [index_dict.get(df_industry.index.values[x]) for x in range(len(df_industry.index))]

            empty.append(df_industry)

        else:
            if j not in df.index.levels[0].unique().tolist():
                df_industry = pd.DataFrame(np.zeros((7, df.shape[1])), columns=df.columns,
                                           index=[j, 'Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M',
                                                  '>$5M, <=$25M', '>$25M'])
                empty.append(df_industry)

    final = pd.concat(empty)

    final.name = 'Industry and Size Group'

    final[u'Total M/WBE (#)'] = final[u'MBE Black (#)'] + final [
        u'MBE Asian American (#)'] + final[u'MBE Hispanic American (#)'] + final[u'WBE - Caucasian (#)']  # worked

    final[u'Total M/WBE ($)'] = final[u'MBE Black ($)'] + final[
        u'MBE Asian American ($)'] + final[u'MBE Hispanic American ($)'] + final[u'WBE - Caucasian ($)']  # worked

    final['Total MWBE (%)'] = final['Total M/WBE ($)'].astype(float) / final['Total ($)'].astype(float)

    final = final.fillna(0)

    index_order = {'Total': 'A', 'Micro Purchase': 'B', 'Small Purchase': 'C', '>$100K, <=$1M': 'D',
                   '>$1M, <=$5M': 'E', '>$5M, <=$25M': 'F', '>$25M': 'G'}

    df_list = []
    index_list = []

    for i in ['Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M', '>$5M, <=$25M', '>$25M']:
        if i in np.array(final.index):  # changed data structure of iterable.
            temp = pd.DataFrame(final.loc[i, :].astype(float).sum()).T
            df_list.append(temp)
            index_list.append(i)

    df = pd.concat(df_list)
    df.index = index_list

    tot = pd.DataFrame(df.sum()).T
    tot.index = ['Total']

    total_subsection = pd.concat([tot, df])

    total_subsection[u'Total M/WBE (#)'] = total_subsection[u'MBE Black (#)'] + total_subsection[u'MBE Asian American (#)'] + total_subsection[u'MBE Hispanic American (#)'] + total_subsection[u'WBE - Caucasian (#)']  # worked

    total_subsection[u'Total M/WBE ($)'] = total_subsection[u'MBE Black ($)'] + total_subsection[u'MBE Asian American ($)'] + total_subsection[u'MBE Hispanic American ($)'] + total_subsection[u'WBE - Caucasian ($)']  # worked

    total_subsection['Total MWBE (%)'] = total_subsection['Total M/WBE ($)'] / total_subsection['Total ($)']  # worked

    overall1 = pd.concat([final, total_subsection])

    overall1 = overall1.fillna(0)

    ###############

    path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Compliance Appendices'

    writer = pd.ExcelWriter(path +'\\'+ r'Table C and D.MWBE Participation Goals FY%s %s %s.xlsx' % (str(FY)[2:4], FQ, str(t)), engine='xlsxwriter')

    startrow = 6
    startcol = 1

    overall1 = overall1[[u'MBE Black (#)', u'MBE Black ($)', u'MBE Asian American (#)', u'MBE Asian American ($)',
                         u'MBE Hispanic American (#)', u'MBE Hispanic American ($)', u'WBE Black (#)', u'WBE Black ($)',
                         u'WBE Asian (#)', u'WBE Asian ($)', u'WBE Hispanic (#)', u'WBE Hispanic ($)',
                         u'WBE - Caucasian (#)', u'WBE - Caucasian ($)', u'Non-Certified (#)', u'Non-Certified ($)',
                         u'EBE (#)', u'EBE ($)', u'Certified as Both MBE and WBE (#)',
                         u'Certified as Both MBE and WBE ($)', u'Total M/WBE (#)', u'Total M/WBE ($)',
                         u'Total MWBE (%)', u'Total (#)', u'Total ($)']]

    overall1.to_excel(writer, sheet_name='Table C - Pr. Goals by Industry', startrow=6, startcol=1, header=False,
                      index=True)

    workbook = writer.book
    worksheet = writer.sheets['Table C - Pr. Goals by Industry']

    dollar_signs = workbook.add_format({'num_format': '$###,###,###,##0'})
    percentage_signs = workbook.add_format({'num_format': '0%'})
    gridlines = workbook.add_format({'border': 1})

    merge_format = workbook.add_format({
        'bold': 1,
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'fg_color': '#daeef3'})

    merge_format.set_text_wrap()
    merge_format.set_font_size(11)

    merge_format1 = workbook.add_format({
        'bold': 1,
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'fg_color': '#daeef3', 'size': 11})

    merge_format2 = workbook.add_format({
        'bold': 1,
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'fg_color': '#16365C', 'size': 11})

    merge_format2.set_font_color('white')

    size_group_body = workbook.add_format({'align': 'right', 'size': 11})
    size_group_body.set_right(1)
    size_group_body.set_left(1)

    size_group_body.set_top(1)
    size_group_body.set_bottom(1)

    size_group_index = workbook.add_format({'bold': 1, 'align': 'left', 'size': 11})
    size_group_index.set_right(1)
    size_group_index.set_left(1)
    size_group_index.set_top(1)
    size_group_index.set_bottom(1)

    industry_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

    industry_index.set_bg_color('#FFCC99')
    industry_index.set_right(1)
    industry_index.set_left(1)
    industry_index.set_top(1)
    industry_index.set_bottom(1)

    industry_body = workbook.add_format({'bold': 1,
                                         'align': 'center',
                                         'size': 11})

    industry_body.set_bg_color('#FFCC99')

    total_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

    total_index.set_bg_color('#e26b0a')
    total_index.set_right(1)
    total_index.set_left(1)
    total_index.set_top(1)
    total_index.set_bottom(1)

    total_body = workbook.add_format({'bold': 1,
                                      'align': 'center',
                                      'size': 11})

    total_body.set_bg_color('#e26b0a')

    format5 = workbook.add_format({'bold': 1,
                                   'align': 'center', 'size': 11})

    format5.set_bg_color('#FFCC99')
    format5.set_right(1)
    format5.set_left(1)
    format5.set_top(1)
    format5.set_bottom(1)

    format6 = workbook.add_format({'fg_color': '#FDE9D9', 'bold': 1, 'size': 11})
    format6.set_bottom(1)
    format6.set_top(1)

    format7 = workbook.add_format({'size': 11})

    format8 = workbook.add_format({'bold': 1,
                                   'size': 11})
    format8.set_bg_color('#E26B0A')

    format9 = workbook.add_format({'bold': 1,
                                   'align': 'center',
                                   'size': 11})
    format9.set_border(0)

    format10 = workbook.add_format({'bold': 1,
                                    'align': 'center',
                                    'size': 11})

    format10.set_bg_color('#FDE9D9')

    format11 = workbook.add_format({'bold': 1,
                                    'align': 'center', 'size': 11})

    format11.set_bg_color('#FFCC99')

    format12 = workbook.add_format({'bold': 1,
                                    'align': 'center',
                                    'size': 11})

    format12.set_bg_color('#FFCC99')
    format12.set_right(1)
    format12.set_left(1)
    format12.set_top(1)
    format12.set_bottom(1)

    format12_ = workbook.add_format({'bold': 1,
                                     'align': 'center',
                                     'size': 11})

    format12_.set_bg_color('#FFCC99')

    dollar_signs = workbook.add_format({'num_format': '$###,###,###,##0'})
    percentage_signs = workbook.add_format({'num_format': '0%'})
    gridlines = workbook.add_format({'border': 1})

    worksheet.merge_range('B3:AA3', 'Table C - FY%s Quarter %s' % (str(FY)[2:4], FQ[1]) + ' Prime Contracts Subject to Participation Goals - Disaggregated by Industry', merge_format2)
    worksheet.merge_range('C4:H4', 'MBE', merge_format)
    worksheet.merge_range('B4:B6', 'Industry and Size Group', merge_format)
    worksheet.merge_range('I4:P4', 'WBE', merge_format)
    worksheet.merge_range('C5:D5', 'Black', merge_format)
    worksheet.merge_range('E5:F5', 'Asian', merge_format)
    worksheet.merge_range('G5:H5', 'Hispanic', merge_format)
    worksheet.merge_range('I5:J5', 'Black', merge_format)
    worksheet.merge_range('K5:L5', 'Asian', merge_format)
    worksheet.merge_range('M5:N5', 'Hispanic', merge_format)
    worksheet.merge_range('O5:P5', 'Caucasian', merge_format)
    worksheet.merge_range('Q4:R5', 'Non-certified', merge_format)
    worksheet.merge_range('S4:T5', 'EBE', merge_format)
    worksheet.merge_range('U4:V5', 'Certified as Both MBE and WBE', merge_format)
    worksheet.merge_range('W4:Y5', 'Total M/WBE', merge_format)
    worksheet.merge_range('Z4:AA5', 'Total', merge_format)

    worksheet.write('C6', '#', merge_format)
    worksheet.write('D6', '$', merge_format)
    worksheet.write('E6', '#', merge_format)
    worksheet.write('F6', '$', merge_format)
    worksheet.write('G6', '#', merge_format)
    worksheet.write('H6', '$', merge_format)
    worksheet.write('I6', '#', merge_format)
    worksheet.write('J6', '$', merge_format)
    worksheet.write('K6', '#', merge_format)
    worksheet.write('L6', '$', merge_format)
    worksheet.write('M6', '#', merge_format)
    worksheet.write('N6', '$', merge_format)
    worksheet.write('O6', '#', merge_format)
    worksheet.write('P6', '$', merge_format)
    worksheet.write('Q6', '#', merge_format)
    worksheet.write('R6', '$', merge_format)
    worksheet.write('S6', '#', merge_format)
    worksheet.write('T6', '$', merge_format)
    worksheet.write('U6', '#', merge_format)
    worksheet.write('V6', '$', merge_format)
    worksheet.write('W6', '#', merge_format)
    worksheet.write('X6', '$', merge_format)
    worksheet.write('Y6', '%', merge_format)
    worksheet.write('Z6', '#', merge_format)
    worksheet.write('AA6', '$', merge_format)

    worksheet.set_column('B:B', 24)

    industry = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']

    for i in [overall1]:
        for h, j in [['D', 'MBE Black ($)'], ['F', 'MBE Asian American ($)'], ['H', 'MBE Hispanic American ($)'],
                     ['J', 'WBE Black ($)'], ['L', 'WBE Asian ($)'], ['N', 'WBE Hispanic ($)'],
                     ['P', 'WBE - Caucasian ($)'], ['R', 'Non-Certified ($)'], ['T', 'EBE ($)'],
                     ['V', 'Certified as Both MBE and WBE ($)'], ['X', 'Total M/WBE ($)'], ['AA', 'Total ($)']]:
            worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[j].max()))) + 4.5)
            worksheet.conditional_format(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': dollar_signs})
        for h, j in [['C', 'MBE Black (#)'], ['E', 'MBE Asian American (#)'], ['G', 'MBE Hispanic American (#)'],
                     ['I', 'WBE Black (#)'], ['K', 'WBE Asian (#)'], ['M', 'WBE Hispanic (#)'],
                     ['O', 'WBE - Caucasian (#)'], ['Q', 'Non-Certified (#)'], ['S', 'EBE (#)'],
                     ['U', 'Certified as Both MBE and WBE (#)'], ['W', 'Total M/WBE (#)'], ['Z', 'Total (#)']]:
            worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[j].max()))) + 2.0)
        for k, l in [['Y', 'Total MWBE (%)']]:
            worksheet.set_column(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[l].max()))) + 3.5)
            worksheet.conditional_format(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': percentage_signs})

    for i in range(len(overall1.index)):
        if overall1.index[i] in industry:
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': industry_body})
            worksheet.write('B' + str(i + 7), overall1.index.tolist()[i], industry_index)
        elif overall1.index[i] in ['Total']:
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': total_body})
            worksheet.write('B' + str(i + 7), overall1.index.tolist()[i], total_index)
        elif overall1.index[i] in primes.Agency.unique():
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': industry_body})
            worksheet.write('B' + str(i + 7), overall1.index.tolist()[i], industry_index)
        elif overall1.index[i] in [key for key in size_group_dict]:
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': size_group_body})
            worksheet.write('B' + str(i + 7), overall1.index.tolist()[i], size_group_index)

    worksheet.conditional_format('C' + str(startrow + 1) + ':AA' + str(startrow + overall1.shape[0]),
                                 {'type': 'no_errors', 'format': gridlines})

    # ####
    empty = []
    shape = []
    empty2 = []
    agency_total_list = []

    for i in sorted(primes['Agency'].unique()):

        total = []

        pw_ = primes[primes['Agency'] == i]

        primeg_ebe = primeg_ebe[primeg_ebe['Agency']==i]

        if pw_.shape[0] > 0:

            industry = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']

            a1_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a1_1.name = 'MBE Black (#)'
            a1_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
                ['Industry', 'SizeGroup']).ContractValue.sum()
            a1_2.name = 'MBE Black ($)'

            a2_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a2_1.name = 'MBE Asian American (#)'
            a2_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
                ['Industry', 'SizeGroup']).ContractValue.sum()
            a2_2.name = 'MBE Asian American ($)'

            a3_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a3_1.name = 'MBE Hispanic American (#)'
            a3_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).ContractValue.sum()
            a3_2.name = 'MBE Hispanic American ($)'

            a4_1 = pw_[pw_['ReportCategory'] == 'WBE - Black'].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a4_1.name = 'WBE Black (#)'
            a4_2 = pw_[pw_['ReportCategory'] == 'WBE - Black'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a4_2.name = 'WBE Black ($)'

            # WBE - Asian
            a5_1 = pw_[pw_['ReportCategory'] == 'WBE - Asian'].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a5_1.name = 'WBE Asian (#)'
            a5_2 = pw_[pw_['ReportCategory'] == 'WBE - Asian'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a5_2.name = 'WBE Asian ($)'

            # WBE Hispanic
            a6_1 = pw_[pw_['ReportCategory'] == 'WBE - Hispanic'].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a6_1.name = 'WBE Hispanic (#)'
            a6_2 = pw_[pw_['ReportCategory'] == 'WBE - Hispanic'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a6_2.name = 'WBE Hispanic ($)'

            # WBE Caucasian
            a7_1 = pw_[pw_['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a7_1.name = 'WBE - Caucasian (#)'
            a7_2 = pw_[pw_['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
                ['Industry', 'SizeGroup']).ContractValue.sum()
            a7_2.name = 'WBE - Caucasian ($)'

            # Non Certified
            a8_1 = pw_[pw_['ReportCategory'].isnull()].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a8_1.name = 'Non-Certified (#)'
            a8_2 = pw_[pw_['ReportCategory'].isnull()].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a8_2.name = 'Non-Certified ($)'

            # MBE and WBE
            a9_1 = pw_[pw_['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).ContractID.nunique()
            a9_1.name = 'Certified as Both MBE and WBE (#)'
            a9_2 = pw_[pw_['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).ContractValue.sum()
            a9_2.name = 'Certified as Both MBE and WBE ($)'

            # Total M/WBE
            a10_1 = pw_[pw_['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a10_1.name = 'Total M/WBE (#)'
            a10_2 = pw_[pw_['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a10_2.name = 'Total M/WBE ($)'

            # Total
            a11_1 = pw_.groupby(['Industry', 'SizeGroup']).ContractValue.sum()
            a11_1.name = 'Total ($)'
            a11_2 = pw_.groupby(['Industry', 'SizeGroup']).ContractID.nunique()
            a11_2.name = 'Total (#)'

            try:
                a10_3 = a10_2 / a11_1
                a10_3.name = 'Total MWBE (%)'
            except:
                col = np.zeros(shape=(len(a11_1), 1))

            try:
                col1 = primeg_ebe.groupby(['Industry', 'SizeGroup']).ContractID.nunique()
                col1.name = 'EBE (#)'
            except:
                col1 = primeg_ebe.groupby(['Industry', 'SizeGroup']).ContractID.nunique()
                col1.iloc[:] = 0
                col1.name = 'EBE (#)'

            try:
                col2 = primeg_ebe.groupby(['Industry', 'SizeGroup']).ContractValue.sum()
                col2.name = 'EBE ($)'
            except:
                col2 = primeg_ebe.ContractID.nunique()
                col2.iloc[:] = 0
                col2.name = 'EBE ($)'

            df = pd.concat(
                [a1_1, a1_2, a2_1, a2_2, a3_1, a3_2, a4_1, a4_2, a5_1, a5_2, a6_1, a6_2, a7_1, a7_2, a8_1, a8_2, col1,
                 col2, a9_1,
                 a9_2, a10_1, a10_2, a10_3, a11_2, a11_1], axis=1)

            df = df.fillna(0)

            size_group_dict = {'Micro Purchase': 1, 'Small Purchase': 2, '>$100K, <=$1M': 3, '>$1M, <=$5M': 4,
                               '>$5M, <=$25M': 5,
                               '>$25M': 6}  # This data frame has all the info, but the order is WRONG and labels are WRONG

            for j in industry:  # df is a multi index level
                if j in df.index.levels[0].unique().tolist():
                    index_order = {'Total': 'A', 'Micro Purchase': 'B', 'Small Purchase': 'C', '>$100K, <=$1M': 'D',
                                   '>$1M, <=$5M': 'E', '>$5M, <=$25M': 'F', '>$25M': 'G'}

                    index_dict = {'A': str(j), 'B': 'Micro Purchase', 'C': 'Small Purchase', 'D': '>$100K, <=$1M',
                                  'E': '>$1M, <=$5M', 'F': '>$5M, <=$25M', 'G': '>$25M'}

                    df_industry = df.loc[(j, df.index.levels[1].tolist()), :]  # sliced it from the original
                    industry_total_row = df_industry.sum()
                    industry_total_row = pd.DataFrame(industry_total_row)
                    industry_total_row = industry_total_row.T

                    total.append(industry_total_row)

                    df_zeros = pd.DataFrame(np.zeros((6 - df_industry.shape[0], df_industry.shape[1])),
                                            columns=df_industry.columns, index=[(j, y) for y in
                                                                                ['Micro Purchase', 'Small Purchase',
                                                                                 '>$100K, <=$1M',
                                                                                 '>$1M, <=$5M', '>$5M, <=$25M', '>$25M']
                                                                                if
                                                                                y not in [x[1] for x in
                                                                                          df_industry.index if
                                                                                          isinstance(x, tuple)]])

                    df_industry = pd.concat([df_industry, df_zeros])

                    df_industry = pd.concat([industry_total_row, df_industry], axis=0)

                    df_industry.index.values[0] = tuple([str(j), 'Total'])

                    df_industry.index = [index_order.get(df_industry.index.values[x][1]) for x in
                                         range(len(df_industry.index.values))]

                    df_industry = df_industry.sort_index(ascending=True)

                    industry_shape = df_industry.shape

                    shape.append(industry_shape)

                    df_industry.index = [index_dict.get(df_industry.index.values[x]) for x in
                                         range(len(df_industry.index))]

                    empty.append(df_industry)

                else:
                    if j not in df.index.levels[0].unique().tolist():
                        df_industry = pd.DataFrame(np.zeros((7, df.shape[1])), columns=df.columns,
                                                   index=[j, 'Micro Purchase', 'Small Purchase', '>$100K, <=$1M',
                                                          '>$1M, <=$5M', '>$5M, <=$25M', '>$25M'])
                        empty.append(df_industry)

                    industry_total_row = pd.DataFrame(np.zeros((1, df.shape[1])), columns=df.columns)
                    total.append(industry_total_row)

            final = pd.concat(empty)

            empty = []

            final.name = 'Industry and Size Group'

            final['Total MWBE (%)'] = final['Total M/WBE ($)'].astype(float) / final['Total ($)'].astype(float)

            final = final.fillna(0)  # The Order and Labels are Correct, but there is No Total SubSection

            df_test = pd.concat(total, axis=0)

            tot = pd.DataFrame(df_test.sum(axis=0)).T

            tot.index = [str(i)]

            tot['Total MWBE (%)'] = tot['Total M/WBE ($)'] / tot['Total ($)']

            agency_total_list.append(tot)

            overall1 = pd.concat([tot, final])

            empty2.append(overall1)

    tab2_final = pd.concat(empty2)

    agency_total = pd.concat(agency_total_list).sum()

    agency_total.name = 'Total'

    bottom_total_row = pd.DataFrame(agency_total).T

    bottom_total_row[u'Total M/WBE (#)'] = bottom_total_row[u'MBE Black (#)'] + bottom_total_row[
        u'MBE Asian American (#)'] + bottom_total_row[u'MBE Hispanic American (#)'] + bottom_total_row[
                                               u'WBE - Caucasian (#)']  # worked

    bottom_total_row[u'Total M/WBE ($)'] = bottom_total_row[u'MBE Black ($)'] + total_subsection[
        u'MBE Asian American ($)'] + total_subsection[u'MBE Hispanic American ($)'] + total_subsection[
                                               u'WBE - Caucasian ($)']  # worked

    bottom_total_row['Total MWBE (%)'] = bottom_total_row['Total M/WBE ($)'] / bottom_total_row['Total ($)']

    df_final = pd.concat([tab2_final, bottom_total_row])

    df_final[u'Total M/WBE (#)'] = df_final[u'MBE Black (#)'] + df_final[u'MBE Asian American (#)'] + df_final[u'MBE Hispanic American (#)'] + df_final[u'WBE - Caucasian (#)']

    df_final[u'Total M/WBE ($)'] = df_final[u'MBE Black ($)'].astype(float) + df_final[u'MBE Asian American ($)'].astype(float) + df_final[u'MBE Hispanic American ($)'].astype(float) + df_final[u'WBE - Caucasian ($)'].astype(float)

    ###############################################################################

    startrow = 6
    startcol = 1

    df_final.to_excel(writer, sheet_name='Table D - Prime Goals by Agency', startrow=startrow, startcol=startcol,
                      index=True, header=False)

    workbook = writer.book
    worksheet = writer.sheets['Table D - Prime Goals by Agency']

    dollar_signs = workbook.add_format({'num_format': '$###,###,###,##0'})
    percentage_signs = workbook.add_format({'num_format': '0%'})
    gridlines = workbook.add_format({'border': 1})

    merge_format = workbook.add_format({
        'bold': 1,
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'fg_color': '#daeef3'})

    merge_format.set_text_wrap()
    merge_format.set_font_size(11)

    merge_format1 = workbook.add_format({
        'bold': 1,
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'fg_color': '#daeef3', 'size': 10})

    merge_format2 = workbook.add_format({
        'bold': 1,
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'fg_color': '#16365C', 'size': 11})

    merge_format2.set_font_color('white')

    format4 = workbook.add_format({'bold': 1,
                                   'align': 'center',
                                   'size': 11})

    format4.set_bg_color('#FDE9D9')
    format4.set_right(1)
    format4.set_left(1)
    format4.set_top(1)
    format4.set_bottom(1)

    format4_ = workbook.add_format({'bold': 1,
                                    'align': 'center',
                                    'size': 11})

    format4_.set_bg_color('#FDE9D9')

    format5 = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

    format5.set_bg_color('#FFCC99')
    format5.set_right(1)
    format5.set_left(1)
    format5.set_top(1)
    format5.set_bottom(1)

    format6 = workbook.add_format({'fg_color': '#FDE9D9', 'bold': 1, 'size': 10})
    format6.set_bottom(1)
    format6.set_top(1)

    format7 = workbook.add_format({'size': 10})

    format8 = workbook.add_format({'bold': 1,
                                   'size': 11})
    format8.set_bg_color('#E26B0A')

    format9 = workbook.add_format({'bold': 1,
                                   'align': 'center',
                                   'size': 11})
    format9.set_border(0)

    format10 = workbook.add_format({'bold': 1,
                                    'align': 'center',
                                    'size': 11})

    format10.set_bg_color('#FDE9D9')

    format11 = workbook.add_format({'bold': 1,
                                    'align': 'center', 'size': 11})

    format11.set_bg_color('#FFCC99')

    format12 = workbook.add_format({'bold': 1,
                                    'align': 'center',
                                    'size': 11})

    format12.set_bg_color('#FFCC99')
    format12.set_right(1)
    format12.set_left(1)
    format12.set_top(1)
    format12.set_bottom(1)

    format12_ = workbook.add_format({'bold': 1,
                                     'align': 'center',
                                     'size': 11})

    format12_.set_bg_color('#FFCC99')

    size_group_body = workbook.add_format({'align': 'right', 'size': 11})
    size_group_body.set_right(1)
    size_group_body.set_left(1)
    size_group_body.set_top(1)
    size_group_body.set_bottom(1)

    size_group_index = workbook.add_format({'bold': 1, 'align': 'left', 'size': 11})
    size_group_index.set_right(1)
    size_group_index.set_left(1)
    size_group_index.set_top(1)
    size_group_index.set_bottom(1)

    agency_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

    agency_index.set_bg_color('#FFCC99')
    agency_index.set_right(1)
    agency_index.set_left(1)
    agency_index.set_top(1)
    agency_index.set_bottom(1)

    agency_body = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})
    agency_body.set_bg_color('#FFCC99')

    industry_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

    industry_index.set_bg_color('#FDE9D9')
    industry_index.set_right(1)
    industry_index.set_left(1)
    industry_index.set_top(1)
    industry_index.set_bottom(1)

    industry_body = workbook.add_format({'bold': 1,
                                         'align': 'center',
                                         'size': 11})

    industry_body.set_bg_color('#FDE9D9')

    total_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

    total_index.set_bg_color('#e26b0a')
    total_index.set_right(1)
    total_index.set_left(1)
    total_index.set_top(1)
    total_index.set_bottom(1)

    total_body = workbook.add_format({'bold': 1,
                                      'align': 'center',
                                      'size': 11})

    total_body.set_bg_color('#e26b0a')

    total_index = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

    total_index.set_bg_color('#e26b0a')
    total_index.set_right(1)
    total_index.set_left(1)
    total_index.set_top(1)
    total_index.set_bottom(1)

    total_body = workbook.add_format({'bold': 1,
                                      'align': 'center',
                                      'size': 11})

    total_body.set_bg_color('#e26b0a')

    worksheet.merge_range('B3:AA3','Table D - FY%s Quarter %s Prime Contracts Subject to Participation Goals - Disaggregated by Agency' % (str(FY)[2:4], FQ[1]), merge_format2)
    worksheet.merge_range('C4:H4', 'MBE', merge_format)
    worksheet.merge_range('B4:B6', 'Industry and Size Group', merge_format)
    worksheet.merge_range('I4:P4', 'WBE', merge_format)
    worksheet.merge_range('C5:D5', 'Black', merge_format)
    worksheet.merge_range('E5:F5', 'Asian', merge_format)
    worksheet.merge_range('G5:H5', 'Hispanic', merge_format)
    worksheet.merge_range('I5:J5', 'Black', merge_format)
    worksheet.merge_range('K5:L5', 'Asian', merge_format)
    worksheet.merge_range('M5:N5', 'Hispanic', merge_format)
    worksheet.merge_range('O5:P5', 'Caucasian', merge_format)
    worksheet.merge_range('Q4:R5', 'Non-certified', merge_format)
    worksheet.merge_range('S4:T5', 'EBE', merge_format)
    worksheet.merge_range('U4:V5', 'Certified as Both MBE and WBE', merge_format)
    worksheet.merge_range('W4:Y5', 'Total M/WBE', merge_format)
    worksheet.merge_range('Z4:AA5', 'Total', merge_format)

    worksheet.write('C6', '#', merge_format)
    worksheet.write('D6', '$', merge_format)
    worksheet.write('E6', '#', merge_format)
    worksheet.write('F6', '$', merge_format)
    worksheet.write('G6', '#', merge_format)
    worksheet.write('H6', '$', merge_format)
    worksheet.write('I6', '#', merge_format)
    worksheet.write('J6', '$', merge_format)
    worksheet.write('K6', '#', merge_format)
    worksheet.write('L6', '$', merge_format)
    worksheet.write('M6', '#', merge_format)
    worksheet.write('N6', '$', merge_format)
    worksheet.write('O6', '#', merge_format)
    worksheet.write('P6', '$', merge_format)
    worksheet.write('Q6', '#', merge_format)
    worksheet.write('R6', '$', merge_format)
    worksheet.write('S6', '#', merge_format)
    worksheet.write('T6', '$', merge_format)
    worksheet.write('U6', '#', merge_format)
    worksheet.write('V6', '$', merge_format)
    worksheet.write('W6', '#', merge_format)
    worksheet.write('X6', '$', merge_format)
    worksheet.write('Y6', '%', merge_format)
    worksheet.write('Z6', '#', merge_format)
    worksheet.write('AA6', '$', merge_format)

    for i in [df_final]:
        for h, j in [['D', 'MBE Black ($)'], ['F', 'MBE Asian American ($)'], ['H', 'MBE Hispanic American ($)'],
                     ['J', 'WBE Black ($)'], ['L', 'WBE Asian ($)'], ['N', 'WBE Hispanic ($)'],
                     ['P', 'WBE - Caucasian ($)'], ['R', 'Non-Certified ($)'], ['T', 'EBE ($)'],
                     ['V', 'Certified as Both MBE and WBE ($)'], ['X', 'Total M/WBE ($)'], ['AA', 'Total ($)']]:
            worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[j].max()))) + 4.5)
            worksheet.conditional_format(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': dollar_signs})
        for h, j in [['C', 'MBE Black (#)'], ['E', 'MBE Asian American (#)'], ['G', 'MBE Hispanic American (#)'],
                     ['I', 'WBE Black (#)'], ['K', 'WBE Asian (#)'], ['M', 'WBE Hispanic (#)'],
                     ['O', 'WBE - Caucasian (#)'], ['Q', 'Non-Certified (#)'], ['S', 'EBE (#)'],
                     ['U', 'Certified as Both MBE and WBE (#)'], ['W', 'Total M/WBE (#)'], ['Z', 'Total (#)']]:
            worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[j].max()))) + 2.0)
        for k, l in [['Y', 'Total MWBE (%)']]:
            worksheet.set_column(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[l].max()))) + 3.5)
            worksheet.conditional_format(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': percentage_signs})

    for i in range(len(df_final.index)):
        if df_final.index[i] in ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']:
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': industry_body})
            worksheet.write('B' + str(i + 7), df_final.index.tolist()[i], industry_index)
        elif df_final.index[i] in primes['Agency'].unique():
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': agency_body})
            worksheet.write('B' + str(i + 7), df_final.index.tolist()[i], agency_index)
        elif df_final.index[i] in [key for key in size_group_dict]:
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': size_group_body})
            worksheet.write('B' + str(i + 7), df_final.index.tolist()[i], size_group_index)
        elif df_final.index[i] in ['Total']:
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': total_body})
            worksheet.write('B' + str(i + 7), df_final.index.tolist()[i], total_index)

    worksheet.set_column('B:B', 24)

    worksheet.set_column('Y:Y', 5, percentage_signs)

    worksheet.conditional_format('C' + str(startrow + 1) + ':AA' + str(startrow + df_final.shape[0]),
                                 {'type': 'no_errors', 'format': gridlines})

    prime_goals.to_excel(writer, sheet_name='Prime Goals Data', startrow=0, startcol=0, index=False, header=True)

    workbook = writer.book
    worksheet = writer.sheets['Prime Goals Data']

    worksheet.set_column('A:A', 8.5)
    worksheet.set_column('B:B', 8.5)
    worksheet.set_column('C:C', 18)
    worksheet.set_column('D:D', 18)
    worksheet.set_column('E:E', 20)
    worksheet.set_column('F:F', 20)
    worksheet.set_column('G:G', 19)
    worksheet.set_column('H:H', 18)
    worksheet.set_column('I:I', 26)
    worksheet.set_column('J:J', 66)
    worksheet.set_column('K:K', 19)
    worksheet.set_column('L:L', 22)
    worksheet.set_column('M:M', 129)
    worksheet.set_column('N:N', 22)
    worksheet.set_column('O:O', 22)
    worksheet.set_column('P:P', 21)
    worksheet.set_column('Q:Q', 27)
    worksheet.set_column('R:R', 18)
    worksheet.set_column('S:S', 18)
    worksheet.set_column('T:T', 18)
    worksheet.set_column('U:U', 18)
    worksheet.set_column('V:V', 19)
    worksheet.set_column('W:W', 23)
    worksheet.set_column('X:X', 28)
    worksheet.set_column('Y:Y', 18)

    writer.save()

#Compliance Report Appendices E and F
if __name__ == "__main__":

    subs = subs_post_master

    try:
        subs_ebe = subs[subs[u'SubVendorNumber'].isin(ebe[u'FMS Vendor Number'])]
        print (subs_ebe.shape)
    except:
        print ('subs_ebe failed')

    industry = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']

    a1_1 = subs[subs['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
        ['Industry', 'SizeGroup']).SubContractID.nunique()
    a1_1.name = 'MBE Black (#)'

    a1_2 = subs[subs['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
        ['Industry', 'SizeGroup']).SubValue.sum().astype(float)
    a1_2.name = 'MBE Black ($)'

    a2_1 = subs[subs['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
        ['Industry', 'SizeGroup']).SubContractID.nunique()
    a2_1.name = 'MBE Asian (#)'
    a2_2 = subs[subs['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
        ['Industry', 'SizeGroup']).SubValue.sum().astype(float)
    a2_2.name = 'MBE Asian ($)'

    a3_1 = subs[subs['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
        ['Industry', 'SizeGroup']).SubContractID.nunique()
    a3_1.name = 'MBE Hispanic (#)'
    a3_2 = subs[subs['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
        ['Industry', 'SizeGroup']).SubValue.sum().astype(float)
    a3_2.name = 'MBE Hispanic ($)'

    a4_1 = subs[subs['ReportCategory'] == 'WBE - Black'].groupby(['Industry', 'SizeGroup']).SubContractID.nunique()
    a4_1.name = 'WBE Black (#)'
    a4_2 = subs[subs['ReportCategory'] == 'WBE - Black'].groupby(['Industry', 'SizeGroup']).SubValue.sum().astype(float)
    a4_2.name = 'WBE Black ($)'

    # WBE - Asian
    a5_1 = subs[subs['ReportCategory'] == 'WBE - Asian'].groupby(['Industry', 'SizeGroup']).SubContractID.nunique()
    a5_1.name = 'WBE Asian (#)'
    a5_2 = subs[subs['ReportCategory'] == 'WBE - Asian'].groupby(['Industry', 'SizeGroup']).SubValue.sum().astype(float)
    a5_2.name = 'WBE Asian ($)'

    # WBE Hispanic
    a6_1 = subs[subs['ReportCategory'] == 'WBE - Hispanic'].groupby(['Industry', 'SizeGroup']).SubContractID.nunique()
    a6_1.name = 'WBE Hispanic (#)'
    a6_2 = subs[subs['ReportCategory'] == 'WBE - Hispanic'].groupby(['Industry', 'SizeGroup']).SubValue.sum().astype(float)
    a6_2.name = 'WBE Hispanic ($)'

    # WBE Caucasian
    a7_1 = subs[subs['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
        ['Industry', 'SizeGroup']).SubContractID.nunique()
    a7_1.name = 'WBE - Caucasian (#)'
    a7_2 = subs[subs['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
        ['Industry', 'SizeGroup']).SubValue.sum().astype(float)
    a7_2.name = 'WBE - Caucasian ($)'

    # Non Certified
    a8_1 = subs[subs['ReportCategory'].isnull()].groupby(['Industry', 'SizeGroup']).SubContractID.nunique()
    a8_1.name = 'Non-Certified (#)'
    a8_2 = subs[subs['ReportCategory'].isnull()].groupby(['Industry', 'SizeGroup']).SubValue.sum().astype(float)
    a8_2.name = 'Non-Certified ($)'

    # MBE and WBE
    a9_1 = subs[subs['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
        ['Industry', 'SizeGroup']).SubContractID.nunique()
    a9_1.name = 'Certified as Both MBE and WBE (#)'
    a9_2 = subs[subs['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
        ['Industry', 'SizeGroup']).SubValue.sum().astype(float)
    a9_2.name = 'Certified as Both MBE and WBE ($)'

    # Total M/WBE
    a10_1 = subs[subs['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).SubContractID.nunique()
    a10_1.name = 'Total M/WBE (#)'

    a10_2 = subs[subs['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).SubValue.sum().astype(float)
    a10_2.name = 'Total M/WBE ($)'

    # Total
    a11_1 = subs.groupby(['Industry', 'SizeGroup']).SubValue.sum().astype(float)
    a11_1.name = 'Total ($)'
    a11_2 = subs.groupby(['Industry', 'SizeGroup']).SubContractID.nunique()
    a11_2.name = 'Total (#)'

    if a10_2.empty == False and a11_1.empty == False:
        a10_3 = a10_2 / a11_1
        a10_3.name = 'Total MWBE (%)'
    else:
        a10_3 = pd.Series([])
        a10_3.name = 'Total MWBE (%)'

    try:
        col1 = subs_ebe.groupby(['Industry', 'SizeGroup']).SubContractID.nunique()
        col1.name = 'EBE (#)'
    except:
        col1 = subs.groupby(['Industry', 'SizeGroup']).SubContractID.nunique()
        col1.iloc[:] = 0
        col1.name = 'EBE (#)'

    try:
        col2 = subs_ebe.groupby(['Industry', 'SizeGroup']).SubValue.sum().astype(float)
        col2.name = 'EBE ($)'
    except:
        col2 = subs.groupby(['Industry', 'SizeGroup']).SubValue.sum().astype(float)
        col2.iloc[:] = 0
        col2.name = 'EBE ($)'

    df = pd.concat(
        [a1_1, a1_2, a2_1, a2_2, a3_1, a3_2, a4_1, a4_2, a5_1, a5_2, a6_1, a6_2, a7_1, a7_2, a8_1, a8_2, col1, col2,
         a9_1, a9_2, a10_1, a10_2, a10_3, a11_2, a11_1], axis=1)

    df = df.fillna(0)

    empty = []

    size_group_dict = {'Micro Purchase': 1, 'Small Purchase': 2, '>$100K, <=$1M': 3, '>$1M, <=$5M': 4,
                       '>$5M, <=$25M': 5, '>$25M': 6}

    shape = []

    for j in industry:
        if j in subs.Industry.unique().tolist():
            index_order = {'Total': 'A', 'Micro Purchase': 'B', 'Small Purchase': 'C', '>$100K, <=$1M': 'D',
                           '>$1M, <=$5M': 'E', '>$5M, <=$25M': 'F', '>$25M': 'G'}

            index_dict = {'A': str(j), 'B': 'Micro Purchase', 'C': 'Small Purchase', 'D': '>$100K, <=$1M',
                          'E': '>$1M, <=$5M', 'F': '>$5M, <=$25M', 'G': '>$25M'}

            df_industry = df.loc[(j, ['Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M', '>$5M, <=$25M', '>$25M']), :]
            industry_total_row = df_industry.sum()
            industry_total_row = pd.DataFrame(industry_total_row)
            industry_total_row = industry_total_row.T

            df_zeros = pd.DataFrame(np.zeros((6 - df_industry.shape[0], df_industry.shape[1])),
                                    columns=df_industry.columns,
                                    index=[(j, y) for y in
                                           ['Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M',
                                            '>$5M, <=$25M', '>$25M'] if
                                           y not in [x[1] for x in df_industry.index if isinstance(x, tuple)]])

            df_industry = pd.concat([df_industry, df_zeros])

            df_industry = pd.concat([industry_total_row, df_industry], axis=0)

            df_industry.index.values[0] = tuple([str(j), 'Total'])

            df_industry.index = [index_order.get(df_industry.index.values[x][1]) for x in
                                 range(len(df_industry.index.values))]

            df_industry = df_industry.sort_index(ascending=True)

            industry_shape = df_industry.shape

            shape.append(industry_shape)

            df_industry.index = [index_dict.get(df_industry.index.values[x]) for x in range(len(df_industry.index))]

            empty.append(df_industry)
        else:
            df_industry = pd.DataFrame(np.zeros((7, df.shape[1])), columns=df.columns,
                                       index=[j, 'Micro Purchase', 'Small Purchase', '>$100K, <=$1M', '>$1M, <=$5M',
                                              '>$5M, <=$25M', '>$25M'])
            empty.append(df_industry)

    final = pd.concat(empty)
    final.name = 'Industry and Size Group'

    final['Total MWBE (%)'] = final['Total M/WBE ($)'] / final['Total ($)']  # The Order and Labels are Correct, but there is No Total SubSection

    index_order = {'Total': 'A', 'Micro Purchase': 'B', 'Small Purchase': 'C', '>$100K, <=$1M': 'D',
                   '>$1M, <=$5M': 'E', '>$5M, <=$25M': 'F', '>$25M': 'G'}

    list = []
    index_list = []

    try:
        micro = pd.DataFrame(final.loc['Micro Purchase', :].sum()).T
        list.append(micro)
        index_list.append('Micro Purchase')
    except (KeyError):
        pass
    try:
        small = pd.DataFrame(final.loc['Small Purchase', :].sum()).T
        list.append(small)
        index_list.append('Small Purchase')
    except (KeyError):
        pass
    try:
        level1 = pd.DataFrame(final.loc['>$100K, <=$1M', :].sum()).T
        list.append(level1)
        index_list.append('>$100K, <=$1M')
    except (KeyError):
        pass
    try:
        level2 = pd.DataFrame(final.loc['>$1M, <=$5M', :].sum()).T
        list.append(level2)
        index_list.append('>$1M, <=$5M')
    except (KeyError):
        pass
    try:
        level3 = pd.DataFrame(final.loc['>$5M, <=$25M', :].sum()).T
        list.append(level3)
        index_list.append('>$5M, <=$25M')
    except (KeyError):
        pass
    try:
        level4 = pd.DataFrame(final.loc['>$25M', :].sum()).T
        list.append(level4)
        index_list.append('>$25M')
    except (KeyError):
        pass
    except (ValueError):
        level4 = pd.DataFrame(final.loc['>$25M', :]).T
        list.append(level4)
        index_list.append('>$25M')

    df = pd.concat(list)

    df.index = index_list

    tot = pd.DataFrame(df.sum()).T
    tot.index = ['Total']

    total_portion = pd.concat([tot, df])
    total_portion['Total MWBE (%)'] = total_portion['Total M/WBE ($)'] / total_portion['Total ($)']

    subs_industry_summary = pd.concat([final, total_portion])

    empty = []

    df = pd.concat(list)

    df.index = index_list

    tot = pd.DataFrame(df.sum()).T
    tot.index = ['Total']

    total_portion = pd.concat([tot, df])

    total_portion['Total MWBE (%)'] = total_portion['Total M/WBE ($)'] / total_portion['Total ($)']

    table_e_final = pd.concat([final, total_portion])

    table_e_final = table_e_final[
        [u'MBE Black (#)', u'MBE Black ($)', u'MBE Asian (#)', u'MBE Asian ($)', u'MBE Hispanic (#)',
         u'MBE Hispanic ($)', u'WBE Black (#)', u'WBE Black ($)', u'WBE Asian (#)', u'WBE Asian ($)',
         u'WBE Hispanic (#)', u'WBE Hispanic ($)', u'WBE - Caucasian (#)', u'WBE - Caucasian ($)', u'Non-Certified (#)',
         u'Non-Certified ($)', u'EBE (#)', u'EBE ($)', u'Certified as Both MBE and WBE (#)',
         u'Certified as Both MBE and WBE ($)', u'Total M/WBE (#)', u'Total M/WBE ($)', u'Total MWBE (%)', u'Total (#)',
         u'Total ($)']]

    table_e_final[u'Certified as Both MBE and WBE (#)'] = table_e_final[u'WBE Black (#)'] + table_e_final[
        u'WBE Asian (#)'] + table_e_final[u'WBE Hispanic (#)']

    table_e_final[u'Total M/WBE (#)'] = table_e_final[u'MBE Black (#)'] + table_e_final[u'MBE Asian (#)'] + \
                                        table_e_final[u'MBE Hispanic (#)'] + table_e_final[u'WBE - Caucasian (#)']

    table_e_final = table_e_final.fillna(0)

    ##
    dict = {}
    empty = []
    shape = []
    empty2 = []

    agency_total_list = []

    for i in sorted(subs['Agency'].unique()):

        pw_ = subs[subs['Agency'] == i]

        try:
            pw_ebe = subs_ebe[subs_ebe['Agency'] == i]
        except:
            pass

        if pw_.shape[0] > 0:

            industry = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']

            a1_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
                ['Industry', 'SizeGroup']).SubContractID.count()
            a1_1.name = 'MBE Black (#)'
            a1_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Black', 'WBE - Black'])].groupby(
                ['Industry', 'SizeGroup']).SubValue.sum()
            a1_2.name = 'MBE Black ($)'

            a2_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
                ['Industry', 'SizeGroup']).SubContractID.count()
            a2_1.name = 'MBE Asian (#)'
            a2_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Asian', 'WBE - Asian'])].groupby(
                ['Industry', 'SizeGroup']).SubValue.sum()
            a2_2.name = 'MBE Asian ($)'

            a3_1 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).SubContractID.count()
            a3_1.name = 'MBE Hispanic (#)'
            a3_2 = pw_[pw_['ReportCategory'].isin(['Male-Owned MBE - Hispanic', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).SubValue.sum()
            a3_2.name = 'MBE Hispanic ($)'

            a4_1 = pw_[pw_['ReportCategory'] == 'WBE - Black'].groupby(
                ['Industry', 'SizeGroup']).SubContractID.count()
            a4_1.name = 'WBE Black (#)'
            a4_2 = pw_[pw_['ReportCategory'] == 'WBE - Black'].groupby(['Industry', 'SizeGroup']).SubValue.sum()
            a4_2.name = 'WBE Black ($)'

            # WBE - Asian
            a5_1 = pw_[pw_['ReportCategory'] == 'WBE - Asian'].groupby(
                ['Industry', 'SizeGroup']).SubContractID.count()
            a5_1.name = 'WBE Asian (#)'
            a5_2 = pw_[pw_['ReportCategory'] == 'WBE - Asian'].groupby(['Industry', 'SizeGroup']).SubValue.sum()
            a5_2.name = 'WBE Asian ($)'

            # WBE Hispanic
            a6_1 = pw_[pw_['ReportCategory'] == 'WBE - Hispanic'].groupby(
                ['Industry', 'SizeGroup']).SubContractID.count()
            a6_1.name = 'WBE Hispanic (#)'
            a6_2 = pw_[pw_['ReportCategory'] == 'WBE - Hispanic'].groupby(['Industry', 'SizeGroup']).SubValue.sum()
            a6_2.name = 'WBE Hispanic ($)'

            # WBE Caucasian
            a7_1 = pw_[pw_['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
                ['Industry', 'SizeGroup']).SubContractID.count()
            a7_1.name = 'WBE - Caucasian (#)'
            a7_2 = pw_[pw_['ReportCategory'] == 'WBE - Caucasian Woman'].groupby(
                ['Industry', 'SizeGroup']).SubValue.sum()
            a7_2.name = 'WBE - Caucasian ($)'

            # Non Certified
            a8_1 = pw_[pw_['ReportCategory'].isnull()].groupby(['Industry', 'SizeGroup']).SubContractID.count()
            a8_1.name = 'Non-Certified (#)'
            a8_2 = pw_[pw_['ReportCategory'].isnull()].groupby(['Industry', 'SizeGroup']).SubValue.sum()
            a8_2.name = 'Non-Certified ($)'

            # MBE and WBE
            a9_1 = pw_[pw_['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).SubContractID.count()
            a9_1.name = 'Certified as Both MBE and WBE (#)'
            a9_2 = pw_[pw_['ReportCategory'].isin(['WBE - Black', 'WBE - Asian', 'WBE - Hispanic'])].groupby(
                ['Industry', 'SizeGroup']).SubValue.sum()
            a9_2.name = 'Certified as Both MBE and WBE ($)'

            # Total M/WBE
            a10_1 = pw_[pw_['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).SubContractID.count()
            a10_1.name = 'Total M/WBE (#)'
            a10_2 = pw_[pw_['MWBE_Status'] == 'MWBE'].groupby(['Industry', 'SizeGroup']).SubValue.sum()
            a10_2.name = 'Total M/WBE ($)'

            # Total
            a11_1 = pw_.groupby(['Industry', 'SizeGroup']).SubValue.sum()
            a11_1.name = 'Total ($)'
            a11_2 = pw_.groupby(['Industry', 'SizeGroup']).SubContractID.count()
            a11_2.name = 'Total (#)'

            a10_3 = a10_2.astype(float)  # dummy column
            a10_3.name = 'Total MWBE (%)'

            try:
                col1 = pw_ebe.groupby(['Industry', 'SizeGroup']).SubContractID.count().astype(float)
                col1.name = 'EBE (#)'
            except:
                col1 = pw_.groupby(['Industry', 'SizeGroup']).SubContractID.count().astype(float)
                col1.iloc[:] = 0
                col1.name = 'EBE (#)'

            try:
                col2 = pw_ebe.groupby(['Industry', 'SizeGroup']).SubValue.sum().astype(float)
                col2.name = 'EBE ($)'
            except:
                col2 = pw_.groupby(['Industry', 'SizeGroup']).SubContractID.count().astype(float)
                col2.iloc[:] = 0
                col2.name = 'EBE ($)'

            df = pd.concat(
                [a1_1, a1_2, a2_1, a2_2, a3_1, a3_2, a4_1, a4_2, a5_1, a5_2, a6_1, a6_2, a7_1, a7_2, a8_1, a8_2, col1,
                 col2, a9_1,
                 a9_2, a10_1, a10_2, a10_3, a11_2, a11_1], axis=1)

            df = df.fillna(0)

            size_group_dict = {'Micro Purchase': 1, 'Small Purchase': 2, '>$100K, <=$1M': 3, '>$1M, <=$5M': 4,
                               '>$5M, <=$25M': 5,
                               '>$25M': 6}  # This data frame has all the info, but the order is WRONG and labels are WRONG

            industry = df.index.levels[0].tolist()

            industry = ['Professional Services', 'Construction Services', 'Standardized Services', 'Goods']

            for j in industry:  # df is a multi index level
                if j in df.index.levels[0].tolist():
                    index_order = {'Total': 'A', 'Micro Purchase': 'B', 'Small Purchase': 'C', '>$100K, <=$1M': 'D',
                                   '>$1M, <=$5M': 'E', '>$5M, <=$25M': 'F', '>$25M': 'G'}

                    index_dict = {'A': str(j), 'B': 'Micro Purchase', 'C': 'Small Purchase', 'D': '>$100K, <=$1M',
                                  'E': '>$1M, <=$5M', 'F': '>$5M, <=$25M', 'G': '>$25M'}

                    df_industry = df.loc[(j, df.index.levels[1].tolist()), :]  # sliced it from the original
                    industry_total_row = df_industry.sum()
                    industry_total_row = pd.DataFrame(industry_total_row)
                    industry_total_row = industry_total_row.T

                    np.zeros((6 - df_industry.shape[0], df_industry.shape[1]))

                    df_zeros = pd.DataFrame(np.zeros((6 - df_industry.shape[0], df_industry.shape[1])),
                                            columns=df_industry.columns, index=[(j, y) for y in
                                                                                ['Micro Purchase', 'Small Purchase',
                                                                                 '>$100K, <=$1M', '>$1M, <=$5M',
                                                                                 '>$5M, <=$25M', '>$25M'] if y not in [x[1] for x in df_industry.index if isinstance(x, tuple)]])
                    df_industry = pd.concat([df_industry, df_zeros])

                    df_industry = pd.concat([industry_total_row, df_industry], axis=0)

                    df_industry.index.values[0] = tuple([str(j), 'Total'])

                    df_industry.index = [index_order.get(df_industry.index.values[x][1]) for x in
                                         range(len(df_industry.index.values))]

                    df_industry = df_industry.sort_index(ascending=True)

                    industry_shape = df_industry.shape

                    shape.append(industry_shape)

                    df_industry.index = [index_dict.get(df_industry.index.values[x]) for x in
                                         range(len(df_industry.index))]

                    empty.append(df_industry)

                else:
                    if j not in df.index.levels[0].unique().tolist():
                        df_industry = pd.DataFrame(np.zeros((7, df.shape[1])), columns=df.columns,
                                                   index=[j, 'Micro Purchase', 'Small Purchase', '>$100K, <=$1M',
                                                          '>$1M, <=$5M', '>$5M, <=$25M', '>$25M'])
                    empty.append(df_industry)

            final = pd.concat(empty)

            empty = []

            final.name = 'Industry and Size Group'

            final['Total MWBE (%)'] = final['Total M/WBE ($)'].astype(float) / final['Total ($)'].astype(float)

            final = final.fillna(0)  # The Order and Labels are Correct, but there is No Total SubSection

            total = []

            for z in industry:
                total.append(final.loc[z])

            df_test = pd.concat(total, axis=1)

            tot = pd.DataFrame(df_test.sum(axis=1)).T

            tot.index = [str(i)]

            tot['Total MWBE (%)'] = tot['Total M/WBE ($)'] / tot['Total ($)']

            agency_total_list.append(tot)

            overall1 = pd.concat([tot, final])

            empty2.append(overall1)

    primes_agency_summary = pd.concat(empty2)

    agency_total = pd.concat(agency_total_list).sum()

    agency_total.name = 'Total'

    bottom_total_row = pd.DataFrame(agency_total).T

    bottom_total_row['Total MWBE (%)'] = bottom_total_row['Total M/WBE ($)'] / bottom_total_row['Total ($)']

    f_o = pd.concat([primes_agency_summary, bottom_total_row])

    f_o[u'Certified as Both MBE and WBE (#)'] = f_o[u'WBE Black (#)'] + f_o[u'WBE Asian (#)'] + f_o[u'WBE Hispanic (#)']

    f_o[u'Total M/WBE (#)'] = f_o[u'MBE Black (#)'] + f_o[u'MBE Asian (#)'] + f_o[u'MBE Hispanic (#)'] + f_o[
        u'WBE - Caucasian (#)']

    ###
    path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Compliance Appendices'

    writer = pd.ExcelWriter(path + '\\' + r'Table E and F.MWBE Subcontracting on Primes FY%s %s %s.xlsx' % (str(FY)[2:4], FQ, str(t)), engine='xlsxwriter')

    startrow = 6
    startcol = 1

    table_e_final.to_excel(writer, sheet_name='Table E - Subs by Industry', startrow=6, startcol=1, header=False,
                           index=True)

    workbook = writer.book
    worksheet = writer.sheets['Table E - Subs by Industry']

    worksheet.set_row(2, 18)

    merge_format = workbook.add_format(
        {'bold': 1, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'fg_color': '#daeef3'})
    merge_format.set_text_wrap()
    merge_format.set_font_size(11)

    merge_format1 = workbook.add_format(
        {'bold': 1, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'fg_color': '#daeef3', 'size': 10})

    merge_format2 = workbook.add_format(
        {'bold': 1, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'fg_color': '#16365C', 'size': 11})
    merge_format2.set_font_color('white')

    format3 = workbook.add_format({'bold': 1, 'align': 'left', 'size': 11})
    format3.set_right(1)
    format3.set_left(1)
    format3.set_top(1)
    format3.set_bottom(1)

    format3_ = workbook.add_format({'align': 'right', 'size': 11})
    format3_.set_right(1)
    format3_.set_left(1)
    format3_.set_top(1)
    format3_.set_bottom(1)

    format4 = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})
    format4.set_bg_color('#FDE9D9')
    format4.set_right(1)
    format4.set_left(1)
    format4.set_top(1)
    format4.set_bottom(1)

    format4_ = workbook.add_format({'bold': 1,
                                    'align': 'right',
                                    'size': 11})

    format4_.set_bg_color('#FDE9D9')

    format5 = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})
    format5.set_bg_color('#FFCC99')
    format5.set_right(1)
    format5.set_left(1)
    format5.set_top(1)
    format5.set_bottom(1)

    format6 = workbook.add_format({'fg_color': '#FDE9D9', 'bold': 1, 'size': 10})
    format6.set_bottom(1)
    format6.set_top(1)

    format7 = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})
    format7.set_bg_color('#FDE9D9')

    format8 = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})

    format8.set_bg_color('#FFCC99')

    format9 = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})
    format9.set_bg_color('#e26b0a')
    format9.set_right(1)
    format9.set_left(1)
    format9.set_top(1)
    format9.set_bottom(1)

    format12_ = workbook.add_format({'bold': 1,
                                     'align': 'right',
                                     'size': 11})

    format12_.set_bg_color('#FFCC99')

    dollar_signs = workbook.add_format({'num_format': '$###,###,###,##0'})
    percentage_signs = workbook.add_format({'num_format': '0%'})
    gridlines = workbook.add_format({'border': 1})

    worksheet.set_column('B:B', 24)

    worksheet.merge_range('B3:AA3', 'Table E - FY%s' % (str(FY)[2:4]) + ' Quarter 1-' + str(FQ)[
        1] + ' Approved Subcontracts on Open Primes Subject to Participation Goals - Disaggregated by Industry',
                          merge_format2)
    worksheet.merge_range('C4:H4', 'MBE', merge_format)
    worksheet.merge_range('B4:B6', 'Industry and Size Group', merge_format)
    worksheet.merge_range('I4:P4', 'WBE', merge_format)
    worksheet.merge_range('C5:D5', 'Black', merge_format)
    worksheet.merge_range('E5:F5', 'Asian', merge_format)
    worksheet.merge_range('G5:H5', 'Hispanic', merge_format)
    worksheet.merge_range('I5:J5', 'Black', merge_format)
    worksheet.merge_range('K5:L5', 'Asian', merge_format)
    worksheet.merge_range('M5:N5', 'Hispanic', merge_format)
    worksheet.merge_range('O5:P5', 'Caucasian', merge_format)
    worksheet.merge_range('Q4:R5', 'Non-certified', merge_format)
    worksheet.merge_range('S4:T5', 'EBE', merge_format)
    worksheet.merge_range('U4:V5', 'Certified as Both MBE and WBE', merge_format)
    worksheet.merge_range('W4:Y5', 'Total M/WBE', merge_format)
    worksheet.merge_range('Z4:AA5', 'Total', merge_format)

    worksheet.write('C6', '#', merge_format)
    worksheet.write('D6', '$', merge_format)
    worksheet.write('E6', '#', merge_format)
    worksheet.write('F6', '$', merge_format)
    worksheet.write('G6', '#', merge_format)
    worksheet.write('H6', '$', merge_format)
    worksheet.write('I6', '#', merge_format)
    worksheet.write('J6', '$', merge_format)
    worksheet.write('K6', '#', merge_format)
    worksheet.write('L6', '$', merge_format)
    worksheet.write('M6', '#', merge_format)
    worksheet.write('N6', '$', merge_format)
    worksheet.write('O6', '#', merge_format)
    worksheet.write('P6', '$', merge_format)
    worksheet.write('Q6', '#', merge_format)
    worksheet.write('R6', '$', merge_format)
    worksheet.write('S6', '#', merge_format)
    worksheet.write('T6', '$', merge_format)
    worksheet.write('U6', '#', merge_format)
    worksheet.write('V6', '$', merge_format)
    worksheet.write('W6', '#', merge_format)
    worksheet.write('X6', '$', merge_format)
    worksheet.write('Y6', '%', merge_format)
    worksheet.write('Z6', '#', merge_format)
    worksheet.write('AA6', '$', merge_format)

    worksheet.conditional_format('C' + str(startrow + 1) + ':AA' + str(startrow + table_e_final.shape[0]),
                                 {'type': 'no_errors', 'format': gridlines})

    for i in [table_e_final]:
        for h, j in [['D', 'MBE Black ($)'], ['F', 'MBE Asian ($)'], ['H', 'MBE Hispanic ($)'], ['J', 'WBE Black ($)'],
                     ['L', 'WBE Asian ($)'], ['N', 'WBE Hispanic ($)'], ['P', 'WBE - Caucasian ($)'],
                     ['R', 'Non-Certified ($)'], ['T', 'EBE ($)'], ['V', 'Certified as Both MBE and WBE ($)'],
                     ['X', 'Total M/WBE ($)'], ['AA', 'Total ($)']]:
            worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[j].max()))) + 4.5)
            worksheet.conditional_format(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': dollar_signs})
        for h, j in [['C', 'MBE Black (#)'], ['E', 'MBE Asian (#)'], ['G', 'MBE Hispanic (#)'], ['I', 'WBE Black (#)'],
                     ['K', 'WBE Asian (#)'], ['M', 'WBE Hispanic (#)'], ['O', 'WBE - Caucasian (#)'],
                     ['Q', 'Non-Certified (#)'], ['S', 'EBE (#)'], ['U', 'Certified as Both MBE and WBE (#)'],
                     ['W', 'Total M/WBE (#)'], ['Z', 'Total (#)']]:
            worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[j].max()))) + 2.0)
        for k, l in [['Y', u'Total MWBE (%)']]:
            worksheet.set_column(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[l].max()))) + 7.5)
            worksheet.conditional_format(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': percentage_signs})

    industry = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']

    for i in range(len(table_e_final.index)):
        if table_e_final.index[i] in industry:
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': format4})
            worksheet.write('B' + str(i + 7), table_e_final.index.tolist()[i], format4)
        elif table_e_final.index[i] in ['Total']:
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': format9})
            worksheet.write('B' + str(i + 7), table_e_final.index.tolist()[i], format9)
        elif table_e_final.index[i] in [key for key in size_group_dict]:
            worksheet.write('B' + str(i + 7), table_e_final.index.tolist()[i], format3)
        elif table_e_final.index[i] in subs.Agency.unique().tolist():
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': format12})
            worksheet.write('B' + str(i + 7), table_e_final.index.tolist()[i], format12)

    worksheet.set_column('Y:Y', 5, percentage_signs)

    ###

    f_o.to_excel(writer, sheet_name='Table F - Subcontract by Agency', startrow=6, startcol=1, index=True, header=False)

    workbook = writer.book
    worksheet = writer.sheets['Table F - Subcontract by Agency']

    worksheet.set_row(2, 18)

    merge_format = workbook.add_format(
        {'bold': 1, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'fg_color': '#daeef3'})

    merge_format.set_text_wrap()
    merge_format.set_font_size(11)

    merge_format2 = workbook.add_format({
        'bold': 1,
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'fg_color': '#16365C', 'size': 11})

    merge_format2.set_font_color('white')

    format4 = workbook.add_format({'bold': 1,
                                   'align': 'center',
                                   'size': 11})

    format3 = workbook.add_format({'bold': 1, 'align': 'left', 'size': 11})
    format3.set_right(1)
    format3.set_left(1)
    format3.set_top(1)
    format3.set_bottom(1)

    format3_ = workbook.add_format({'align': 'right', 'size': 11})
    format3_.set_right(1)
    format3_.set_left(1)
    format3_.set_top(1)
    format3_.set_bottom(1)

    format4.set_bg_color('#FDE9D9')
    format4.set_right(1)
    format4.set_left(1)
    format4.set_top(1)
    format4.set_bottom(1)

    format4_ = workbook.add_format({'bold': 1,
                                    'align': 'center',
                                    'size': 11})

    format4_.set_bg_color('#FDE9D9')

    format5 = workbook.add_format({'bold': 1, 'align': 'center', 'size': 11})
    format5.set_bg_color('#FFCC99')
    format5.set_right(1)
    format5.set_left(1)
    format5.set_top(1)
    format5.set_bottom(1)

    format6 = workbook.add_format({'fg_color': '#FDE9D9', 'bold': 1, 'size': 10})
    format6.set_bottom(1)
    format6.set_top(1)

    format7 = workbook.add_format({'size': 10})

    format8 = workbook.add_format({'bold': 1,
                                   'size': 11})
    format8.set_bg_color('#E26B0A')

    format10 = workbook.add_format({'bold': 1,
                                    'align': 'center',
                                    'size': 11})

    format10.set_bg_color('#FDE9D9')

    format11 = workbook.add_format({'bold': 1,
                                    'align': 'center', 'size': 11})

    dollar_signs = workbook.add_format({'num_format': '$###,###,###,##0'})
    percentage_signs = workbook.add_format({'num_format': '0%'})

    format11.set_bg_color('#FFCC99')

    format12 = workbook.add_format({'bold': 1,
                                    'align': 'center',
                                    'size': 11})

    format12.set_bg_color('#FFCC99')
    format12.set_right(1)
    format12.set_left(1)
    format12.set_top(1)
    format12.set_bottom(1)

    format12_ = workbook.add_format({'bold': 1,
                                     'align': 'center',
                                     'size': 11})

    format12_.set_bg_color('#FFCC99')

    worksheet.merge_range('B3:AA3', 'Table F - FY%s' % str(FY)[2:4] + ' Quarter 1-' + str(FQ)[
        1] + ' Approved Subcontracts on Open Primes Subject to Participation Goals - Disaggregated by Agency',
                          merge_format2)
    worksheet.merge_range('C4:H4', 'MBE', merge_format)
    worksheet.merge_range('B4:B6', 'Industry and Size Group', merge_format)
    worksheet.merge_range('I4:P4', 'WBE', merge_format)
    worksheet.merge_range('C5:D5', 'Black', merge_format)
    worksheet.merge_range('E5:F5', 'Asian', merge_format)
    worksheet.merge_range('G5:H5', 'Hispanic', merge_format)
    worksheet.merge_range('I5:J5', 'Black', merge_format)
    worksheet.merge_range('K5:L5', 'Asian', merge_format)
    worksheet.merge_range('M5:N5', 'Hispanic', merge_format)
    worksheet.merge_range('O5:P5', 'Caucasian', merge_format)
    worksheet.merge_range('Q4:R5', 'Non-certified', merge_format)
    worksheet.merge_range('S4:T5', 'EBE', merge_format)
    worksheet.merge_range('U4:V5', 'Certified as Both MBE and WBE', merge_format)
    worksheet.merge_range('W4:Y5', 'Total M/WBE', merge_format)
    worksheet.merge_range('Z4:AA5', 'Total', merge_format)

    worksheet.write('C6', '#', merge_format)
    worksheet.write('D6', '$', merge_format)
    worksheet.write('E6', '#', merge_format)
    worksheet.write('F6', '$', merge_format)
    worksheet.write('G6', '#', merge_format)
    worksheet.write('H6', '$', merge_format)
    worksheet.write('I6', '#', merge_format)
    worksheet.write('J6', '$', merge_format)
    worksheet.write('K6', '#', merge_format)
    worksheet.write('L6', '$', merge_format)
    worksheet.write('M6', '#', merge_format)
    worksheet.write('N6', '$', merge_format)
    worksheet.write('O6', '#', merge_format)
    worksheet.write('P6', '$', merge_format)
    worksheet.write('Q6', '#', merge_format)
    worksheet.write('R6', '$', merge_format)
    worksheet.write('S6', '#', merge_format)
    worksheet.write('T6', '$', merge_format)
    worksheet.write('U6', '#', merge_format)
    worksheet.write('V6', '$', merge_format)
    worksheet.write('W6', '#', merge_format)
    worksheet.write('X6', '$', merge_format)
    worksheet.write('Y6', '%', merge_format)
    worksheet.write('Z6', '#', merge_format)
    worksheet.write('AA6', '$', merge_format)

    worksheet.set_column('B:B', 24)

    worksheet.conditional_format('C' + str(startrow + 1) + ':AA' + str(startrow + f_o.shape[0]),
                                 {'type': 'no_errors', 'format': gridlines})

    for i in [f_o]:
        for h, j in [['D', 'MBE Black ($)'], ['F', 'MBE Asian ($)'], ['H', 'MBE Hispanic ($)'], ['J', 'WBE Black ($)'],
                     ['L', 'WBE Asian ($)'], ['N', 'WBE Hispanic ($)'], ['P', 'WBE - Caucasian ($)'],
                     ['R', 'Non-Certified ($)'], ['T', 'EBE ($)'], ['V', 'Certified as Both MBE and WBE ($)'],
                     ['X', 'Total M/WBE ($)'], ['AA', 'Total ($)']]:
            worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[j].max()))) + 4.5)
            worksheet.conditional_format(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': dollar_signs})
        for h, j in [['C', 'MBE Black (#)'], ['E', 'MBE Asian (#)'], ['G', 'MBE Hispanic (#)'], ['I', 'WBE Black (#)'],
                     ['K', 'WBE Asian (#)'], ['M', 'WBE Hispanic (#)'], ['O', 'WBE - Caucasian (#)'],
                     ['Q', 'Non-Certified (#)'], ['S', 'EBE (#)'], ['U', 'Certified as Both MBE and WBE (#)'],
                     ['W', 'Total M/WBE (#)'], ['Z', 'Total (#)']]:
            worksheet.set_column(h + str(startrow + 1) + ':' + h + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[j].max()))) + 2.0)
        for k, l in [['Y', 'Total MWBE (%)']]:
            worksheet.set_column(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0] - 1),
                                 len(str(int(i[l].max()))) + 7.5)
            worksheet.conditional_format(k + str(startrow + 1) + ':' + k + str(startrow + i.shape[0]),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': percentage_signs})

    for i in range(len(f_o.index)):
        if f_o.index[i] in industry:
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': format4_})
            worksheet.write('B' + str(i + 7), f_o.index.tolist()[i], format4)
        elif f_o.index[i] in subs.Agency.unique().tolist():
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': format12_})
            worksheet.write('B' + str(i + 7), f_o.index.tolist()[i], format12)
        elif f_o.index[i] in [key for key in size_group_dict]:
            worksheet.write('B' + str(i + 7), f_o.index.tolist()[i], format3)
        elif f_o.index[i] in ['Total']:
            worksheet.conditional_format(str('C' + str(i + 7) + ':' + 'AA' + str(i + 7)),
                                         {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': format9})
            worksheet.write('B' + str(i + 7), f_o.index.tolist()[i], format9)

    worksheet.set_column('Y:Y', 5, percentage_signs)

    subs_post_master.to_excel(writer, sheet_name='Subs Data', startrow=0, startcol=0, index=False, header=True)

    workbook = writer.book
    worksheet = writer.sheets['Subs Data']

    worksheet.set_column('A:A', 9)
    worksheet.set_column('B:B', 24)
    worksheet.set_column('C:C', 25)
    worksheet.set_column('D:D', 26)
    worksheet.set_column('E:E', 17)
    worksheet.set_column('F:F', 24)
    worksheet.set_column('G:G', 16)
    worksheet.set_column('H:H', 20)
    worksheet.set_column('I:I', 22)
    worksheet.set_column('J:J', 29)
    worksheet.set_column('K:K', 29)
    worksheet.set_column('L:L', 17)
    worksheet.set_column('M:M', 13)
    worksheet.set_column('N:N', 24)
    worksheet.set_column('O:O', 58)
    worksheet.set_column('P:P', 26)
    worksheet.set_column('Q:Q', 21)
    worksheet.set_column('R:R', 11)
    worksheet.set_column('S:S', 70)
    worksheet.set_column('T:T', 34)

    writer.save()

#Compliance Report Appendices G and H
if __name__ == "__main__":

    Access_Path = r'\\csc.nycnet\mocs\mocs_user_share\Contracts\Vendor Programs Unit\Waivers\Waivers Master List\MWBEWaivers_BackEnd.accdb;'
    conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Access_Path
    cnxn = pyodbc.connect(conn_str)
    crsr = cnxn.cursor()

    sql = """SELECT tblWaivers.Agency, tblWaivers.REG_DT, tblWaivers.[Decision Date], tblWaivers.LGL_NM_WaiverRequester, tblWaivers.TSP_Original, tblWaivers.TSP_Requested, tblWaivers.WaiverDetermination, tblWaivers.TSP_Actual, tblWaivers.ContractID, tblWaivers.PrimeIndustry, tblWaivers.WaiverID, tblWaivers.BidReleaseDate, tblWaivers.RegistrationStatus FROM tblWaivers;"""

    crsr.execute(sql)

    df = pd.DataFrame([tuple(x) for x in crsr.fetchall()])

    df.columns = ['Agency', 'Registration Date', 'Decision Date', 'LGL_NM_WaiverRequester', 'TSP_Original',
                  'TSP_Requested', 'WaiverDetermination', 'TSP_Actual', 'ContractID', 'PrimeIndustry', 'WaiverID',
                  'BidReleaseDate', 'RegistrationStatus']

    waivers = df[['Agency', 'Registration Date', 'Decision Date', 'LGL_NM_WaiverRequester', 'TSP_Original', 'TSP_Requested',
         'WaiverDetermination', 'TSP_Actual', 'ContractID', 'PrimeIndustry', 'WaiverID']]

    waivers.loc[:, 'Registration Date'] = pd.to_datetime(waivers['Registration Date'])

    waivers = waivers[~waivers['Registration Date'].isnull()]

    waivers = waivers[(waivers['Registration Date'] >= start_date) & (waivers['Registration Date'] <= end_date)]

    waivers['Registration Date'] = [str(x.date().strftime('%m/%d/%Y')) for x in waivers['Registration Date']]

    waivers['Decision Date'] = pd.to_datetime(waivers['Decision Date'])

    waivers['Decision Date'] = [str(x.date().strftime('%m/%d/%Y')) for x in pd.to_datetime(waivers['Decision Date'])]

    waivers = waivers[
        ['Agency', 'Registration Date', 'Decision Date', 'LGL_NM_WaiverRequester', 'TSP_Original', 'TSP_Requested',
         'WaiverDetermination', 'TSP_Actual', 'ContractID', 'PrimeIndustry', 'WaiverID']]
    waivers.columns = ['Agency', 'Registration Date', 'Decision Date', 'Vendor', 'M/WBE Participation Goal', 'Request',
                       'Waiver Determination', 'Actual', 'Contract ID', 'Industry', 'WaiverID']

    waivers['Agency'] = waivers['Agency'].str.replace('DCAS856', 'DCAS 856')
    waivers['Agency'] = waivers['Agency'].str.replace('DCAS857', 'DCAS 857')

    determined_waivers = df[
        ['Agency', 'Decision Date', 'BidReleaseDate', 'LGL_NM_WaiverRequester', 'TSP_Original', 'TSP_Requested',
         'WaiverDetermination', 'TSP_Actual', 'RegistrationStatus', 'WaiverID']]

    determined_waivers.loc[:, 'TSP_Actual'] = determined_waivers['TSP_Actual'].fillna(0)

    determined_waivers = determined_waivers[~determined_waivers['Decision Date'].isnull()]

    determined_waivers.loc[:, 'Decision Date'] = pd.to_datetime(determined_waivers['Decision Date'])

    determined_waivers = determined_waivers.sort_values(['Decision Date'], ascending=True)

    determined_waivers = determined_waivers[
        (determined_waivers['Decision Date'] >= start_date) & (determined_waivers['Decision Date'] <= end_date)]

    determined_waivers.loc[:, 'Decision Date'] = [str(x.date().strftime('%m/%d/%Y')) for x in
                                                  determined_waivers['Decision Date']]

    determined_waivers['BidReleaseDate'] = pd.to_datetime(determined_waivers['BidReleaseDate'])

    determined_waivers.loc[:, 'BidReleaseDate'] = [str(x.date().strftime('%m/%d/%Y')) for x in
                                                   determined_waivers['BidReleaseDate']]

    determined_waivers = determined_waivers[
        ['Agency', 'Decision Date', 'BidReleaseDate', 'LGL_NM_WaiverRequester', 'TSP_Original', 'TSP_Requested',
         'WaiverDetermination', 'TSP_Actual', 'RegistrationStatus', 'WaiverID']]
    determined_waivers.columns = ['Agency', 'Decision Date', 'BidReleaseDate', 'Vendor', 'M/WBE Participation Goal',
                                  'Request', 'Waiver Determination', 'Actual', 'Registration Status', 'WaiverID']

    determined_waivers['Agency'] = determined_waivers['Agency'].str.replace('DCAS856', 'DCAS 856')
    determined_waivers['Agency'] = determined_waivers['Agency'].str.replace('DCAS857', 'DCAS 857')

    letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L']

    path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Compliance Appendices'

    writer = pd.ExcelWriter(path +'\\'+ r'Appendices G and H MWBE Requests and Determinations FY%s %s %s.xlsx' % (
        str(FY)[2:4], FQ, str(t)), engine='xlsxwriter')
    workbook = writer.book

    #######
    ##TAB 1
    #######

    startcol = 1
    startrow = 3

    header_format1 = workbook.add_format(
        {'fg_color': '#16365C', 'align': 'center', 'valign': 'vcenter', 'font_color': 'white'})
    header_format2 = workbook.add_format(
        {'fg_color': '#DAEEF3', 'align': 'center', 'valign': 'vcenter', 'font_color': 'black', 'border': 1})
    dollar_signs = workbook.add_format({'num_format': '$###,###,###,##0'})
    gridlines = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
    center = workbook.add_format({'align': 'center', 'valign': 'vcenter'})
    percentage_signs = workbook.add_format({'num_format': '0.00%', 'align': 'center', 'valign': 'vcenter'})

    determined_waivers.to_excel(writer, sheet_name='Table G - Waivers Determined', startcol=startcol, startrow=startrow,
                                index=False, header=False)

    worksheet2 = writer.sheets['Table G - Waivers Determined']

    worksheet2.set_row(1, 18)

    worksheet2.merge_range('B2:K2', 'Table G - FY%s Quarter %s Request for Full or Partial M/WBE Waivers Determined' % (
    str(FY)[2:4], FQ[1]), header_format1)

    df = determined_waivers

    for i in range(len(df.columns.values)):
        if letters[i + 1] in ['F', 'G', 'I']:
            worksheet2.write(letters[i + 1] + str(3), df.columns.values[i], header_format2)
            worksheet2.set_column(letters[i + 1] + ':' + letters[i + 1],
                                  max(max([len(str(x)) for x in df.iloc[:, i]]) + 2, len(df.columns.values[i]) + 3),
                                  percentage_signs)
        elif letters[i + 1] != 'L':
            worksheet2.write(letters[i + 1] + str(3), df.columns.values[i], header_format2)
            worksheet2.set_column(letters[i + 1] + ':' + letters[i + 1],
                                  max(max([len(str(x)) for x in df.iloc[:, i]]) + 2, len(df.columns.values[i]) + 3),
                                  center)

    # could make letters more free-form in future
    worksheet2.conditional_format('B' + str(startrow + 1) + ':K' + str(startrow + df.shape[0]),
                                  {'type': 'no_errors', 'format': gridlines})

    #######
    ##TAB 2
    #######

    if waivers.shape[0] > 0:

        startcol = 1
        startrow = 3

        waivers.to_excel(writer, sheet_name='Table H - Registered Waivers', index=False, header=False,
                         startcol=startcol, startrow=startrow)
        worksheet = writer.sheets['Table H - Registered Waivers']

        worksheet.set_row(1, 18)

        df = waivers
        for i in range(len(df.columns.values)):
            if letters[i + 1] in ['F', 'G', 'I']:
                worksheet.write(letters[i + 1] + str(3), df.columns.values[i], header_format2)
                column_width = max(max([len(str(x)) for x in df.iloc[:, i]]) + 2, len(df.columns.values[i]) + 3)
                worksheet.set_column(letters[i + 1] + ':' + letters[i + 1], column_width, percentage_signs)
            else:
                worksheet.write(letters[i + 1] + str(3), df.columns.values[i], header_format2)
                column_width = max(max([len(str(x)) for x in df.iloc[:, i]]) + 2, len(df.columns.values[i]) + 3)
                worksheet.set_column(letters[i + 1] + ':' + letters[i + 1], column_width, center)

        worksheet.merge_range('B2:L2',
                              'Table H - FY%s Quarter %s Request for Full or Partial M/WBE Waivers Registered' % (
                                  str(FY)[2:4], FQ[1]), header_format1)
        worksheet.conditional_format('B' + str(startrow + 1) + ':L' + str(startrow + df.shape[0]),
                                     {'type': 'no_errors', 'format': gridlines})

        writer.save()

    elif waivers.shape[0] == 0:  # print empty sheet with header if no reg waivers
        startcol = 1
        startrow = 3

        waivers.to_excel(writer, sheet_name='Table H - Registered Waivers', index=False, header=False,
                         startcol=startcol,
                         startrow=startrow)
        worksheet = writer.sheets['Table H - Registered Waivers']

        for i in xrange(len(df.columns.values)):
            worksheet.write(letters[i + 1] + str(3), df.columns.values[i], header_format2)
            worksheet.set_column(letters[i + 1] + ':' + letters[i + 1], len(df.columns.values[i]) + 3, center)

        worksheet.merge_range('B2:L2',
                              'Table H - FY 20%s Quarter %s Request for Full or Partial M/WBE Waivers Registered' % (
                                  str(FY)[2:4], FQ[1]), header_format1)

        writer.save()

#Compliance Report Appendix I
if __name__ == '__main__':

    os.system(r'python "C:\FY19 Q3 Inventory\LL1 PROG\Python 3\APT_App_I1.py"')

    apt_table = pd.read_pickle(
        r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\FY18 MWBE Prog Files\Scripts\Optimized\LL1 Reporting - PRODUCTION\apt_rpt_tbl_frm_psr_n_all_except_na.pkl')

    apt_table.columns = ['req_cont_for_goods_services', 'req_contract_explanation', 'srvc_const_occ_multi_site',
                         'srvc_occur_multi_sites_expl', 'sin_indiv_pro_service_proj', 'single_indiv_proj_desc',
                         'cont_ref_uniq_unusual_goods', 'unique_unusual_goods_expl', 'epin']

    df = pq.read_table(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Open Contracts\open_contracts_%s.parquet' % (str(t))).to_pandas()

    df.columns = ['Agency', 'DOC_CD', 'DOC_DEPT_CD', 'DOC_ID', 'ContractID', 'EPIN', 'ContractValue', 'MWBE_LL',
                  'Method', 'VendorTIN', 'VendorNumber', 'VendorName', 'Purpose', 'StartDate', 'EndDate',
                  'RegistrationDate', 'Industry', 'ExcludeAll', 'ExcludeCategory', 'STATE_FED_FUNDED', 'MWBE72Fed',
                  'MWBE_GOALS', 'NoTSPReason', 'Base_EPIN', 'TSP', 'Goal_Black', 'Goal_Asian', 'Goal_Hispanic',
                  'Goal_Woman', 'Goal_Unspecified']

    df['RegistrationDate'] = pd.to_datetime(df['RegistrationDate'])

    df = df[(df['RegistrationDate'] >= start_date) & (df['RegistrationDate'] <= end_date)]

    df = df[df['Agency'] != 'DOE']
    df = df[df['Method'].isin(['Accelerated', 'Competitive Sealed Bid', 'Demonstration Project', 'Innovative','Negotiated Acquisition', 'Negotiated Acquisition Extension', 'Request for Proposal'])]
    df = df[df['ContractValue'] > 10000000]

    df = df[~df['Industry'].isin(["Human Services", "Non-Procurement"])]
    df = df[df['ContractValue'] > 10000000]

    df1 = df[df['ExcludeAll'] == False]
    df2 = df[(df['ExcludeAll'] == True) & (df['ExcludeAll'] == 16)]

    df = pd.concat([df1, df2])

    # AD HOC LINE
    # df = df[~df['ContractID'].isin(['MMA185720196200631','MMA185720196200676', 'MMA185720196200586', 'MMA185720196200662'])]

    df = df[~df['NoTSPReason'].isin([float(1), float(3), float(6), float(7), float(8), float(11), float(12)])]

    df = df.merge(apt_table, how='left', left_on='Base_EPIN', right_on='epin')

    df = df.drop_duplicates(['ContractID', 'Base_EPIN'])

    def basis_for_deter(row):
        if row['req_cont_for_goods_services'] in (1.,0.):
            return "Requirement Contract"
        elif row['srvc_const_occ_multi_site'] in (1.,0.):
            return "Multiple Site"
        elif row['sin_indiv_pro_service_proj'] in (1.,0):
            return "Single Indivisible Project"
        elif row['cont_ref_uniq_unusual_goods'] in (1.,0.):
            return "Unique/Unusual Good or Service"
        elif row['Industry'] == 'Human Services':
            return 'Human Services'
        elif row['Industry'] != 'Human Services':
            return 'Unknown'

    df['Basis for Determination'] = df.apply(basis_for_deter, axis=1)

    df = df[~df['Basis for Determination'].isin(['Human Services', 'Unknown'])]

    if 'Negotiated Acquisition' in df['Method'].unique():  # No Negotiated Acquisition Contracts have been observed for over a year.
        print ('There are Negotiated Acquisition Contracts - Join with apt_rpt_tbl_frm_psr_n_nego_acq Must Be Done') # Rare but second query must be performed at this juncture.
        os.system(r'python "C:\FY19 Q3 Inventory\LL1 PROG\Python 3\APT_App_I2.py"')
        neg_acq = pd.read_pickle(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\FY18 MWBE Prog Files\Scripts\Optimized\LL1 Reporting - PRODUCTION\apt_rpt_tbl_frm_psr_n_nego_acq.pkl')
        df = df[df['Method'] == 'Negotiated Acquisition'].merge(neg_acq, how='left', left_on='Base_EPIN',  right_on='epin')
        df = df.drop_duplicates(['Base_EPIN', 'ContractID'])
    else:
        print ('No Negotiated Acquisition Contracts in this Large Scale Review')

    df['ContractValue'] = df['ContractValue'].astype(float)

    df.sort_values(['ContractValue'], ascending=False, inplace=True)

    tup = str(tuple(df['ContractID'].unique()[0:1000]))
    tup = tup.replace('\'', '\\\'')
    tup = tup.replace('u', '')

    fname = 'large_scale_fms.py'
    line1 = """import cx_Oracle\nimport pandas as pd\nuid = \'jlin\'\npwd = \'Purple22\'\nservice = \'cwprd1.fisalan.nycnet\'\ndb = cx_Oracle.connect(uid + \"/\" + pwd + \"@\" + service)\n\ncursor = db.cursor()\nsql_list = ['Select DOC_CD, DOC_DEPT_CD,DOC_ID, CTCLS_CD FROM FMS01.PO_DOC_AWDDET WHERE DOC_CD || DOC_DEPT_CD || DOC_ID IN %s']\ncursor.execute(sql_list[0])\ndf1 = pd.DataFrame([[x for x in y] for y in cursor], columns=['DOC_CD', 'DOC_DEPT_CD', 'DOC_ID', 'CTCLS_CD'])\nsql_list = ['Select DOC_CD, DOC_DEPT_CD,DOC_ID, CTCLS_CD FROM FMS01.MA_DOC_AWDDET WHERE DOC_CD || DOC_DEPT_CD || DOC_ID IN %s']\ncursor.execute(sql_list[0])\ndf = pd.DataFrame([[x for x in y] for y in cursor], columns=['DOC_CD', 'DOC_DEPT_CD', 'DOC_ID', 'CTCLS_CD'])\ndf = pd.concat([df,df1])\ndf = df.drop_duplicates()\ndf = df.drop_duplicates()\ndf.to_csv('large_scale_capital.txt', index = False)""" % (tup, tup)
    with open(fname, 'w') as f:  # w stands for writing
        f.write('{}'.format(line1))  # .format() replaces the {}

    os.system(r'python "S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\LL1PROG\large_scale_fms.py"')

    cap = pd.read_csv('large_scale_capital.txt', header = 0)
    cap['ContractID'] = cap['DOC_CD'].astype(str) + cap['DOC_DEPT_CD'].astype(str) + cap['DOC_ID'].astype(str)

    df = df.merge(cap, how = 'left', on = ['ContractID'])

    df = df[~((df['CTCLS_CD'].isin(['C','RB','RC','X'])) & (df['ContractValue']>25000000))]

    large_scale = df[['Agency', 'VendorName', 'Purpose', 'Method', 'Industry', 'Basis for Determination', 'ContractValue']]

    large_scale.columns = ['Agency', 'Vendor Name', 'Purpose', 'Method', 'Industry', 'Basis for Determination', 'Contract Value']

    path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Compliance Appendices'

    writer = pd.ExcelWriter(path +'\\'+ r'Table I.MWBE Program.Large Scale Registrations FY%s %s %s.xlsx' % (str(FY)[2:4], FQ, str(t)), engine='xlsxwriter')

    startcol = 1
    startrow = 2

    large_scale.to_excel(writer, sheet_name='Large Scale Review', startcol=startcol, startrow=startrow, index=False)

    workbook = writer.book

    worksheet = writer.sheets['Large Scale Review']

    header_format1 = workbook.add_format({'fg_color': '#1F497D', 'align': 'center', 'valign': 'vcenter', 'font_color': 'white'})
    header_format2 = workbook.add_format(
        {'fg_color': '#C0C0C0', 'align': 'center', 'valign': 'vcenter', 'font_color': 'black', 'border': 1})
    header_format3 = workbook.add_format(
        {'fg_color': '#C0C0C0', 'align': 'left', 'valign': 'vcenter', 'font_color': 'black', 'border': 1})
    header_format3.set_text_wrap()
    dollar_signs = workbook.add_format({'num_format': '$###,###,###,##0'})
    gridlines = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})

    letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']

    worksheet.merge_range('B2:H2', 'Table I - FY%s Quarter %s Large Scale Contracts' % (str(FY)[2:4], str(FQ)[1]),
                          header_format1)

    for i in range(len(large_scale.columns.values)):
        if letters[i + 1] != 'H':
            worksheet.write(letters[i + 1] + str(3), large_scale.columns.values[i], header_format2)
            worksheet.set_column(letters[i + 1] + ':' + letters[i + 1], large_scale.iloc[:, i].str.len().max() + 6)
        else:
            worksheet.write(letters[i + 1] + str(3), large_scale.columns.values[i], header_format2)
            worksheet.set_column(letters[i + 1] + ':' + letters[i + 1], len(str(large_scale.iloc[:, i].max())) + 6,
                                 dollar_signs)

    worksheet.conditional_format('B' + str(startrow + 1) + ':H' + str(startrow + large_scale.shape[0] + 1),
                                 {'type': 'no_errors', 'format': gridlines})

    n = large_scale.shape[0] + startrow + 3

    worksheet.write('C' + str(n),
                    "Requirements Contracts: Requirements contracts allow the City to leverage economies of scale for goods or services that require vendors to respond as needed. Dividing requirements contracts would be impractical as it would impede the pace and increase the price at which the City receives essential goods or services. In many instances splitting requirements contracts would also create unduly burdensome accountability obstacles requiring multiple vendors to be held liable for very closely interconnected scopes of work.",
                    header_format3)
    worksheet.write('C' + str(int(n) + 1),
                    "Single Indivisible Project - A single indivisible project is defined as when work occurs at a single identifiable building or geographic location. Dividing such projects would have a detrimental administrative effect due to the interconnected nature of the work.",
                    header_format3)
    worksheet.write('C' + str(int(n) + 2),
                    r"Multiple Site - A multiple site project is defined as when work occurs at several locations, but is connected through infrastructural or technical requirements. In spite of a project's scope extending across multiple sites or geographic locations, the necessity for uniformity in implementation and/or the specialized nature of the work requires procurement through a single vendor to otherwise avoid unduly and burdensome accountability obstacles brought about through the use of multiple vendors.",
                    header_format3)

    worksheet.set_row(n - 1, 162)
    worksheet.set_row(n, 88)
    worksheet.set_row(n + 1, 161)

    writer.save()

end = time.monotonic()

print (end - start)

