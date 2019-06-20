import pandas as pd
import numpy as np
import datetime
import os

#sbs_filename is the unprocessed MWBE Vendor List (all the data provided by SBS FY-to-date) re-saved no password.

data_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\PRODUCTION\Datasets'

oc_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\Open Contracts Localized'

t = datetime.datetime.now().date()

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

def ethnicity_function(x):
    if x == 'Non-Minority':
        return 'Caucasian Female'
    elif 'Asian' in x:
        return 'Asian American'
    elif x == 'Black':
        return 'Black American'
    elif x == 'Hispanic':
        return 'Hispanic American'

ethnicity_function = np.vectorize(ethnicity_function)

a = 'MWBEType'
b = 'EthGen'

def reportcategory_function(row):  # qryCertMWBE_02b_Update_RepCat
    if row[a] == 'MBE' and row[b] == 'Asian American':
        return 'Male-Owned MBE - Asian'
    elif row[a] == 'MBE' and row[b] == 'Black American':
        return 'Male-Owned MBE - Black'
    elif row[a] == 'MBE' and row[b] == 'Hispanic American':
        return 'Male-Owned MBE - Hispanic'
    elif row[a] == 'MBE' and row[b] == 'Asian-Indian':
        return 'Male-Owned MBE - Hispanic'
    elif row[a] == 'WBE':
        return 'WBE - Caucasian Woman'
    elif row[a] in ['M/WBE', 'MWBE'] and row[b] == 'Asian American':
        return 'WBE - Asian'
    elif row[a] in ['M/WBE', 'MWBE'] and row[b] == 'Black American':
        return 'WBE - Black'
    elif row[a] in ['M/WBE', 'MWBE'] and row[b] == 'Hispanic American':
        return 'WBE - Hispanic'

def LBE_FL(x):
    if x == 'LBE':
        return '1'
    else:
        return '0'

LBE_FL = np.vectorize(LBE_FL)

c = 'ETHNICITY_x'

def ethnicity_clean(row):  # qryCertMWBE_02b_Update_RepCat
    if 'ian' in str(row[c]).lower() and 'ific' in str(row[c]).lower():
        return 'Asian Pacific'
    elif 'ian' in str(row[c]).lower() and 'ian' in str(row[c]).lower():
        return 'Asian Indian'
    elif 'panic' in str(row[c]).lower():
        return 'Hispanic'
    elif 'rity' in str(row[c]).lower():
        return 'Non-Minority'
    elif 'ack' in str(row[c]).lower():
        return 'Black'

#Read in R_VEND_CUST from FMS.
os.system('python S:\Contracts\\\\\"Research and IT\"\\\\\"08 - MWBE\"\\\\\"DAS Only\"\\\\\"09 - Python and R Scripts\"\\\\LL1ProgFY19Q3\\\\LL1PROG\\\\R_VEND_CUST.py')

#Localize R_VEND_CUST

sbs_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\PRODUCTION\SBS MWBE'

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\LL1PROG'

FMS_VEND_CUST = pd.read_table(path +'\\'+ r'R_VEND_CUST.txt', header = 0, sep = ',', dtype = {'VEND_CUST_CD':str, 'TIN':str})

sbs_orig = pd.read_excel(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\SBS MWBE List to Access\sbs_original.xlsx', header= 0, dtype = {'FMS Vendor Number':str}) # , dtype = {'TAXID_SS':str} #Cumulative SBS Data. Append all files together (Q1 --> Q3).

sbs_orig = sbs_orig[sbs_orig['Application Type'].isin(['MWBE','M/WBE'])]

sbs_orig = sbs_orig.drop_duplicates(subset = ['FMS Vendor Number','TAXID_SS'], keep = 'last') #Because there is a DOC_Creation CODe and last means the most recent data point.

vcd_col = sbs_orig['FMS Vendor Number'].drop_duplicates()
tin_col = sbs_orig['TAXID_SS'].drop_duplicates()

FMS_VEND_CUST = FMS_VEND_CUST[['VEND_CUST_CD', 'TIN']]

vcd_col = vcd_col[vcd_col.isin(FMS_VEND_CUST['VEND_CUST_CD'])].dropna()#deduped VCDs in FMS

tin_col = tin_col[tin_col.isin(FMS_VEND_CUST['TIN'])].dropna() #deduped TINs in FMS

sbs = pd.concat([FMS_VEND_CUST[FMS_VEND_CUST['VEND_CUST_CD'].isin(vcd_col)],FMS_VEND_CUST[FMS_VEND_CUST['TIN'].isin(tin_col)]])

sbs = sbs.drop_duplicates(subset = ['VEND_CUST_CD','TIN'],keep = 'first') #pure set of fms vend_cust_cd originals.

#Append Data Columns back.
sbs = sbs.merge(sbs_orig, how = 'left', left_on = 'VEND_CUST_CD', right_on = 'FMS Vendor Number')
sbs = sbs.drop_duplicates(keep = 'first')

sbs = sbs.merge(sbs_orig, how = 'left', left_on = 'TIN', right_on = 'TAXID_SS')
sbs = sbs.drop_duplicates(subset = ['VEND_CUST_CD'], keep = 'first')

sbs['Record ID_x'] = sbs['Record ID_x'].fillna(sbs['Record ID_y'])
sbs['TAXID_SS_x'] = sbs['TAXID_SS_x'].fillna(sbs['TAXID_SS_y'])
sbs['FMS Vendor Number_x'] = sbs['FMS Vendor Number_x'].fillna(sbs['FMS Vendor Number_y'])
sbs['LEGAL_BUSINESS_NAME_x'] = sbs['LEGAL_BUSINESS_NAME_x'].fillna(sbs['LEGAL_BUSINESS_NAME_y'])
sbs['Application Type_x'] = sbs['Application Type_x'].fillna(sbs['Application Type_y'])
sbs['MWBE Type_x'] = sbs['MWBE Type_x'].fillna(sbs['MWBE Type_y'])
sbs['ETHNICITY_x'] = sbs['ETHNICITY_x'].fillna(sbs['ETHNICITY_y'])
sbs['Expiration Date_x'] = sbs['Expiration Date_x'].fillna(sbs['Expiration Date_y'])
sbs['Certification Date_x'] = sbs['Certification Date_x'].fillna(sbs['Certification Date_y'])

sbs.loc[:,'ETHNICITY'] = sbs.apply(ethnicity_clean, axis = 1)

sbs = sbs[['VEND_CUST_CD', 'TIN', 'Record ID_x', 'TAXID_SS_x', 'FMS Vendor Number_x', 'LEGAL_BUSINESS_NAME_x', 'Application Type_x', 'MWBE Type_x', 'ETHNICITY', 'Expiration Date_x', 'Certification Date_x']]
sbs.columns = ['FMS_VENDOR_ID', 'TIN', 'Record ID', 'TAXID_SS', 'FMS Vendor Number', 'LEGAL_BUSINESS_NAME', 'Application Type', 'MWBEType', 'ETHNICITY', 'Expiration Date', 'Certification Date']

#Data Cleaning Steps Must Do
sbs['MWBEType'] = sbs['MWBEType'].str.replace(' ','')

sbs['EthGen'] = ethnicity_function(sbs['ETHNICITY'])

sbs.loc[:,'ReportCategory'] = sbs.apply(reportcategory_function, axis=1)

sbs.loc[:,'LBE_FL'] = LBE_FL(sbs['Application Type'])

sbs['Expiration Date'] = pd.to_datetime(sbs['Expiration Date'])
sbs['Certification Date'] = pd.to_datetime(sbs['Certification Date'])

sbs['FMS_VENDOR_ID']= sbs['FMS_VENDOR_ID'].astype(str)

sbs = sbs.drop_duplicates(subset = ['FMS_VENDOR_ID', 'TIN'])

sbs = sbs[[u'FMS_VENDOR_ID', u'TIN', u'Application Type', u'MWBEType', u'ETHNICITY', u'EthGen', u'ReportCategory']]
