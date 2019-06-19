import pandas as pd
import pyodbc
import datetime
import os
import datetime as dt

#sbs_filename is the unprocessed MWBE Vendor List re-saved with password removed.
#MWBE SBS List can't be inserted into MWBE Database. Work-around: Insert into Blank Database, connect as linked table.

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

#Localize Data From R_VEND_CUST
os.system('python S:\Contracts\\\\\"Research and IT\"\\\\\"08 - MWBE\"\\\\\"DAS Only\"\\\\\"09 - Python and R Scripts\"\\\Development\\\\\"SBS MWBE List to Access\"\\\\R_VEND_CUST.py')

#Post Process Raw SBS List and Filter LBEs only
sbs_filename = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\SBS MWBE List to Access\sbs_original.xlsx'

FMS_VEND_CUST = pd.read_table(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\SBS MWBE List to Access\R_VEND_CUST.txt', header = 0, sep = ',', dtype = {'VEND_CUST_CD':str, 'TIN':str})

FMS_VEND_CUST = FMS_VEND_CUST.drop_duplicates()

multi_vendors = FMS_VEND_CUST.groupby(['TIN']).VEND_CUST_CD.count().sort_values(ascending = False).reset_index() #Take this groupby object, reference these TINS in original R_VEND_CUST,

multi_vendors = multi_vendors[multi_vendors['VEND_CUST_CD'] > 1] #TINs with more than 1 VCD

sbs = pd.read_excel(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\SBS MWBE List to Access\sbs_original.xlsx', header= 0, dtype = {'FMS Vendor Number':str}) # , dtype = {'TAXID_SS':str} #Cumulative SBS Data. Append all files together (Q1 --> Q3).

sbs_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\SBS MWBE List to Access'

list = os.listdir(sbs_path)

xl = pd.ExcelFile(sbs_path + '/' + [x for x in list if 'sbs_original' in x][0])

sbs = pd.read_excel(sbs_path + '/' + [x for x in list if 'sbs_original' in x][0], sheetname = [x for x in xl.sheet_names if 'LBE' in x][0])

sbs = sbs[[u'Record ID', u'FMS Vendor Number', u'TAXID_SS', u'LEGAL_BUSINESS_NAME', u'Application Type', [x for x in sbs.columns if 'WBE' in x][0], u'ETHNICITY', u'Expiration Date', u'Certification Date']]

FMS_VEND_CUST = FMS_VEND_CUST[FMS_VEND_CUST['TIN'].isin(multi_vendors['TIN'])] #reference in R_VEND

df = sbs.merge(FMS_VEND_CUST, how = 'left', left_on = 'TAXID_SS', right_on = 'TIN')

df = df[['Record ID', 'FMS Vendor Number','TAXID_SS', 'VEND_CUST_CD', 'LEGAL_BUSINESS_NAME', 'Application Type', [x for x in sbs.columns if 'WBE' in x][0], 'ETHNICITY', 'Expiration Date','Certification Date']]

df['VEND_CUST_CD'] = df['VEND_CUST_CD'].fillna(0)

def merge_vendor_number(row):
    if row['VEND_CUST_CD']==0: #is None did not work
        return row['FMS Vendor Number']
    elif row['VEND_CUST_CD'] is not None:
        return row['VEND_CUST_CD']

df['FMSVendorNumber'] = df.apply(merge_vendor_number, axis=1)
df['FMS Vendor Number'] = df['FMSVendorNumber']
sbs_expanded = df[['Record ID', 'TAXID_SS', 'FMS Vendor Number', 'LEGAL_BUSINESS_NAME', 'Application Type', [x for x in sbs.columns if 'WBE' in x][0], 'ETHNICITY', 'Expiration Date','Certification Date']]
sbs_expanded['TAXID_SS'] = sbs_expanded['TAXID_SS'].str.replace('-', '') #qryCertMWBE_00_RemoveHyphenFromTIN
sbs_expanded['ETHNICITY'] = sbs_expanded['ETHNICITY'].str.replace(' ', '') #qryCertMWBE_00_TrimEthnicity

def ethnicity_function(row): #qryCertMWBE_02a_Update_EthGen
      if row['ETHNICITY'] == 'Non-Minority':
           return 'Caucasian Female'
      elif row['ETHNICITY'] == 'AsianIndian' or row['ETHNICITY'] == 'AsianPacific':
           return 'Asian American'
      elif row['ETHNICITY'] == 'Black':
           return 'Black American'
      elif row['ETHNICITY'] == 'Hispanic':
           return 'Hispanic American'

sbs_expanded['EthGen'] = sbs_expanded.apply(ethnicity_function, axis = 1)

def reportcategory_function(row):  # qryCertMWBE_02b_Update_RepCat
     if row[[x for x in sbs.columns if 'WBE' in x][0]] == 'MBE' and row['EthGen'] == 'Asian American':
          return 'Male-Owned MBE - Asian'
     elif row[[x for x in sbs.columns if 'WBE' in x][0]] == 'MBE' and row['EthGen'] == 'Black American':
          return 'Male-Owned MBE - Black'
     elif row[[x for x in sbs.columns if 'WBE' in x][0]] == 'MBE' and row['EthGen'] == 'Hispanic American':
          return 'Male-Owned MBE - Hispanic'
     elif row[[x for x in sbs.columns if 'WBE' in x][0]] == 'WBE':
          return 'WBE - Caucasian Woman'
     elif row[[x for x in sbs.columns if 'WBE' in x][0]] == 'MWBE' and row['EthGen'] == 'Asian American':
          return 'WBE - Asian'
     elif row[[x for x in sbs.columns if 'WBE' in x][0]] == 'MWBE' and row['EthGen'] == 'Black American':
          return 'WBE - Black'
     elif row[[x for x in sbs.columns if 'WBE' in x][0]] == 'MWBE' and row['EthGen'] == 'Hispanic American':
          return 'WBE - Hispanic'

sbs_expanded['ReportCategory'] = sbs_expanded.apply(reportcategory_function, axis=1)

def LBE_FL(row):
    if row['Application Type'] == 'LBE':
        return '1'
    else:
        return '0'

sbs_expanded['LBE_FL'] = sbs_expanded.apply(LBE_FL, axis = 1)

###############################################

df_lbe = df.fillna('0')