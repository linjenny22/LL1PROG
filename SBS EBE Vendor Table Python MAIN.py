import pandas as pd
import pyodbc
import datetime
import os

#sbs_filename is the unprocessed MWBE Vendor List (all the data provided to MOCS by SBS in FY so far) re-saved with password removed.
#Technical Workaround: Hard to insert into large MWBE Database. Work-around: Insert into Blank Database, connect as linked table.

t = datetime.datetime.now()

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

#Read in R_VEND_CUST from FMS.
os.system('python S:\Contracts\\\\\"Research and IT\"\\\\\"08 - MWBE\"\\\\\"DAS Only\"\\\\\"09 - Python and R Scripts\"\\\Development\\\\\"SBS MWBE List to Access\"\\\\R_VEND_CUST.py')

#Localize R_VEND_CUST
FMS_VEND_CUST = pd.read_table(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\SBS MWBE List to Access\R_VEND_CUST.txt', header = 0, sep = ',', dtype = {'VEND_CUST_CD':str, 'TIN':str})

multi_vendors = FMS_VEND_CUST.groupby(['TIN']).VEND_CUST_CD.count().sort_values(ascending = False).reset_index() #Take this groupby object, reference these TINS in original R_VEND_CUST,

# Step 2
multi_vendors = multi_vendors[multi_vendors['VEND_CUST_CD'] > 1]

#Precautionary Drop Duplicates
FMS_VEND_CUST = FMS_VEND_CUST.drop_duplicates(['VEND_CUST_CD'])

#Aggregated file of SBS Vendor Data of to-date FY

sbs_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\SBS MWBE List to Access'

list = os.listdir(sbs_path)

xl = pd.ExcelFile(sbs_path + '/' + [x for x in list if 'sbs_original' in x][0])

ebe = pd.read_excel(sbs_path + '/' + [x for x in list if 'sbs_original' in x][0], sheetname = [x for x in xl.sheet_names if 'EBE' in x][0])

s_o_check = ebe[ebe['FMS Vendor Number'].isin(FMS_VEND_CUST['VEND_CUST_CD'])]

if s_o_check.shape[0] == ebe.shape[0]:
    pass
else:
    'There were Vendor Numbers in EBE List not in FMS'

if len(ebe[u'ETHNICITY'].unique()) > 2:
    def ethnicity_function(row):  # qryCertMWBE_02a_Update_EthGen
        if row['ETHNICITY'] == 'Non-Minority':
            return 'Caucasian Female'
        elif row['ETHNICITY'] == 'AsianIndian' or row['ETHNICITY'] == 'AsianPacific':
            return 'Asian American'
        elif row['ETHNICITY'] == 'Black':
            return 'Black American'
        elif row['ETHNICITY'] == 'Hispanic':
            return 'Hispanic American'

    ebe['EthGen'] = ebe.apply(ethnicity_function, axis=1)
else:
    ebe['EthGen'] = pd.Series(['0']*len(ebe['ETHNICITY'])) #spaceholder
    ebe[u'ETHNICITY'] = pd.Series(['0']*len(ebe['ETHNICITY']))

if len(ebe['M/WBE Type'].unique())>2:
    def reportcategory_function(row):  # qryCertMWBE_02b_Update_RepCat
        if row['M/WBE Type'] == 'MBE' and row['EthGen'] == 'Asian American':
            return 'Male-Owned MBE - Asian'
        elif row['M/WBE Type'] == 'MBE' and row['EthGen'] == 'Black American':
            return 'Male-Owned MBE - Black'
        elif row['M/WBE Type'] == 'MBE' and row['EthGen'] == 'Hispanic American':
            return 'Male-Owned MBE - Hispanic'
        elif row['M/WBE Type'] == 'WBE':
            return 'WBE - Caucasian Woman'
        elif row['M/WBE Type'] == 'M/WBE' and row['EthGen'] == 'Asian American':
            return 'WBE - Asian'
        elif row['M/WBE Type'] == 'M/WBE' and row['EthGen'] == 'Black American':
            return 'WBE - Black'
        elif row['M/WBE Type'] == 'M/WBE' and row['EthGen'] == 'Hispanic American':
            return 'WBE - Hispanic'

    ebe['ReportCategory'] = ebe.apply(reportcategory_function, axis=1)
else:
    ebe['ReportCategory'] = pd.Series(['0']*len(ebe['ETHNICITY'])) #spaceholder

def LBE_FL(row):
    if row['Application Type'] == 'LBE':
        return '1'
    else:
        return '0'

ebe['LBE_FL'] = ebe.apply(LBE_FL, axis=1)

ebe = ebe[[          u'Record ID',            u'TAXID_SS',   u'FMS Vendor Number',
       u'LEGAL_BUSINESS_NAME',    u'Application Type',          u'M/WBE Type',u'ETHNICITY',
           u'Expiration Date',  u'Certification Date',              u'EthGen',
            u'ReportCategory',              u'LBE_FL']]

ebe = ebe.drop_duplicates([u'FMS Vendor Number', u'TAXID_SS'])
