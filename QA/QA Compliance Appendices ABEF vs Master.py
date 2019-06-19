import pandas as pd
import os
import datetime as dt
from datetime import timedelta

today = dt.datetime.now()
t = dt.datetime.now().date()

def date_range(t):
    if t.month >= 7 and t.month <= 9:
        date_range_start = dt.date(t.year - 1, 7, 1) #Dates for whole cumulative year.
        date_range_end = dt.date(t.year, 6, 30)
        FY = t.year
        FQ = 'Q4'
    elif t.month >= 10 and t.month <= 12:
        date_range_start = dt.date(t.year, 7, 1) #First Quarter
        date_range_end = dt.date(t.year, 9, 30)
        FY = t.year + 1
        FQ = 'Q1'
    elif t.month >= 1 and t.month <= 3:
        date_range_start = dt.date(t.year - 1, 7, 1) #Second Quarter
        date_range_end = dt.date(t.year - 1, 12, 31)
        FY = t.year
        FQ = 'Q2'
    elif t.month >= 4 and t.month <= 6:
        date_range_start = dt.date(t.year - 1, 7, 1) #Third Quarter
        date_range_end = dt.date(t.year, 3, 31)
        FY = t.year
        FQ = 'Q3'
    return [date_range_start, date_range_end, FQ, FY]

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Compliance Appendices'

[date_range_start, date_range_end, FQ, FY] = date_range(t)

#Script QA consistency btw Master and Compliance Appendices A,B,E and F

doc_date_path = r'S:\Contracts\Research and IT\08 - MWBE\=MWBE Team=\LL1\Master\FY%s %s' % (str(FY)[2:4], str(FQ))

list = os.listdir(doc_date_path)

try:
    list.remove('Archived')
except:
    pass

list = [x[-15:-5] for x in list]

masters_date = pd.to_datetime(sorted(list)[-1:])[0].date()

doc_date_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Compliance Appendices'

list = os.listdir(doc_date_path)

try:
    list.remove('Archived')
except:
    pass

list = [x[-15:-5] for x in list]

compliance_date = t

# data_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\PRODUCTION\Datasets'

# start_date = str(date_range_start.month) + '/' + str(date_range_start.day) + '/' + str(date_range_start.year)
# end_date = str(date_range_end.month) + '/' + str(date_range_end.day) + '/' + str(date_range_end.year)

industry_list = ['Construction Services', 'Goods', 'Professional Services', 'Standardized Services']

agency = ['ACS', 'BIC', 'CCHR', 'CCRB', 'DCA', 'DCAS', 'DCLA', 'DCP', 'DDC', 'DEP', 'DFTA', 'DHS', 'DOB', 'DOC', 'DOF', 'DOHMH', 'DOI', 'DOITT', 'DOP', 'DORIS', 'DOT', 'DPR', 'DSNY', 'DYCD', 'FDNY', 'HPD', 'HRA', 'LAW', 'LPC', 'MOCJ', 'NYCEM', 'NYPD', 'OATH', 'SBS', 'TLC']

industry = ['Construction Services','Goods','Professional Services','Standardized Services']

size_group = ['Micro Purchase','Small Purchase','>$100K, <=$1M','>$1M, <=$5M','>$5M, <=$25M', '>$25M']

size_group_combined_util = ['Micro Purchase','Small Purchase','Over $100K']

compliance_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Compliance Appendices'

combined_util_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Masters'

list = os.listdir(compliance_path)
list1 = os.listdir(combined_util_path)

#######################################
#QA Compliance Report Appendix A and B
#######################################

a_industry = pd.read_excel(compliance_path + '/' + [x for x in list if 'Table A' in x if str(compliance_date) in x][0], sheetname='Table A - Primes Ind. Summary',
                            usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                                     23, 24, 25, 26], skiprows=6, header=None, index_col=0)

a_industry.columns = ['MBE Black #', 'MBE Black $', 'MBE Asian #', 'MBE Asian $', 'MBE Hispanic #',
                       'MBE Hispanic $', 'WBE Black #', 'WBE Black $', 'WBE Asian #', 'WBE Asian $',
                       'WBE Hispanic #', 'WBE Hispanic $', 'WBE Caucasian #', 'WBE Caucasian $', 'Non-Certified #',
                       'Non-Certified $', 'EBE #', 'EBE $', 'Both MBE and WBE #', 'Both MBE and WBE $',
                       'Total MWBE #', 'Total MWBE $', 'Total MWBE %', 'Total #', 'Total $']

if a_industry['Non-Certified #'].equals(a_industry['Total MWBE #']):
    print('Non-Certified equals Total MWBE')
else:
    pass

b_agency = pd.read_excel(compliance_path + '/' + [x for x in list if 'Table A' in x if str(compliance_date) in x][0], sheetname='Table B - Primes by Agency', usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26], skiprows=6, header=None, index_col=0)

b_agency.columns = ['MBE Black #', 'MBE Black $', 'MBE Asian #', 'MBE Asian $', 'MBE Hispanic #', 'MBE Hispanic $',
                     'WBE Black #', 'WBE Black $', 'WBE Asian #', 'WBE Asian $', 'WBE Hispanic #', 'WBE Hispanic $',
                     'WBE Caucasian #', 'WBE Caucasian $', 'Non-Certified #', 'Non-Certified $', 'EBE #', 'EBE $',
                     'Both MBE and WBE #', 'Both MBE and WBE $', 'Total MWBE #', 'Total MWBE $', 'Total MWBE %',
                     'Total #', 'Total $']

if b_agency['Non-Certified #'].equals(a_industry['Total MWBE #']):
    print('Non-Certified equals Total MWBE')
else:
    pass

if (b_agency.loc['Total',:] - a_industry.loc['Total',:]).sum() <0.001:
    pass
else:
    print ('Total Rows are Not Equal')

if (b_agency['WBE Black #'] + b_agency['WBE Asian #'] + b_agency['WBE Hispanic #'] - b_agency['Both MBE and WBE $']).sum() <0.001:
    pass
else:
    print ('Total Rows are Not Equal')

if (b_agency['WBE Black #'] + b_agency['WBE Asian #'] + b_agency['WBE Hispanic #'] - b_agency['Both MBE and WBE $']).sum() <0.001:
    pass
else:
    print ('Total Rows are Not Equal')

if (a_industry['WBE Black #'] + a_industry['WBE Asian #'] + a_industry['WBE Hispanic #'] - a_industry['Both MBE and WBE $']).sum() <0.001:
    pass
else:
    print ('Total Rows are Not Equal')



combined_util = pd.read_excel(combined_util_path + '/' + [x for x in list1 if 'licate_' in x if str(masters_date) in x][0], sheetname='Summary', usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22], skiprows=5, header=None, index_col=0)

###

combined_util_agency_rows = combined_util.loc[agency][[2, 3, 4, 5, 6]]
combined_util_agency_rows.columns = ['Total MWBE #', 'Total MWBE $', 'Total MWBE %', 'Total #', 'Total $']

combined_util_industry_rows = combined_util.loc[industry][[2, 3, 4, 5, 6]]
combined_util_industry_rows.columns = ['Total MWBE #', 'Total MWBE $', 'Total MWBE %', 'Total #', 'Total $']

combined_util_sizegroups_rows = combined_util.loc[size_group_combined_util][[2, 3, 4, 5, 6]]
combined_util_sizegroups_rows.columns = ['Total MWBE #', 'Total MWBE $', 'Total MWBE %', 'Total #', 'Total $']

#Compares Five Columns of Combined  Util to Same Columns in Appendices A and B. Returns Error in case of mismatch.

df = (a_industry.loc[industry][[u'Total MWBE #', u'Total MWBE $', u'Total MWBE %', u'Total #', u'Total $']].fillna(0) - combined_util_industry_rows) > 0.1
cols = df.columns
bt = df.apply(lambda x: abs(x) > 0)
if bt.sum().sum() == 0:
     pass
else:
    print (bt.apply(lambda x: list(cols[x.values]), axis=1))
    print (bt.sum().sum())

df = (a_industry.loc[size_group][[u'Total MWBE #', u'Total MWBE $', u'Total MWBE %', u'Total #', u'Total $']].fillna(0) - combined_util_sizegroups_rows) > 0.5
cols = df.columns
bt = df.apply(lambda x: abs(x) > 0)
if bt.sum().sum() == 0:
       pass
else:
       print (bt.apply(lambda x: list(cols[x.values]), axis=1))


#Table B(1) vs Master by Agency comparing columns: u'Total MWBE #', u'Total MWBE $', u'Total MWBE %', u'Total #', u'Total $'
df = (b_agency.loc[industry][[u'Total MWBE #', u'Total MWBE $', u'Total MWBE %', u'Total #', u'Total $']].fillna(0) - combined_util_agency_rows) > 0.1
cols = df.columns
bt = df.apply(lambda x: abs(x) > 0.01)
if bt.sum().sum() == 0:
     pass
else:
     print (bt.sum().sum())

#Table B(2) vs Master by Size Group
x1 = b_agency.loc[size_group][[u'Total MWBE #', u'Total MWBE $', u'Total MWBE %', u'Total #', u'Total $']].fillna(0)
x2 = x1.loc[['Micro Purchase', 'Small Purchase']]
over100 = pd.DataFrame(x1.loc[['>$100K, <=$1M','>$1M, <=$5M','>$5M, <=$25M','>$25M']].sum()).T
over100.index = ['Over $100K']

df = pd.concat([x2.groupby(x2.index).sum(), over100])
df['Total MWBE %'] = df['Total MWBE $'].astype(float) / df['Total $'].astype(float)

df = (df - combined_util_sizegroups_rows)
cols = df.columns
bt = df.apply(lambda x: abs(x) > 0.01) #True False Matrix. Removes estimation in next step. can set to zero.
if bt.sum().sum() == 0:
    pass
else:
    print (bt.apply(lambda x: list(cols[x.values]), axis=1))

#Table B(3) vs Master by Industry
x1 = b_agency.loc[industry][[u'Total MWBE #', u'Total MWBE $', u'Total MWBE %', u'Total #', u'Total $']].fillna(0)
x1 = x1.groupby(x1.index).sum()
x1['Total MWBE %'] = x1['Total MWBE $'].astype(float) / x1['Total $'].astype(float)
bt = (x1 - combined_util_industry_rows) > 0.1
cols = df.columns
if bt.sum().sum() == 0:
      pass
else:
      print ('Table B by Industry Error vs Master')
      print (bt.apply(lambda x: list(cols[x.values]), axis=1))

# ##########################################
# #QA Compliance Report Appendix E and F
# ##########################################

combined_util_agency_rows = combined_util.loc[agency][[7, 8, 9, 10, 11]]
combined_util_agency_rows.columns = ['MWBE Subcontracts #', 'MWBE Subcontracts $', 'MWBE Subcontracts%', 'Total Subcontracts #', 'Total Subcontracts $']

combined_util_industry_rows = combined_util.loc[industry][[7, 8, 9, 10, 11]]
combined_util_industry_rows.columns = ['MWBE Subcontracts #', 'MWBE Subcontracts $', 'MWBE Subcontracts%', 'Total Subcontracts #', 'Total Subcontracts $']

combined_util_sizegroups_rows = combined_util.loc[size_group_combined_util][[7, 8, 9, 10, 11]]
combined_util_sizegroups_rows.columns = ['MWBE Subcontracts #', 'MWBE Subcontracts $', 'MWBE Subcontracts%', 'Total Subcontracts #', 'Total Subcontracts $']

#Compare Subs Industry

e_industry = pd.read_excel(compliance_path + '/' + [x for x in list if 'Table E' in x if str(compliance_date) in x][0], sheetname='Table E - Subs by Industry',usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26], skiprows=6, header=None, index_col=0)

e_industry.columns = ['MBE Black #', 'MBE Black $', 'MBE Asian #', 'MBE Asian $', 'MBE Hispanic #',
                      'MBE Hispanic $', 'WBE Black #', 'WBE Black $', 'WBE Asian #', 'WBE Asian $',
                      'WBE Hispanic #', 'WBE Hispanic $', 'WBE Caucasian #', 'WBE Caucasian $', 'Non-Certified #',
                      'Non-Certified $', 'EBE #', 'EBE $', 'Both MBE and WBE #', 'Both MBE and WBE $',
                      'Total MWBE #', 'Total MWBE $', 'Total MWBE %', 'Total #', 'Total $']

f_agency = pd.read_excel(compliance_path + '/' + [x for x in list if 'Table E' in x if str(compliance_date) in x][0], sheetname='Table F - Subcontract by Agency', usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26], skiprows=6, header=None, index_col=0)

f_agency.columns = ['MBE Black #', 'MBE Black $', 'MBE Asian #', 'MBE Asian $', 'MBE Hispanic #', 'MBE Hispanic $',
                     'WBE Black #', 'WBE Black $', 'WBE Asian #', 'WBE Asian $', 'WBE Hispanic #', 'WBE Hispanic $',
                     'WBE Caucasian #', 'WBE Caucasian $', 'Non-Certified #', 'Non-Certified $', 'EBE #', 'EBE $',
                     'Both MBE and WBE #', 'Both MBE and WBE $', 'Total MWBE #', 'Total MWBE $', 'Total MWBE %',
                     'Total #', 'Total $']

#Table E(1/2) vs Master by Industry
df = (e_industry.loc[industry][['Total MWBE #', 'Total MWBE $', 'Total MWBE %', 'Total #','Total $']] - combined_util_industry_rows) > 0.1
cols = df.columns
bt = df.apply(lambda x: abs(x) > 0)
if bt.sum().sum() == 0:
    pass
else:
    print(bt.apply(lambda x: list(cols[x.values]), axis=1))

#Table E(2/2) vs Master by Size Group
df = (e_industry.loc[size_group][['Total MWBE #', 'Total MWBE $', 'Total MWBE %', 'Total #','Total $']] - combined_util_sizegroups_rows) > 0.1
cols = df.columns
bt = df.apply(lambda x: abs(x) > 0)
if bt.sum().sum() == 0:
    pass
else:
    print(bt.apply(lambda x: list(cols[x.values]), axis=1))

#Table E(1/3) vs Master by Agency
df = (f_agency.loc[agency][[u'Total MWBE #', u'Total MWBE $', u'Total MWBE %', u'Total #', u'Total $']].fillna(0) - combined_util_agency_rows) > 0.1
cols = df.columns
bt = df.apply(lambda x: abs(x) > 0)
if bt.sum().sum() == 0:
    pass
else:
    print (bt.apply(lambda x: list(cols[x.values]), axis=1))

#Table E(2/3) vs Master by Industry
df = (f_agency.loc[industry][[u'Total MWBE #', u'Total MWBE $', u'Total MWBE %', u'Total #', u'Total $']].fillna(0) - combined_util_industry_rows) > 0.1
cols = df.columns
bt = df.apply(lambda x: abs(x) > 0)
if bt.sum().sum() == 0:
    pass
else:
    print (bt.apply(lambda x: list(cols[x.values]), axis=1))

#Table E(3/3) vs Master by Size Group
x1 = f_agency.loc[size_group][[u'Total MWBE #', u'Total MWBE $', u'Total MWBE %', u'Total #', u'Total $']].fillna(0)
x2 = x1.loc[['Micro Purchase', 'Small Purchase']]
over100 = pd.DataFrame(x1.loc[['>$100K, <=$1M','>$1M, <=$5M','>$5M, <=$25M','>$25M']].sum()).T
over100.index = ['Over $100K']

df = pd.concat([x2.groupby(x2.index).sum(), over100])
df['Total MWBE %'] = df['Total MWBE $'].astype(float) / df['Total $'].astype(float)

df = (df - combined_util_sizegroups_rows)
cols = df.columns
bt = df.apply(lambda x: abs(x) > 0.01) #True False Matrix. Removes estimation in next step. can set to zero.
if bt.sum().sum() == 0:
     pass
else:
     print (bt.apply(lambda x: list(cols[x.values]), axis=1))

######################################
#QA Compliance Report Appendix C and D
######################################

c_industry = pd.read_excel(compliance_path + '/' + [x for x in list if 'Table C' in x if str(compliance_date) in x][0], sheetname=0,
                            usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                                     23, 24, 25, 26], skiprows=6, header=None, index_col=0)

c_industry.columns = ['MBE Black #', 'MBE Black $', 'MBE Asian #', 'MBE Asian $', 'MBE Hispanic #',
                       'MBE Hispanic $', 'WBE Black #', 'WBE Black $', 'WBE Asian #', 'WBE Asian $',
                       'WBE Hispanic #', 'WBE Hispanic $', 'WBE Caucasian #', 'WBE Caucasian $', 'Non-Certified #',
                       'Non-Certified $', 'EBE #', 'EBE $', 'Both MBE and WBE #', 'Both MBE and WBE $',
                       'Total MWBE #', 'Total MWBE $', 'Total MWBE %', 'Total #', 'Total $']

d_industry = pd.read_excel(compliance_path + '/' + [x for x in list if 'Table C' in x if str(compliance_date) in x][0], sheetname='Table D - Prime Goals by Agency',
                            usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                                     23, 24, 25, 26], skiprows=6, header=None, index_col=0)

d_industry.columns = ['MBE Black #', 'MBE Black $', 'MBE Asian #', 'MBE Asian $', 'MBE Hispanic #',
                       'MBE Hispanic $', 'WBE Black #', 'WBE Black $', 'WBE Asian #', 'WBE Asian $',
                       'WBE Hispanic #', 'WBE Hispanic $', 'WBE Caucasian #', 'WBE Caucasian $', 'Non-Certified #',
                       'Non-Certified $', 'EBE #', 'EBE $', 'Both MBE and WBE #', 'Both MBE and WBE $',
                       'Total MWBE #', 'Total MWBE $', 'Total MWBE %', 'Total #', 'Total $']

if c_industry['Non-Certified #'].equals(c_industry['Total MWBE #']):
    print('Non-Certified equals Total MWBE')
else:
    pass

if d_industry['Non-Certified #'].equals(d_industry['Total MWBE #']):
    print('Non-Certified equals Total MWBE')
else:
    pass

if (c_industry.loc['Total',:] - d_industry.loc['Total',:]).sum() <0.005:
    pass
else:
    print('Total Rows are Not Equal')
    print ((c_industry.loc['Total',:] - d_industry.loc['Total',:]).sum())

if (c_industry['WBE Black #'] + c_industry['WBE Asian #'] + c_industry['WBE Hispanic #'] - c_industry['Both MBE and WBE $']).sum() <0.001:
    pass
else:
    print ('Total Rows are Not Equal')

if (d_industry['WBE Black #'] + d_industry['WBE Asian #'] + d_industry['WBE Hispanic #'] - d_industry['Both MBE and WBE $']).sum() <0.001:
    pass
else:
    print ('Total Rows are Not Equal')

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\QA Logs'

df = pd.read_excel(path +'\\'+'Masters_vs_Compliance_QA.xlsx', header = 0)

df.loc[df.shape[0]] = [today, masters_date, compliance_date,'Yes', 'FY'+str(FY)[2:4], FQ]

df['Master_Doc_Date'] = [x.date() for x in pd.to_datetime(df['Master_Doc_Date'])]
df['Compliance_Doc_Date'] = [x.date() for x in pd.to_datetime(df['Compliance_Doc_Date'])]

writer = pd.ExcelWriter(path + '\\' + 'Masters_vs_Compliance_QA.xlsx', engine='xlsxwriter')

df.to_excel(writer, sheet_name = 'Masters_Comp_QA', index = False)

worksheet = writer.sheets['Masters_Comp_QA']

workbook = writer.book

center = workbook.add_format({'align': 'center', 'valign': 'vcenter'})

worksheet.set_column('A:A', 24, center)
worksheet.set_column('B:B', 21, center)
worksheet.set_column('C:C', 25, center)
worksheet.set_column('D:D', 15, center)
worksheet.set_column('E:E', 9, center)
worksheet.set_column('F:F', 9, center)

writer.save()

