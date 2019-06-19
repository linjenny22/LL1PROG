import pandas as pd
import numpy as np
import datetime
import os

today = datetime.datetime.now().date()

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

filepath = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Directors Handout'

doc_date = today

print ('Masters Date: %s' % str(doc_date))

filepath_master = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1ProgFY19Q3\Datasets\Outputs\Masters'

master = r'FY%s %s LL1 and LL129 Replicate_%s.xlsx' % (str(FY)[2:4], FQ, str(doc_date))

agency_list_df = pd.read_excel(filepath_master + '\\' + master, skiprows = 4, header = 0)
agency_list = [x for x in agency_list_df.iloc[0:35,1]]

master = r'FY%s %s LL1 and LL129 Replicate OMWBE_%s.xlsx' % (str(FY)[2:4], FQ, str(doc_date))

# Checks City-Wide Panel ($ and #) Consistency
# Checks Agencies City Total Panels Consistency

for x in agency_list:

    sheet = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % (x)

    sheet1 = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % ('ACS') #check all agencies against one agency.

    df = pd.read_excel(sheet, sheetname = '1. Combined Primes and Subs', skiprows = 3, usecols = [6,7], header = 1)

    direc_citywide_util = df.ix[9,0]

    df1 = pd.read_excel(sheet1, sheetname = '1. Combined Primes and Subs', skiprows = 3, usecols = [6,7], header = 1)

    if all(df==df1) == False:
        print('City Wide Panel Consistency Across All Files:')
        print(x)
        print(df)
        print(df1)

    master_in = pd.read_excel(filepath_master + '\\' + master, skiprows = 5, usecols = [1,20], header = None)

    if round(float(master_in.ix[35,20]),4)*100 == float(direc_citywide_util[0:5]):
        pass

    master_in.columns = ['Agency', 'Percent']

# Checks Agency-Specific Panel (#)s Against Master Combined Util Rows

    sheet1 = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % (x)

    df1 = pd.read_excel(sheet1, sheetname = '1. Combined Primes and Subs', skiprows = 3, usecols = [2,3], header = 1)

    df = pd.read_excel(filepath_master + '\\' + master, skiprows = 5, usecols = [1, 2, 7, 12, 14, 18, 5, 10, 16, 21], index_col = 0, header = None)

    series1 = pd.DataFrame(df1.Count[0:9])
    s1 = [x for x in series1['Count'].astype(int)]

    series2 = pd.DataFrame(pd.Series(df.loc[x], index = [2, 7, 12, 14, 18, 5, 10, 16, 21]).tolist())
    series2 = series2[0].astype(int)
    series2 = [x for x in series2]

    if s1!=series2:
        print('Agency-Specific Panel Contract Counts (#)s vs Row in Master Summary Sheet:')
        print(x)
        print(s1)
        print(series2)

#Checks Agency-Specific Panel ($) Values Against Master List Summary Sheet

    sheet1 = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % (x)

    df1 = pd.read_excel(sheet1, sheetname = '1. Combined Primes and Subs', skiprows = 3, usecols = [2,3], header = 1)

    if round(df1['Value'][4]/df1['Value'][8],4) == float(df1.loc[9, 'Count'][0:5])/100:
        pass

    if round(float(df1.loc[9, 'Count'][0:5])) == (round(master_in[master_in['Agency'] == x]['Percent'].item(),2)*100):
        pass

    df = pd.read_excel(filepath_master + '\\' + master, skiprows = 5, usecols = [1,3, 8, 13, 15, 19, 6, 11, 17, 22], index_col = 0, header = None)

    series1 = pd.DataFrame(df1.Value[0:9])
    s1 = [x for x in np.round(series1['Value'])]

    series2 = pd.DataFrame(pd.Series(df.loc[x], index = [3, 8, 13, 15, 19, 6, 11, 17, 22]).tolist())
    series2 = [x for x in np.round(series2[0],0)]

    if s1 !=series2:
        print('Agency-Specific Panel ($) Values vs Row in Master Summary Sheet:')
        print(s1)
        print(series2)
        print(x)

#Checking Across tabs in Handouts: Checking Prime # Values in Panels Matches Summary Sheet

    sheet1 = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % (x)

    df1 = pd.read_excel(sheet1, sheetname = '1. Combined Primes and Subs', skiprows = 3, usecols = [2,3], header = 1)

    sheet1 = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % (x)

    df2 = pd.read_excel(sheet1, sheetname='2. Primes Summary Table', skiprows=5, usecols=[1, 21, 24], index_col = 0, header= None)

    if df2.loc['Total'][21].astype(int) == df1.Value[0].astype(int) and df2.loc['Total'][24].astype(int) == df1.Value[5].astype(int):
        pass
    else:
        print('Panel Primes Values vs Primes Summary Table:')
        print(x)
        print(df2.loc['Total'][21])
        print(df1.Value[0])
        print(df2.loc['Total'][24])
        print(df1.Value[5])

#Checking Across tabs in Handouts: Checking Prime $ Counts in Panels Matches Summary Sheet

    sheet1 = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % (x)

    df1 = pd.read_excel(sheet1, sheetname = '1. Combined Primes and Subs', skiprows = 3, usecols = [2,3], header = 1)

    sheet1 = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % (x)

    df2 = pd.read_excel(sheet1, sheetname='2. Primes Summary Table', skiprows=5, usecols=[1, 21, 24], index_col = 0, header= None)

    if df2.loc['Total'][21].astype(int) == df1.Value[0].astype(int) and df2.loc['Total'][24].astype(int) == df1.Value[5].astype(int):
        pass
    else:
        print('Panel Primes Values vs Primes Summary Table:')
        print(x)
        print(df2.loc['Total'][21])
        print(df1.Value[0])
        print(df2.loc['Total'][24])
        print(df1.Value[5])

#Checking within handouts: Checking Sub $ Values in Panels Matches Summary Sheet

    sheet1 = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % (x)

    df1 = pd.read_excel(sheet1, sheetname = '1. Combined Primes and Subs', skiprows = 3, usecols = [2,3], header = 1)

    sheet1 = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % (x)

    try:
        df2 = pd.read_excel(sheet1, sheetname='3. Subs Summary Table', skiprows=5, usecols=[1, 21, 24], index_col = 0, header= None)
        if df2.loc['Total'][21].astype(float) - df1.Value[1].astype(float) < 0.001 and df2.loc['Total'][24].astype(float) - df1.Value[6].astype(float) < 0.001:
            pass
        else:
            print('Panel Subs Values vs Subs Summary Table:')
            print(x)
            print (df2.loc['Total'][21].astype(float) - df1.Value[1].astype(float))
            print(df2.loc['Total'][21])
            print(df1.Value[1])
            print(df2.loc['Total'][24].astype(float) - df1.Value[6].astype(float))
            print(df2.loc['Total'][24])
            print(df1.Value[6])
    except:
        pass

    sheet1 = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % (x)

    df1 = pd.read_excel(sheet1, sheetname='1. Combined Primes and Subs', skiprows=3, usecols=[2, 3], header=1)

    sheet1 = filepath + '\\' + r'Directors Meeting_Agency Handout_%s.xlsx' % (x)

    try:
        df2 = pd.read_excel(sheet1, sheetname='3. Subs Summary Table', skiprows=5, usecols=[1, 20, 23], index_col=0,
                            header=None)
        if df2.loc['Total'][20].astype(float) - df1.Count[1].astype(float) < 0.001 and df2.loc['Total'][23].astype(float) - \
                df1.Count[7].astype(float) < 0.001:
            pass
        else:
            print('Panel Subs Values vs Subs Summary Table:')
            print(x)
            print (df2.loc['Total'][20].astype(float) - df1.Count[1].astype(float))
            print(df2.loc['Total'][20])
            print(df1.Count[1])
            print (df2.loc['Total'][23].astype(float) - df1.Count[7].astype(float))
            print(df2.loc['Total'][23])
            print(df1.Count[7])
    except:
        pass

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\QA Logs'

df = pd.read_excel(path +'\\'+'Masters_vs_Directors_Handouts_QA.xlsx', header = 0)

dh_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\01 - Reporting\01 - LL1 and LL129\03 - Directors Meeting\Agency Handout'

list = os.listdir(dh_path)

list = [x for x in list if 'FY %s %s' % (str(FY)[2:4], FQ) in x] # if FQ in x

list = [x for x in set(list) if str(doc_date)[:-2] in x]

list = sorted(list)

df.loc[df.shape[0]] = [today, doc_date, list[-1][-10:] , 'Yes', 'FY'+str(FY)[2:4], FQ]

df['Masters Version Date'] = [x.date() for x in pd.to_datetime(df['Masters Version Date'])]
df['DH_Version_Date'] = [x.date() for x in pd.to_datetime(df['DH_Version_Date'])]

writer = pd.ExcelWriter(path + '\\' + 'Masters_vs_Directors_Handouts_QA.xlsx', engine='xlsxwriter')

df.to_excel(writer, sheet_name = 'Masters_DH QA', index = False)

worksheet = writer.sheets['Masters_DH QA']

workbook = writer.book
center = workbook.add_format({'align': 'center', 'valign': 'vcenter'})

worksheet.set_column('A:A', 21, center)
worksheet.set_column('B:B', 23, center)
worksheet.set_column('C:C', 19, center)
worksheet.set_column('D:D', 14, center)
worksheet.set_column('E:E', 11, center)
worksheet.set_column('F:F', 11, center)

writer.save()



