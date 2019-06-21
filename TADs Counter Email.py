import os
import pandas as pd
import xlsxwriter
import datetime as dt
from win32com.client import Dispatch

today = dt.datetime.now().date()

t = dt.datetime.now().date()

data_path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\PRODUCTION\Datasets'

#'marcus.cerroni@mocs.nyc.gov;val.strokopytova@mocs.nyc.gov;jenny.lin@mocs.nyc.gov;Joshua.bonilla@mocs.nyc.gov;Bo.Peng@mocs.nyc.gov;'

email_list = 'jenny.lin@mocs.nyc.gov'

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

[date_range_start, date_range_end, FQ, FY] = date_range(t)

start_date = str(date_range_start.month) + '/' + str(date_range_start.day) + '/' + str(date_range_start.year)
end_date = str(date_range_end.month) + '/' + str(date_range_end.day) + '/' + str(date_range_end.year)

today = today.strftime('%Y-%m-%d %H:%M')

received = os.listdir(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\04 - TAD\LL1 Primes\FY%s\%s\Received' % (str(FY)[2:4],FQ))

try:
    received.remove('Archived')
except:
    pass

try:
    received.remove('README.txt')
except:
    pass

for x in ['Archived', 'README.txt', 'Thumbs.db']:
    if x in received:
        received.remove(x)

received = pd.Series(received)
received.name = 'Received TADs'

o = os.listdir(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\04 - TAD\LL1 Primes\FY%s\%s\Sent' % (str(FY)[2:4],FQ))
if '.idea' in o:
    o.remove('.idea')

total_sent = pd.Series(o)
total_sent.name = 'Total TADs Sent'

r_name = [x.split('.')[0] for x in o]

outstanding_files = []

for x in [x.split('.')[0] for x in received]:
    if x in r_name:
        r_name.remove(x)
    else:
        outstanding_files.append(x)
        print('Received But Not Taken Out of Total')
        print(x)


outstanding = pd.Series(r_name)
outstanding.name = 'Outstanding TADs'

outstanding_empty = pd.Series([])
outstanding_empty.name = 'Outstanding TADs'

outst_agency = [x.split('_')[4] for x in outstanding if x[-2:] not in ('56','57')] + ['DCAS ' + x.split('_')[5] for x in outstanding if x[-2:] in ('56','57')]

outst_agency = sorted(outst_agency)

agg = ''

for x in outstanding_files:
    agg += x


a_rem = [y for y in [x.replace(' ', '_') for x in outst_agency]]

ag_delta = []
for x in a_rem:
    if x in agg:
        ag_delta.append(x)

ag_delta = [x.replace('_',' ') for x in ag_delta]

outst_ag = set(outst_agency) - set(ag_delta)

outst_ag = list(outst_ag)

try:
    outst_agency.remove('OMB')
except:
    pass

filepath = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\LL1 TADs Counter and E-mail MOCS INTERNAL' #os.getcwd()

writer = pd.ExcelWriter(filepath + '/' + 'TADs_UpdateSheet.xlsx', engine = 'xlsxwriter')

received.to_excel(writer, startrow=2, startcol=3, sheet_name= 'LL1 TADs Update')

workbook = writer.book
worksheet = writer.sheets['LL1 TADs Update']

percentage_signs = workbook.add_format({'num_format': '0%'})
percentage_signs.set_align('center')
center = workbook.add_format({})
center.set_align('center')

worksheet.write('A1', 'Report Generated:')
worksheet.write('B1', str(today), center)

worksheet.write('A4', 'TADs Received To-Date:')
worksheet.write('B4', len(received), center)

worksheet.write('A6', 'TADs Outstanding To-Date:')
worksheet.write('B6', len(outst_ag), center)

worksheet.write('A8', '% of Total Received To-Date:')
worksheet.write('B8', float(len(received))/float(len(total_sent)), percentage_signs)

worksheet.set_column('A:A', 27, center)
worksheet.set_column('B:B', 17)
worksheet.set_column('D:D', 6)
worksheet.set_column('E:E', 72)
worksheet.set_column('F:F', 41)
worksheet.set_column('G:G', 36)
worksheet.set_column('H:H', 6)
worksheet.set_column('I:I', 40)

if len(outst_ag)>0:

    outstanding.to_excel(writer, startrow=2, startcol=5, sheet_name= 'LL1 TADs Update', index = False)

    outstanding_len = len(outstanding)

else:
    outstanding_empty.to_excel(writer, startrow=2, startcol=5, sheet_name= 'LL1 TADs Update', index = False)

total_sent.to_excel(writer, startrow=2, startcol=7, sheet_name= 'LL1 TADs Update')

writer.save()

emailer_lists = tuple([len(outst_agency)] + outst_agency)

if len(outst_ag)>0:
    mailer = Dispatch("Outlook.Application")
    msg = mailer.CreateItem(0)
    emailTo = email_list
    emailSubject = '[MOCS INTERNAL] FY%s %s LL1 TADs Status Update' % (str(FY)[2:4], FQ)
    msg.To = emailTo
    msg.Subject = emailSubject
    msg.HTMLBody = str("<html>Dear EDS Team, <br>" \
                       "<br>Count of Outstanding Agencies<br>"
                       "<br>" \
                       "<b>%s</b> <br>" \
                       "<br>" \
                       "Outstanding Agencies <br>" \
                       "<br>" \
                       + str("<b>%s</b> <br>" % (str("%s <br>" * (len(outst_agency)))) + \
                       "Best, <br>" \
                       "<br>" \
                       "Jenny Lin <br>" \
                       "Senior Data Analyst | Mayor's Office of Contract Services <br>" \
                       "(212) 720-0861")) % (emailer_lists)
    attachment = filepath + '/' + 'TADs_UpdateSheet.xlsx'
    msg.Attachments.Add(attachment)
    msg.Send()
else:
    mailer = Dispatch("Outlook.Application")
    msg = mailer.CreateItem(0)
    emailTo = email_list
    emailSubject = '[MOCS INTERNAL] FY%s %s LL1 TADs Status Update' % (str(FY)[2:4], FQ)
    msg.To = emailTo
    msg.Subject = emailSubject
    msg.HTMLBody = str("<html>Dear EDS Team, <br>" \
                       "<br>Congrats! All LL1 TADs were received.<br>"
                       "<br>Best, <br>" \
                       "<br>" \
                       "Jenny Lin <br>" \
                       "Senior Data Analyst | Mayor's Office of Contract Services <br>" \
                       "(212) 720-0861")
    attachment = filepath + '/' + 'TADs_UpdateSheet.xlsx'
    msg.Attachments.Add(attachment)
    msg.Send()