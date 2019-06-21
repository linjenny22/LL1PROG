from win32com.client import Dispatch
import datetime as date
import os
from itertools import chain

due_date = '5/06/2019'

outlook = Dispatch("Outlook.Application").GetNamespace("MAPI")
inbox = outlook.Folders('MOCSReporting').Folders('Inbox')
messages = inbox.Items.restrict("[SentOn] > '4/19/2019 08:00 AM'")

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\FY19 Q3 Pre Prod\TADs Creation\testfolder'

for msg in messages:
    if 'Due %s' % str(due_date) in msg.Subject and 'MWBE_Prime' in msg.Subject:
        if msg.Attachments:
            for att in msg.Attachments:
                if '.xlsx' in str(att) and 'MWBE_Prime' in str(att):
                    att.SaveAsFile(path + '\\'+ str(att))

list = [x[x.find('TAD_')+4:x.find('.xlsx')] for x in os.listdir(path) if 'MWBE_Prime' in x]

agencies = ['DCLA','MOCJ', 'City Hall', 'OMB', 'Cityhall', 'OLR', 'NYCEM', 'Law', 'DCP', 'DOI','CCRB', 'NYPD', 'FDNY', 'ACS', 'HRA', 'DHS', 'DOC', 'DFTA', 'LPC', 'TLC', 'CCHR','DYCD', 'DOP', 'SBS', 'HPD', 'DOB', 'DOHMH', 'OATH', 'DEP', 'DSNY', 'BIC', 'DOF','DOT', 'DPR', 'DDC', 'DCAS', 'DoITT', 'DORIS', 'DCA', 'Mayoral', 'FISA']

l = []

for x in agencies:
    l.append([y for y in list if x in y])

y = [item for sublist in l for item in sublist]

y = [z.split(' ')[0] for z in y]

y1 = [z.split('_')[0].replace('-','') if z not in ('DCAS_856', 'DCAS_857') else z for z in y]

#########

outlook = Dispatch("Outlook.Application").GetNamespace("MAPI")
inbox = outlook.Folders('jenny.lin@mocs.nyc.gov').Folders('Inbox')
messages = inbox.Items.restrict("[SentOn] > '4/19/2019 08:00 AM'")

path = r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\LL1 PROG\FY19 Q3 Pre Prod\TADs Creation\testfolder'

for msg in messages:
    if 'Due %s' % str(due_date) in msg.Subject and 'MWBE_Prime' in msg.Subject:
        if msg.Attachments:
            for att in msg.Attachments:
                if '.xlsx' in str(att) and 'MWBE_Prime' in str(att):
                    att.SaveAsFile(path + '\\'+ str(att))

list = [x[x.find('TAD_')+4:x.find('.xlsx')] for x in os.listdir(path) if 'MWBE_Prime' in x]

agencies = ['DCLA','MOCJ', 'City Hall', 'OMB', 'Cityhall', 'OLR', 'NYCEM', 'Law', 'DCP', 'DOI','CCRB', 'NYPD', 'FDNY', 'ACS', 'HRA', 'DHS', 'DOC', 'DFTA', 'LPC', 'TLC', 'CCHR','DYCD', 'DOP', 'SBS', 'HPD', 'DOB', 'DOHMH', 'OATH', 'DEP', 'DSNY', 'BIC', 'DOF','DOT', 'DPR', 'DDC', 'DCAS', 'DoITT', 'DORIS', 'DCA', 'Mayoral', 'FISA']

l = []

for x in agencies:
    l.append([y for y in list if x in y])

y = [item for sublist in l for item in sublist]

y = [z.split(' ')[0] for z in y]

y2 = [z.split('_')[0].replace('-','') if z not in ('DCAS_856', 'DCAS_857') else z for z in y]

########

import os
import pandas as pd
import xlsxwriter
import datetime
from win32com.client import Dispatch

#Script contains email of EDS Team Members

today = datetime.datetime.now()

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

today = today.strftime('%Y-%m-%d %H:%M')

received = os.listdir(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\04 - TAD\LL1 Primes\FY%s\%s\Received' % (str(FY)[2:4],FQ))

if 'Thumbs.db' in received:
    received.remove('Thumbs.db')

received = pd.Series(received)
received.name = 'Received TADs'

o = os.listdir(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\04 - TAD\LL1 Primes\FY%s\%s\Sent' % (str(FY)[2:4],FQ))
if '.idea' in o:
    o.remove('.idea')

if 'Thumbs.db' in o:
    o.remove('Thumbs.db')

total_sent = pd.Series(o)
total_sent.name = 'Total TADs Sent'

r_name = [x.split('.')[0] for x in o]

outstanding_files = []

for x in [x.split('.')[0] for x in received]:
    if x in r_name:
        r_name.remove(x)
    else:
        outstanding_files.append(x)

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

outst_ag = [x for x in outst_ag]

try:
    outst_agency.remove('OMB')
except:
    pass

####

final = [x for x in outst_ag if x not in set(y1) if x not in set(y2)]

if len(final)>0:
    print (final)
else:
    print ('No Outstanding Agencies')