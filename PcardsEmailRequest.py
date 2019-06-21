from win32com.client import Dispatch
import os
import pandas as pd
import xlsxwriter
import datetime

#Run this the First Monday of FQ

today = datetime.datetime.now()
due_date = today + datetime.timedelta(days = 7)

def date_range(today): #Non-Cumulative
    if today.month >= 7 and today.month <= 9:
        date_range_start = datetime.date(today.year, 4, 1) #Dates for whole cumulative year.
        date_range_end = datetime.date(today.year, 6, 30)
        FY = today.year + 1
        FQ = 'Q4'
    elif today.month >= 10 and today.month <= 12:
        date_range_start = datetime.date(today.year, 7, 1) #First Quarter
        date_range_end = datetime.date(today.year, 9, 30)
        FY = today.year + 1
        FQ = 'Q1'
    elif today.month >= 1 and today.month <= 3:
        date_range_start = datetime.date(today.year - 1, 10, 1) #Second Quarter
        date_range_end = datetime.date(today.year - 1, 12, 31)
        FY = today.year
        FQ = 'Q2'
    elif today.month >= 4 and today.month <= 6:
        date_range_start = datetime.date(today.year, 1, 1) #Third Quarter
        date_range_end = datetime.date(today.year, 3, 31)
        FY = today.year
        FQ = 'Q3'
    return [date_range_start, date_range_end, FY, FQ]

[date_range_start, date_range_end, FY, FQ] = date_range(today)

start_date = str(date_range_start.month) + '/' + str(date_range_start.day) + '/' + str(date_range_start.year)
end_date = str(date_range_end.month) + '/' + str(date_range_end.day) + '/' + str(date_range_end.year)

due = due_date.strftime("%A") + ' ' + due_date.strftime("%B") + ' ' + due_date.strftime("%d") + ', ' + due_date.strftime("%Y")

start = date_range_start.strftime("%B") + ' ' + date_range_start.strftime("%d") +', ' + date_range_start.strftime("%Y")
end = date_range_end.strftime("%B") + ' ' + date_range_end.strftime("%d") +', ' + date_range_end.strftime("%Y")

######################################

print (FY)
print (FQ)
print (due_date)

mailer = Dispatch("Outlook.Application")
msg = mailer.CreateItem(0)
emailTo = r'DBork@dcas.nyc.gov'
emailCC = r'marcus.cerroni@mocs.nyc.gov;Val.Strokopytova@mocs.nyc.gov;DAS@mocs.nyc.gov;Bo.Peng@mocs.nyc.gov;Joshua.Bonilla@mocs.nyc.gov'
emailSubject = '[Request!] P-Card Data for FY%s %s' % (str(FY)[2:4], str(FQ))
msg.To = emailTo
msg.CC = emailCC
msg.Subject = emailSubject
msg.HTMLBody = "<html>Hi Diana, <br>" \
               "<br>" \
               "Hope this finds you well.<br>" \
               "<br>" \
               "MOCS is reaching out to obtain FY%s Q%s P-Card data. We report quarterly on M/WBE utilization to City Hall (OneNYC) and need this information to proceed.<br>" \
               "<br>" \
               "Please have your agency send its M/WBE contract data for FY%s Q%s (%s to %s) by <b>%s</b>.<br>" \
               "<br>" \
               "Thank you in advance, and please email us if you have any questions. <br>" \
               "<br>" \
               "Best, <br>" \
               "<br>" \
               "Jenny Lin <br>" \
               "Senior Data Analyst | Mayor's Office of Contract Services <br>" \
               "(212) 720-0861" % (str(FY)[2:4], str(FQ)[1], str(FY)[2:4], str(FQ)[1], str(start), str(end), due)
msg.Send()
