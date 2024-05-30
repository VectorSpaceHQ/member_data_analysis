#!/usr/bin/env python

# Import smtplib for the actual sending function
import smtplib, ssl
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

import datetime as dt
from os import walk
from os.path import join
import os

file_path = os.path.split(os.path.realpath(__file__))[0]
os.chdir(file_path)

me = 'adam@vector-space.org'
you = ['info@vector-space.org','mollie@vector-space.org']

date = dt.datetime.now().strftime('%B %Y')

msg = MIMEMultipart()
msg['Subject'] = 'Member Data Report: {}'.format(date)
msg['From'] = me

body = "Attached is this month's member data analysis. \n\nThis is an automated email."
msg.attach(MIMEText(body, 'plain'))

mypath = "./reports/"
for (dirpath, dirnames, filenames) in walk(mypath):
    filename = sorted(filenames)[-1]
    path_to_pdf = join(dirpath, filename) # Most recent

with open(path_to_pdf, 'rb') as fp:
    pdf = MIMEApplication(fp.read(), subtype="pdf")
pdf.add_header('Content-Disposition','attachment',filename=str(filename))
msg.attach(pdf)

port = 465
smtp_server = "smtp.zoho.com"
with open('email.pw','r') as f:
    password = f.readline().strip('\n')

context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(me, password)
    server.sendmail(me, you, msg.as_string())

print("email successfully sent")
