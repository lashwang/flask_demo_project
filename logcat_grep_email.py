#!/usr/bin/python
# -*- coding: utf-8 -*-
import fire
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
import smtplib


recipients = ['swang@seven.com']
fromaddr = "noreply@seven.com"

def main():
    pass

def send_email_report(msg):
    server = smtplib.SMTP('10.10.10.17:25')
    text = msg.as_string()
    server.sendmail(fromaddr, recipients, text)
    server.quit()


def new_email_msg(subject,content):
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = "noreply@seven.com"
    msg['To'] = ", ".join(recipients)
    msg['Date'] = formatdate(localtime=True)
    msg.attach(MIMEText(content, _subtype='plain', _charset='utf-8'))
    msg.preamble = 'This is a multi-part message in MIME format.'

    return msg

if __name__ == "__main__":
    main()
