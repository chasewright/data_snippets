from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from psycopg2.extras import RealDictCursor
from pandas import DataFrame
import psycopg2 as pg
import pandas as pd
from datetime import datetime, date, timedelta
from email import encoders
from email.message import Message
import smtplib
import datetime
import mimetypes
import schedule
from schedule import every
import time
import os


def job():
    
    #set working directory
    %cd /Users/.../..../

    #connection to db
    try:
        conn = pg.connect("host= port= dbname= user= password=")
        print "I'm connected"
    except:
        print "I am unable to connect to the database"
    
    #set up cursor to execute and query    
    cur = conn.cursor(cursor_factory=RealDictCursor)

    #date calculations to set file date one day behind
    # batch job runs 24 hours to load Redshift data
    
    #today's date
    today = datetime.datetime.fromtimestamp(time.time()-1)
    
    #previous day calculation
    prev_day = today - datetime.timedelta(days=1)
    previous_day = prev_day.strftime('%Y_%m_%d')
    
    # two days ago (for removal of previous file)
    two_days = today - datetime.timedelta(days=2)
    two_days_ago = two_days.strftime('%Y_%m_%d')


    query = """SELECT * FROM marketing.adwords_t WHERE date BETWEEN '2015-01-01' and '2015-01-03' """

    cur.execute(query)

    #convert json objects into dataframe, set columns = sort order
    df = DataFrame(cur.fetchall(), columns=[''])
    
    # removes generic index from data frame
    df2 = df.set_index('first_column')

    #convert dataframe to csv and show as previous day report
    df2.to_csv('adword_tracker_' + previous_day + '.csv')

    #close connection
    conn.close()

    #set email parameters
    emailfrom = "your_email@test.com"
    
    #send to multiple people
    emailto = ["wolfgang@mozart.com", "carl@bach.com"]
    
    fileToSend = "/Users/.../.../adwords_tracker_" + previousday + ".csv"
    username = ""
    password = ""

    msg = MIMEMultipart()
    msg["From"] = emailfrom
    
    #join only necassary if you send multiple people email
    msg['To'] = ", ".join(emailto)
    msg["Subject"] = "Adwords Tracker CSV " + previousday

    #attachment file name
    name = 'adword_tracker_' + previousday + '.csv'

    # Create the body of the message (a plain-text and an HTML version).
    body = """Hi! \n\nAttached is the most recent Adwords Tracker dataset."""
    msg_body = MIMEText(body, 'plain')
    msg.attach(msg_body)

    # Attach dataset as CSV
    ctype, encoding = mimetypes.guess_type(fileToSend)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"

    maintype, subtype = ctype.split("/", 1)

    if maintype == "text":
        fp = open(fileToSend)
        # Note: we should handle calculating the charset
        attachment = MIMEText(fp.read(), _subtype=subtype)
        fp.close()
    else:
        fp = open(fileToSend, "rb")
        attachment = MIMEBase(maintype, subtype)
        attachment.set_payload(fp.read())
        fp.close()
        encoders.encode_base64(attachment)
        
    attachment.add_header("Content-Disposition", "attachment", filename=name)
    msg.attach(attachment)


    # Send the message via SMTP server. Enter host and port name
    s = smtplib.SMTP(host, port)
    s.ehlo()
    s.starttls()
    s.login(username, password)
    # sendmail function takes 3 arguments: sender's address, recipient's address
    s.sendmail(emailfrom, emailto, msg.as_string())
    s.quit()


#schedule 'job' to run every at 6:00 am
schedule.every().day.at("06:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
