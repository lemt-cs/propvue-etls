import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from dotenv import load_dotenv
import os
from pymongo import MongoClient
load_dotenv()
# mongo_uri = os.getenv("MONGODB_URI_LOCAL")
# client = MongoClient(mongo_uri)
# db = client['agentbox']
# coll = db['exec_network_report']
# df = pd.json_normalize(coll.find(), sep="_")

smtp_server = "smtp.gmail.com"
smtp_port = 587
smtp_user = os.getenv("SMTP_EMAIL")
smtp_password = os.getenv("SMTP_PASSWORD")
recipient = "lemuel.torrefiel@belleproperty.com"

def email_df(df, subject):
    # Convert to HTML table (with styling for readability)
    html_table = df.to_html(index=False, border=1, justify="center")

    email_body = f"""
    <html>
        <body>
            <p>Hello,</p>
            <p>Here is the data you requested:</p>
            {html_table}
            <p>Best regards,<br>Your ETL Script</p>
        </body>
    </html>
    """

    # Email Configuration
    sender_email = smtp_user
    receiver_email = recipient

    # Setup MIME message
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject

    # Attach the HTML body
    msg.attach(MIMEText(email_body, "html"))

    # Send Email (Modify SMTP settings as per your setup)
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

    print("Email sent successfully!")

# email_df(df, subject="Listings data")