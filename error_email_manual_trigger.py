import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from dotenv import load_dotenv
import os

load_dotenv()

smtp_server = "smtp.gmail.com"
smtp_port = 587
smtp_user = os.getenv("SMTP_EMAIL")
smtp_password = os.getenv("SMTP_PASSWORD")
recipient = os.getenv("SHAREPOINT_EMAIL")

def error_send_email(subject, msg, original_message_id):
    msg = MIMEText(msg)
    msg["Subject"] = "Re: " + subject  # Prefix with "Re:" to indicate a reply
    msg["From"] = formataddr(("Lemuel Torrefiel", smtp_user))  # Set sender with name
    msg["To"] = recipient
    
    # Set headers for replying to an email
    if original_message_id:
        msg["In-Reply-To"] = original_message_id
        msg["References"] = original_message_id

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(from_addr=smtp_user, to_addrs=recipient, msg=msg.as_string())
        server.quit()
        print("Reply email sent successfully")
    except Exception as e:
        print(f"Failed to send reply email: {e}")
