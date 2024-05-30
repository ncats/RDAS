
import sys

sys.path.append('/home/aom2/RDAS')
sys.path.append('/home/aom2/RDAS/emails')
import sysvars
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from jinja2 import Environment, FileSystemLoader
import os


def send_email(subject, html, recipient):
    print("sending emails to::", recipient)
    sender = ""  # Replace with your email
    password = ""  # Replace with your email password

    # Set up the email
    msg = MIMEMultipart('alternative')
    msg['From'] = ""
    # msg['To'] = ""
    msg['Subject'] = subject

    # Attach both plain text and HTML parts
    # part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    # msg.attach(part1)
    msg.attach(part2)

    # Send the email
    server = smtplib.SMTP('', 587)  # Replace with SMTP server and port
    server.starttls()
    server.login(sender, password)
    text = msg.as_string()
    server.sendmail(sender, recipient, msg.as_string())
    server.quit()


# def render_template(filename, data={}):
#     # template_dir = "path/to/templates"
#     template_dir = os.getcwd()
#     env = Environment(loader=FileSystemLoader(template_dir))
#     template_path = filename  # Relative to the template_dir
#     template = env.get_template(template_path)
#     return template.render(data=data)
    
# the fill_template method was remove, becaus the generation of message was integrated into the html template.


