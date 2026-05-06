import smtplib
from email.message import EmailMessage
from pathlib import Path

from .email_template_engine import EmailTemplateEngine
from utils.tools import _format_recipients, _load_json_file, _recipient_list

CONFIG_PATH = Path(__file__).resolve().with_name("email_config.json")

class EmailClient:

    def __init__(self, mail_to: str = None, mail_from: str = None, mail_cc: str = None):

        config = _load_json_file(CONFIG_PATH)

        self.mail_to = mail_to or config["DEFAULT_TO"]
        self.mail_from = mail_from or config["DEFAULT_FROM"]
        self.mail_cc = mail_cc or config["DEFAULT_CC"]
        self.smtp_protocol = config["SMTP_PROTOCOL"]
        self.smtp_host = config["SMTP_HOST"]
        self.smtp_port = config["SMTP_PORT"]
        self.smtp_auth = config["SMTP_AUTH"]
        self.smtp_starttls_enable = config["SMTP_STARTTLS_ENABLE"]
        self.smtp_timeout = config["SMTP_TIMEOUT_MS"] / 1000.0
        self.smtp_connection_timeout = config["SMTP_CONNECTION_TIMEOUT_MS"] / 1000.0


    def _build_message(self, subject: str, body: str, mail_to: str = None, mail_cc: str = None):

        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self.mail_from
        message["To"] = mail_to or self.mail_to

        cc_value = mail_cc if mail_cc is not None else self.mail_cc
        if cc_value:
            message["Cc"] = cc_value

        message.set_content(body)

        return message
    

    def _build_html_message(self, subject: str, html_body: str, mail_to: str = None, mail_cc: str = None):

        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self.mail_from
        message["To"] = mail_to or self.mail_to

        cc_value = mail_cc if mail_cc is not None else self.mail_cc
        if cc_value:
            message["Cc"] = cc_value

        message.set_content("This email contains HTML content.")
        message.add_alternative(html_body, subtype="html")

        return message


    def send_email(self, subject: str, body: str, mail_to: str = None, mail_cc: str = None):

        message = self._build_message(subject, body, mail_to=mail_to, mail_cc=mail_cc)

        recipients = [message["To"]]
        if message.get("Cc"):
            recipients.extend([email.strip() for email in message["Cc"].split(",") if email.strip()])

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.smtp_connection_timeout) as server:
            server.timeout = self.smtp_timeout
            if self.smtp_starttls_enable:
                server.starttls()
            server.send_message(message, to_addrs=recipients)


    def send_html_alert_email(self, subject: str, payload, title: str = "RDAS Notification", mail_to: str = None, mail_cc: str = None):

        html_body = EmailTemplateEngine.json_to_html_email_body(payload, title=title)

        message = self._build_html_message(subject, html_body, mail_to=mail_to, mail_cc=mail_cc)

        recipients = [message["To"]]
        if message.get("Cc"):
            recipients.extend([email.strip() for email in message["Cc"].split(",") if email.strip()])

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.smtp_connection_timeout) as server:
            server.timeout = self.smtp_timeout
            if self.smtp_starttls_enable:
                server.starttls()
            server.send_message(message, to_addrs=recipients)


    def send_html_summary_email(self, subject: str, all_updates_summary, title: str = "RDAS Alert Summary", mail_to: list = None, mail_cc: list = None):

        payload = {
            "all_updates_summary": all_updates_summary or []
        }

        html_body = EmailTemplateEngine.json_to_html_email_body(
            payload,
            title=title,
            template_name="alert_summary_email_template.html"
        )

        to_value = _format_recipients(mail_to or self.mail_to)
        cc_value = _format_recipients(mail_cc if mail_cc is not None else self.mail_cc)
        message = self._build_html_message(subject, html_body, mail_to=to_value, mail_cc=cc_value)

        recipients = _recipient_list(to_value, cc_value)

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.smtp_connection_timeout) as server:
            server.timeout = self.smtp_timeout
            if self.smtp_starttls_enable:
                server.starttls()
            server.send_message(message, to_addrs=recipients)



if __name__ == "__main__":

    client = EmailClient()

    client.send_html_alert_email(
        subject="RDAS EmailClient Test",

        payload = {
            "data": {
                "total": 3,
                "datasets": ["articles", "trials", "grants"],
                "subscriptions": {
                    "GARD:0000001": "Disease A",
                    "GARD:0000002": "Disease B"
                },
                "GARD:0000001": {
                    "articles": 2,
                    "trials": 1,
                    "grants": 0
                },
                "GARD:0000002": {
                    "articles": 0,
                    "trials": 0,
                    "grants": 4
                },
                "update_date_start": "2026-03-01",
                "update_date_end": "2026-03-31"
            }
        },
         
        mail_to='tongan.zhao@nih.gov',
        mail_cc='zhaotongan@gmail.com',
    )
    
    print("Test email sent.")
