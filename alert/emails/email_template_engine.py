from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape


# Templates live beside this module, so email rendering works no matter which
# pipeline file calls the engine.
TEMPLATE_DIR = Path(__file__).resolve().parent
DEFAULT_TEMPLATE_NAME = "alert_email_template.html"


class EmailTemplateEngine:
    """
    Render RDAS email payloads with Jinja templates.

    The engine keeps a shared Jinja environment so callers only need to pass a
    payload and, optionally, a template name.
    """

    # Autoescaping is enabled for HTML/XML templates because payload values come
    # from pipeline output and may contain user-facing text.
    _environment = Environment(
        loader = FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape = select_autoescape(["html", "xml"]),
    )


    @classmethod
    def json_to_html_email_body(cls, payload, title: str = "RDAS Notification", template_name: str = DEFAULT_TEMPLATE_NAME):
        """
        Convert a JSON-like payload into an HTML email body.

        The payload dictionary is copied before adding a default title, so the
        caller's original object is not modified.
        """

        template = cls._environment.get_template(template_name)

        render_payload = dict(payload or {})
        # Templates expect a title value; callers can override it explicitly.
        render_payload.setdefault("title", title)

        return template.render(render_payload)



if __name__ == "__main__":

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
    }

    html_body = EmailTemplateEngine.json_to_html_email_body(payload)
    print(html_body)    

    # https://htmledit.squarefree.com/
    print(f'\n\n******\n\nPaste the html_body to https://htmledit.squarefree.com/ to see the result.\n\n*****\n\n')
