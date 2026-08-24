# AlertSender Workflow

This document describes the current workflow in `Z_Alert/alert_sender.py`.

## Purpose

`AlertSender` builds and sends RDAS alert emails for users who have subscribed
to GARD diseases. It combines:

- Firebase user subscriptions.
- MySQL clinical-trial alert rows.
- MySQL publication alert rows.
- Previous-year grant alert rows from `GrantAlertHelper`.
- HTML email rendering and delivery through `EmailClient`.
- A MySQL `alert_summary` record for the run.

## Entry Point

`Z_Alert/main.py` runs alert email delivery as Step 8:

```python
runner._run_step_with_timing(
    "Step 8: send_alert_emails()",
    runner.send_alert_emails,
)
```

`send_alert_emails()` creates `AlertSender(days)` and calls:

```python
alert_sender.find_new_and_send_alert()
```

`days` is the alert look-back window used for the displayed date range in the
email payload.

## Initialization

`AlertSender.__init__()`:

- Calls `PipelineBase(init_mysql=True, init_memgraph=False)`.
- Opens a MySQL connection.
- Does not open a Memgraph connection.
- Sets the email subject to `RDAS Notification`.
- Stores `LOOK_BACK_DAYS`.

`find_new_data()` and `process_new_data()` are intentionally not implemented for
this class because `AlertSender` is not a normal per-disease pipeline task.

## Main Objects

Inside `find_new_and_send_alert()`, the sender creates:

- `EmailClient`
  - Renders and sends the user alert email.
  - Renders and sends the admin summary email.
- `FirebaseAgent`
  - Loads active Firebase users and their Firestore GARD subscriptions.
- `GrantAlertHelper`
  - Finds alertable previous-year grant rows.
  - Tracks grant alert pairs that should be recorded in `grant_alert_sent`.

## User Subscription Input

The Firebase user data is expected to include:

```json
{
  "display_name": "Timothy Sheils",
  "email": "timothy.sheils@ncats.nih.gov",
  "gard_id_list": [
    "GARD:0007704",
    "GARD:0007827"
  ],
  "subscriptions": {
    "GARD:0007704": "gastric cancer",
    "GARD:0007827": "tuberculosis"
  }
}
```

The sender skips users with an empty `gard_id_list`.

## Query Sources

For each subscribed GARD ID, `AlertSender` looks for alertable data from three
sources.

### Clinical Trials

Clinical-trial counts come from `clinical_trial`:

```sql
SELECT
    ct.gardId,
    'trials' AS item_name,
    COUNT(*) AS new_items_count
FROM clinical_trial AS ct
WHERE ct.gardId = %s
AND ct.is_new = 1
AND COALESCE(ct.alert_sent, '0') = '0'
GROUP BY ct.gardId
```

Current behavior:

- Requires `clinical_trial.is_new = 1`.
- Requires `clinical_trial.alert_sent` to be missing or `0`.
- Returns a row shaped as `(gard_id, "trials", count)`.
- `alert_sender.py` reads `alert_sent`, but does not update
  `clinical_trial.alert_sent` after delivery.

### Publications

Publication counts come from `publication_article` joined to
`publication_gard_searchterm_pubmed_mapping`:

```sql
SELECT
    m.gard_id,
    'articles' AS item_name,
    COUNT(*) AS new_items_count
FROM publication_article a
INNER JOIN publication_gard_searchterm_pubmed_mapping m
    ON a.pubmed_id = m.pubmed_id
    AND m.gard_id = %s
WHERE a.is_new = 1
GROUP BY m.gard_id
```

Current behavior:

- Requires `publication_article.is_new = 1`.
- Uses the GARD/search-term PubMed mapping table to connect articles to GARD.
- Returns a row shaped as `(gard_id, "articles", count)`.

### Grants

Grant counts are handled by `GrantAlertHelper`:

```python
grant_row, grant_pairs = grantAlertHelper.find_alertable_grants(gard_id)
```

Current behavior:

- Uses previous fiscal year only.
- Does not depend on grant `is_new` flags.
- Excludes grant pairs already stored in `grant_alert_sent`.
- Returns `grant_row` shaped as `(gard_id, "grants", count)`.
- Returns `grant_pairs` shaped as
  `(gard_id, application_id, funding_year)` tuples.

The sender appends `grant_row` to the same `rows` list as articles and trials so
payload assembly stays generic.

## Payload Assembly

For each user, `AlertSender` starts with:

```python
payload = {
    "data": {
        "total": 0,
        "datasets": [],
        "subscriptions": dict(user_subscriptions),
        "update_date_start": update_date_start.strftime("%Y-%m-%d"),
        "update_date_end": update_date_end.strftime("%Y-%m-%d"),
    }
}
```

For each subscribed GARD ID:

1. Query clinical trials and publications.
2. Ask `GrantAlertHelper` for grant data.
3. If there are no rows, skip that GARD ID.
4. If rows exist:
   - Add `payload["data"][gard_id] = {}`.
   - Add the GARD ID to `active_subscriptions`.
   - Add each count by item name:

```python
payload["data"][gardId][item_name] = new_items_count
```

The dataset columns are ordered as:

```python
("articles", "trials", "grants")
```

The final user payload shape is:

```json
{
  "data": {
    "total": 1,
    "datasets": ["articles", "trials", "grants"],
    "subscriptions": {
      "GARD:0023606": "pediatric lymphoma"
    },
    "update_date_start": "2026-08-17",
    "update_date_end": "2026-08-24",
    "GARD:0023606": {
      "articles": 3,
      "trials": 1,
      "grants": 2
    }
  }
}
```

## User Email Sending

If a user has no active subscriptions with alertable rows, no user alert email
is sent.

If the payload has at least one active subscription, the sender calls:

```python
emailClient.send_html_alert_email(
    subject=self.subject,
    payload=payload,
    mail_to=user.get("email"),
    mail_cc=None,
)
```

On success:

- The full payload is logged.
- The user section is appended to `all_updates_summary`.
- Any grant pairs in that successful user email are added to
  `alerted_grant_pairs`.

On failure:

- The exception is logged.
- The user is skipped.
- That user's grant pairs are not added to the run-level grant alert ledger set.

## Grant Alert Tracking

Grant alert tracking is intentionally delayed until after user email delivery.

During the user loop:

- `user_grant_alert_pairs` stores grant pairs for one user.
- `alerted_grant_pairs` stores grant pairs from successful user emails only.

After all user emails are attempted, the sender calls:

```python
grantAlertHelper.save_alert_sent_pairs(alerted_grant_pairs)
```

`GrantAlertHelper` writes to `grant_alert_sent` with:

```sql
INSERT IGNORE INTO grant_alert_sent (gard_id, application_id, funding_year)
VALUES (%s, %s, %s)
```

This uses the `grant_alert_sent` unique key on:

```text
gard_id, application_id, funding_year
```

This prevents old grant alerts from being sent again to later subscribers.

## Alert Summary Storage

After the user loop, `AlertSender` saves the run summary to MySQL:

```python
self.save_alert_summary(datetime.now(), update_date_start, update_date_end, all_updates_summary)
```

`save_alert_summary()` inserts into `alert_summary`:

```sql
INSERT INTO alert_summary (date_sent, from_date, to_date, summary)
VALUES (%s, %s, %s, %s)
```

The `summary` column stores `all_updates_summary` as JSON.

## Admin Summary Email

After saving the summary JSON and updating grant alert tracking, the sender sends
an admin summary email.

Recipients come from:

```text
ALERT_SUMMARY_EMAIL_RECIPIENTS
```

If no user alert emails were sent, the admin subject is changed to:

```text
RDAS Notification Summary - No Updates
```

If `ALERT_SUMMARY_EMAIL_RECIPIENTS` is empty, the sender logs an error and does
not send the admin summary.

## Cleanup

The `finally` block always closes:

- The `AlertSender` MySQL connection through `self.close()`.
- The Firebase connection through `firebaseAgent.close()` when it exists.

## Important Notes

- `AlertSender` does not use Memgraph.
- Clinical-trial and publication alert rows still depend on `is_new`.
- Grant alert rows do not depend on grant `is_new`, because
  `main_grant.py` runs its wrap-up before `main.py` sends alert emails.
- Grant send-once behavior is global by `(gard_id, application_id,
  funding_year)`, not per user.
- `grant_alert_sent` is expected to already exist from `rdas_db_schema.sql`.
