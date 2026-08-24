# Grant Email Alert Process

This document describes how grant updates are included in RDAS alert emails by
`Z_Alert/alert_sender.py`.

## Related Files

- `Z_Alert/main.py`
  - Runs `AlertPipelineRunner.send_alert_emails()` as Step 8.
  - Step 8 creates `AlertSender` and calls `find_new_and_send_alert()`.
- `Z_Alert/alert_sender.py`
  - Builds per-user alert payloads from Firebase subscriptions and MySQL data.
  - Sends user alert emails and the admin summary email.
  - Delegates grant-specific alert logic to `GrantAlertHelper`.
- `Z_Alert/pipelines/pipeline_4_grant/grant_alert_helper.py`
  - Finds alertable previous-year grants for a GARD ID.
  - Excludes grant alerts already recorded in `grant_alert_sent`.
  - Saves newly sent grant alert pairs after successful user emails.
- `rdas_db_schema.sql`
  - Defines `grant_alert_sent`.

## Timing

The grant pipeline is annual:

1. `Z_Alert/main_grant.py` runs once a year.
2. It downloads and imports the latest completed NIH RePORTER grant year.
3. It builds grant/GARD relationships.
4. It updates Memgraph grant nodes and relationships.
5. It runs `runner.run_pipeline_wrapup()`.

`run_pipeline_wrapup()` resets `is_new = 0` for grant tables, including:

- `grant_project`
- `grant_gard_project_relation`
- `grant_gard_project_relation_unique_application_id`
- `grant_project_annotation`

Because `main_grant.py` runs before `Z_Alert/main.py`, grant email alerts must
not depend on `grant_project.is_new = 1` or
`grant_gard_project_relation.is_new = 1`. Those flags may already be reset by
the time `AlertSender` runs.

## Grant Alert Rule

Grant email alerts use this rule:

> For each subscribed GARD ID, alert only previous-fiscal-year grant projects
> that have not already been recorded in `grant_alert_sent`.

The helper uses:

- `grant_gard_project_relation.gard_id`
- `grant_gard_project_relation.application_id`
- `grant_project.APPLICATION_ID`
- `grant_project.FY`
- `grant_alert_sent.gard_id`
- `grant_alert_sent.application_id`
- `grant_alert_sent.funding_year`

The previous fiscal year is selected with:

```sql
p.FY = YEAR(CURDATE()) - 1
```

## Send-Once Ledger

`grant_alert_sent` prevents the same GARD/grant/year alert from being sent more
than once globally:

```sql
CREATE TABLE `grant_alert_sent` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `gard_id` varchar(45) NOT NULL,
  `application_id` bigint(20) NOT NULL,
  `funding_year` int(11) NOT NULL,
  `first_alert_sent_at` datetime NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_grant_alert_sent` (`gard_id`,`application_id`,`funding_year`)
);
```

The unique key means one tuple can only be stored once:

```text
(gard_id, application_id, funding_year)
```

This is a global alert ledger. If a user subscribes to a GARD ID later, they do
not receive old grant alerts already stored in `grant_alert_sent`.

## AlertSender Flow

`AlertSender.find_new_and_send_alert()` does the following:

1. Create `EmailClient`, `FirebaseAgent`, and `GrantAlertHelper`.
2. Load active Firebase users and their subscribed GARD IDs.
3. For each user:
   - Query clinical-trial counts by GARD ID.
   - Query publication counts by GARD ID.
   - Ask `GrantAlertHelper.find_alertable_grants(gard_id)` for grant alert data.
4. For each subscribed GARD ID:
   - If any clinical-trial, publication, or grant row exists, add that disease
     to the email payload.
   - If grant rows exist, collect the grant alert pairs in memory.
5. Send the user alert email.
6. Only after the user email succeeds, add that user's grant pairs to the
   run-level `alerted_grant_pairs` set.
7. After all user emails are attempted:
   - Save the full alert summary JSON to `alert_summary`.
   - Call `GrantAlertHelper.save_alert_sent_pairs(alerted_grant_pairs)`.
8. Send the admin summary email.

The grant ledger update happens after the user email loop so every current
subscriber can receive the first grant alert in the same run. Future subscribers
will not receive those same old grant alerts.

## GrantAlertHelper Methods

### `find_alertable_grants(gard_id)`

Returns two values:

```python
grant_row, grant_pairs = grantAlertHelper.find_alertable_grants(gard_id)
```

`grant_row` is shaped like the clinical-trial and publication rows used by
`AlertSender`:

```python
("GARD:0023606", "grants", 2)
```

`grant_pairs` contains the exact tuples that should be written to
`grant_alert_sent` if the user email succeeds:

```python
{
    ("GARD:0023606", 12345678, 2025),
    ("GARD:0023606", 12345679, 2025),
}
```

### `find_alertable_grant_pairs(gard_id)`

Runs the MySQL query that:

- Starts from `grant_gard_project_relation`.
- Joins to `grant_project`.
- Filters to the previous fiscal year.
- Left joins `grant_alert_sent`.
- Keeps only tuples where the ledger row does not already exist.
- Uses `SELECT DISTINCT` to avoid duplicates when one project matches the same
  GARD ID through multiple grant relationship terms.

### `save_alert_sent_pairs(grant_alert_pairs)`

Writes the successfully sent grant pairs to `grant_alert_sent`:

```sql
INSERT IGNORE INTO grant_alert_sent (gard_id, application_id, funding_year)
VALUES (%s, %s, %s)
```

`INSERT IGNORE` works with the unique key so duplicate pairs from multiple users
in the same run are harmless.

## Email Payload Shape

The email template expects dataset counts under each GARD ID. Grant counts use
the same shape as articles and clinical trials:

```json
{
  "data": {
    "datasets": ["articles", "trials", "grants"],
    "subscriptions": {
      "GARD:0023606": "pediatric lymphoma"
    },
    "GARD:0023606": {
      "articles": 3,
      "trials": 1,
      "grants": 2
    }
  }
}
```

The internal helper data is not sent directly to the email template. It is used
to record the global send-once ledger after successful email delivery.
