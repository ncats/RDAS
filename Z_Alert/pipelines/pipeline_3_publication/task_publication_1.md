# PubMed ID retrieval workflow

This diagram describes `NewPublicationDiscoveryTask.retrieve_pubmed_ids()` in
[`task_publication_1.py`](./task_publication_1.py). For each filtered GARD disease
name, `find_new_data()` passes the GARD ID, name, last update date, and today's
date to this method. The method yields one complete PubMed ID list at a time;
the caller then passes each nonempty list to `find_new_publications()`.

```mermaid
flowchart TD
    A["For each filtered GARD name"] --> B["Call retrieve_pubmed_ids with GARD ID, name, and date bounds"]
    B --> C{"API URL configured and date range valid?"}
    C -- No --> X["Raise an error"]
    C -- Yes --> D["Lowercase and normalize search term; build exact Title/Abstract query; push full date range"]
    D --> E{"Any date ranges pending?"}
    E -- No --> Z["Iterator finished"]
    E -- Yes --> F["Pop a range; request PubMed ESearch with EDAT bounds and retmax 9999"]
    F --> G{"Request and response valid?"}
    G -- "Timeout or connection failure" --> H{"Fewer than 10 attempts?"}
    H -- Yes --> I["Wait 1 second; retry the same range"] --> F
    H -- No --> X
    G -- "HTTP error, malformed result, or inconsistent IDs" --> X
    G -- Yes --> J{"Reported count exceeds IDs returned?"}
    J -- Yes --> K{"Range is one day?"}
    K -- Yes --> X
    K -- No --> L["Split at midpoint; push later range, then earlier range"] --> E
    J -- No --> M["Complete response, including zero matches"]
    M --> N["Insert response into publication_gard_pubmed if GARD ID, term, and date slice are absent; commit"]
    N -- "Database error" --> Y["Roll back and raise"]
    N -- "Inserted or already present" --> O["Yield IDs for this slice, including on reruns"]
    O --> P{"Any IDs?"}
    P -- Yes --> Q["Caller imports article data and GARD/search-term/PMID mappings"] --> E
    P -- No --> E
```

## Why date splitting is necessary

PubMed's `count` is the total number of matches, while `idlist` contains only
the IDs returned by the request. The method requests at most 9,999 IDs. For
example, a `GARD:0000016` search for `coma` reported 41,918 matches but returned
9,999 IDs for the original date range. That response is **not** stored or yielded.
The method splits the inclusive EDAT range into two nonoverlapping ranges and
keeps splitting until every saved slice has `count == len(idlist)`. The stack
processes earlier dates first.

Each complete response is stored with its exact `year_range`, lowercased
`search_term`, reported count, returned IDs, query translation, warnings, and
raw JSON. An existing row skips the insert, but its IDs are yielded again so a
rerun can finish article and mapping imports. A truncated single-day response
raises an error because splitting by date cannot recover its missing IDs.
