from __future__ import annotations

import pandas as pd


TABLE_NAME = "interview_events"


def fetch_interview_events(supabase_client, date_from: str | None = None, date_to: str | None = None, halls=None) -> pd.DataFrame:
    query = supabase_client.table(TABLE_NAME).select("*")

    if date_from:
        query = query.gte("event_date", date_from)
    if date_to:
        query = query.lte("event_date", date_to)
    if halls:
        query = query.in_("hall_name", list(halls))

    result = query.execute()
    return pd.DataFrame(result.data or [])
