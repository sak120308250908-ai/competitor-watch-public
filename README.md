# Competitor Watch

Streamlit app for competitor store analysis using:

- `slot_data` in Supabase
- `interview_events` in Supabase

## Required secrets

Set these in Streamlit Community Cloud:

```toml
SUPABASE_URL = "..."
SUPABASE_KEY = "..."
```

## Main file path

```text
app.py
```

## Local run

```bash
streamlit run app.py
```

## Included features

- Store comparison
- Interview watch
- New machine watch
- New machine x interview overlap summary

## Notes

- This public package intentionally excludes internal upload scripts and operational files.
- `store_mapping_unified.json` is generated from the private master workbook and included as a runtime mapping artifact.
