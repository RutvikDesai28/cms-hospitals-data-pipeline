# CMS Hospitals Dataset Pipeline

Pulls the datasets tagged "Hospitals" from the CMS provider-data API, downloads them as CSVs, and cleans up the column headers to snake_case. Keeps a metadata.json file so re-running it only downloads what changed.

API used: https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items

## Files

- `pipeline.py` - the script, run this
- `requirements.txt` - just `requests`
- `metadata.example.json` - example of what metadata.json looks like
- `DESIGN.md` - notes on why it's built this way, for anyone curious
- `output/` - where the CSVs end up (not committed)
- `metadata.json` - gets created when you run it (not committed)

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate      # linux/mac: source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python pipeline.py
```

First time this will download all ~75 Hospitals datasets into `output/` and create `metadata.json`. Takes a minute or so.

Flags, all optional:

```
--output-dir      default: output
--metadata-file   default: metadata.json
--max-workers     default: 8
--verbose         more logging
```

## How to check it's working

Run it once, then run it again right away:

```bash
python pipeline.py
python pipeline.py
```

Second run should say "unchanged - skipping" for everything and not download anything - that's the incremental part working.

If you want to force one dataset to re-download, open `metadata.json`, find any dataset entry, change its `modified` value to something else, save, and run again. Only that one dataset should re-download.

Check `output/` afterwards - CSVs should be there, headers in snake_case, named like `<identifier>__<original filename>.csv`.

No automated tests, this was checked by actually running it against the real API.