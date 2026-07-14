# Fetch Stage

This package contains the web-scraping stage for DROMIC situation reports.
It downloads the first attachable file from each report post and records
download metadata so repeated runs can resume without re-fetching the same posts.

## DROMIC Scraper

`etl/fetch/dromic.py` is a stateful, idempotent scraper for the DSWD DROMIC
situation-report archive.

### Requirements

- Python dependencies used by the scraper, including `requests` and Selenium
- Google Chrome or Chromium
- A matching ChromeDriver available on `PATH`

### Usage

```bash
python -m fetch.dromic --year 2024
```

### Arguments

- `--year` required. Year of the situation-report archive to scrape.
- `--page` optional. Start from a specific listing page instead of page 1.
- `--last-scrape-date` optional, `YYYY-MM-DD`. Overrides the stored cutoff date.
- `--max-pages` optional, default `100`. Maximum number of listing pages to visit.

### Inputs

The scraper visits the DROMIC listing page for the selected year:

`https://dromic.dswd.gov.ph/category/situation-reports/{year}/`

It then opens each report post and looks for the first downloadable attachment:

- PDF
- DOC / DOCX
- Google Docs links, which are rewritten to a direct export URL when possible

### Outputs

For each year, the scraper writes to:

- `../data/raw/dromic-new/{year}/` for downloaded files
- `../data/raw/dromic-new/{year}/manifest.json` for per-file provenance metadata and resume state
- `../logs/dromic/{year}_scraper_{timestamp}.log` for runtime logs

The manifest stores the latest scrape timestamp plus one entry per successful
download:

- `last_scrape_date`
- `filename`
- `download_url`
- `downloaded_at`
- `post_url`
- `page`

### Behavior

- Walks paginated archive pages and opens each post via its `Read More` link
- Stops early when it encounters a post older than the last stored scrape date
- Skips posts already listed in the manifest
- Saves the manifest after each successful download, so interrupted runs can resume cleanly
- Returns to the listing page after each post before continuing

### Notes

- The script currently downloads only the first attachable file it finds in a post.
- Output paths are relative to the working directory used to launch the command.
- If ChromeDriver is not installed or not visible on `PATH`, Selenium startup will fail.
