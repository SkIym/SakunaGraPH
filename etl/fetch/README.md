**DROMIC**

Stateful, idempotent scraper for DROMIC situation reports.

Usage:
- `python -m fetch.dromic --year [year]`

Arguments:
- `--year` (required): year of situation reports to scrape (listing page `https://dromic.dswd.gov.ph/category/situation-reports/{year}/`)
- `--last-scrape-date` (optional, `YYYY-MM-DD`): override the last scrape date stored in the state file
- `--state-file` (optional): path to the persistent scrape state file (default: `../logs/dromic/scrape_state.json`)
- `--max-pages` (optional, default `100`): maximum number of listing pages to walk

Outputs (per year):
- Downloads → `../data/raw/dromic-new/{year}/`
- Manifest → `../logs/dromic/{year}_manifest.json` — provenance record per file (`filename`, `download_url`, `downloaded_at`, `post_url`, `page`)
- Scrape state → `../logs/dromic/{year}_scrape_state.json` — tracks `last_scrape_date` and `scraped_urls` so re-runs only fetch new posts
- Log → `../logs/dromic/{year}_scraper_{timestamp}.log`

Behavior:
- Walks paginated listing pages, opens each post, and downloads the first attachable file (PDF, DOC/DOCX, or Google Docs link, which is rewritten to a direct `export?format=docx` URL)
- Stops early once it encounters a post older than the last scrape date
- Manifest and state are saved after every successful download, so interrupted runs resume cleanly
- Requires Selenium with a Chrome WebDriver available on PATH


**NDRRMC**
- sit reports can be manually downloaded, but can make crawler connected to parser
- crawler wip
