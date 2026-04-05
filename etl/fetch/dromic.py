# scraper.py — DROMIC situation report scraper (stateful + idempotent)

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

import requests
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

log = logging.getLogger(__name__)

# =============================================================================
# CLI
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DROMIC situation report scraper")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--last-scrape-date",
        type=str,
        default=None,
        help="Override last scrape date (YYYY-MM-DD). Defaults to value in state file.",
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default="../logs/dromic/scrape_state.json",
        help="Path to the persistent scrape state file.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum number of listing pages to scrape.",
    )
    return parser.parse_args()


# =============================================================================
# STATE
# =============================================================================

@dataclass
class ScrapeState:
    last_scrape_date: Optional[str] = None
    scraped_urls: list[str] = field(default_factory=lambda: [])


def load_state(path: Path) -> ScrapeState:
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return ScrapeState(
            last_scrape_date=data.get("last_scrape_date"),
            scraped_urls=data.get("scraped_urls", []),
        )
    return ScrapeState()


def save_state(path: Path, state: ScrapeState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(asdict(state), f, indent=2)


# =============================================================================
# MANIFEST
# =============================================================================

@dataclass
class ManifestEntry:
    filename: str
    download_url: str
    downloaded_at: str  # ISO-8601


def load_manifest(path: Path) -> list[ManifestEntry]:
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return [ManifestEntry(**entry) for entry in json.load(f)]
    return []


def save_manifest(path: Path, entries: list[ManifestEntry]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump([asdict(e) for e in entries], f, indent=2, ensure_ascii=False)


# =============================================================================
# HELPERS
# =============================================================================

def get_post_date(driver: WebDriver) -> datetime:
    """Return the publication datetime of the currently loaded post."""
    date_el = driver.find_element(By.CSS_SELECTOR, "span.published.updated")
    return datetime.strptime(date_el.text.strip(), "%B %d, %Y")


def make_direct_download_link(url: str) -> str:
    """Rewrite viewer/embed URLs to direct download links where possible."""
    if "/document/d/" in url:
        file_id = url.split("/document/d/")[1].split("/")[0]
        return f"https://docs.google.com/document/d/{file_id}/export?format=docx"

    if "docs.google.com/viewer" in url:
        qs = parse_qs(urlparse(url).query)
        if "url" in qs:
            actual_url = unquote(qs["url"][0])
            log.info("Extracted direct file URL: %s", actual_url)
            return actual_url

    return url


def sanitize_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]+', "", name).strip()


def resolve_filename(
    response: requests.Response, url: str, fallback: Optional[str]
) -> str:
    """Derive a clean filename from response headers, URL, or fallback hint."""
    content_disp = response.headers.get("content-disposition", "")
    if content_disp:
        match = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", content_disp)
        if not match:
            match = re.search(r'filename="?([^";]+)"?', content_disp)
        if match:
            return sanitize_filename(unquote(match.group(1)))

    name = os.path.basename(url.split("?")[0])
    if name and name.lower() not in ("", "download", "viewer"):
        return sanitize_filename(name)

    name = fallback or f"downloaded_{int(time.time())}"

    # Append an extension if missing
    if not os.path.splitext(name)[1]:
        ctype = response.headers.get("content-type", "")
        if "pdf" in ctype:
            name += ".pdf"
        elif "word" in ctype or ".doc" in url:
            name += ".docx"
        else:
            name += ".bin"

    return sanitize_filename(name)


def download_file(
    url: str,
    download_dir: Path,
    manifest: list[ManifestEntry],
    filename_hint: Optional[str] = None,
) -> bool:
    """Download a file, record it in the manifest, and return success."""
    try:
        r = requests.get(url, timeout=30, allow_redirects=True)
        if r.status_code != 200:
            log.warning("Skipped (HTTP %d): %s", r.status_code, url)
            return False

        filename = resolve_filename(r, url, filename_hint)
        dest = download_dir / filename

        log.info("Downloading → %s", filename)
        dest.write_bytes(r.content)
        log.info("Saved: %s", filename)

        manifest.append(
            ManifestEntry(
                filename=filename,
                download_url=url,
                downloaded_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
            )
        )
        return True

    except Exception:
        log.exception("Download error: %s", url)
        return False


def extract_first_download_link(driver: WebDriver) -> tuple[Optional[str], Optional[str]]:
    """Return (download_url, link_text) for the first attachable file in the post."""
    selectors = [
        "div.post-content a[href*='.pdf']",
        "div.post-content a[href*='.docx']",
        "div.post-content a[href*='.doc']",
        "div.post-content a[href*='docs.google.com']",
        ".wp-block-file a[href]",
        "p.embed_download a[href]",
    ]

    title = driver.find_element(By.CSS_SELECTOR, "h1.post-title")
    log.info("Post title: %s", title.text.strip())

    for sel in selectors:
        elems = driver.find_elements(By.CSS_SELECTOR, sel)
        if elems:
            href = elems[0].get_attribute("href")
            text = elems[0].text.strip() or "downloaded_file"
            if href:
                return make_direct_download_link(href), text

    return None, None


# =============================================================================
# PAGE HANDLING
# =============================================================================

def handle_page(
    driver: WebDriver,
    wait: WebDriverWait,
    scraped_urls: set[str],
    state: ScrapeState,
    state_path: Path,
    manifest: list[ManifestEntry],
    manifest_path: Path,
    last_scrape_date: datetime,
    download_dir: Path,
) -> bool:
    """
    Process all posts on the current listing page.

    Returns True if the scraper should stop (reached already-seen posts).
    """
    read_mores = driver.find_elements(
        By.XPATH, "//a[contains(.,'Read More')] | //button[contains(.,'Read More')]"
    )
    log.info("Found %d posts on page", len(read_mores))

    for i in range(len(read_mores)):
        # Re-query after each navigation to avoid stale element refs
        read_mores = driver.find_elements(
            By.XPATH, "//a[contains(.,'Read More')] | //button[contains(.,'Read More')]"
        )
        if i >= len(read_mores):
            break

        btn = read_mores[i]
        post_url = btn.get_attribute("href")

        if post_url in scraped_urls:
            log.info("Skipping already scraped: %s", post_url)
            continue

        driver.execute_script("arguments[0].scrollIntoView(true);", btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", btn)

        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.post-content")))

            post_date = get_post_date(driver)
            log.info("Post date: %s", post_date.date())

            if post_date <= last_scrape_date:
                log.info("Post is not newer than last scrape date — stopping")
                return True

            file_url, file_name = extract_first_download_link(driver)

            if file_url:
                success = download_file(file_url, download_dir, manifest, file_name)
                if success:
                    scraped_urls.add(post_url)
                    state.scraped_urls = list(scraped_urls)

                    current_max = (
                        datetime.strptime(state.last_scrape_date, "%Y-%m-%d")
                        if state.last_scrape_date
                        else datetime.min
                    )
                    if post_date > current_max:
                        state.last_scrape_date = post_date.strftime("%Y-%m-%d")

                    save_state(state_path, state)
                    save_manifest(manifest_path, manifest)
            else:
                log.warning("No downloadable link found for: %s", post_url)

        except Exception:
            log.exception("Error processing post: %s", post_url)

        driver.back()
        wait.until(
            EC.presence_of_all_elements_located(
                (By.XPATH, "//a[contains(.,'Read More')]")
            )
        )
        time.sleep(1)

    return False


def goto_page(driver: WebDriver, wait: WebDriverWait, page_num: int) -> bool:
    """Click the pagination link for page_num. Returns False if not found."""
    try:
        el = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                f"//ul[contains(@class,'pagination')]//li//*[normalize-space()='{page_num}']",
            ))
        )
        driver.execute_script("arguments[0].scrollIntoView();", el)
        el.click()
        wait.until(
            EC.presence_of_all_elements_located(
                (By.XPATH, "//a[contains(.,'Read More')]")
            )
        )
        return True
    except Exception:
        log.debug("Pagination link for page %d not found", page_num)
        return False


# =============================================================================
# MAIN
# =============================================================================

def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main() -> None:
    args = parse_args()

    download_dir = Path(f"../data/raw/dromic/{args.year}")
    log_dir = Path("../logs/dromic")
    state_path = Path(args.state_file)
    manifest_path = log_dir / f"manifest/{args.year}_manifest.json"
    log_file = log_dir / f"{args.year}_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    download_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(log_file)

    state = load_state(state_path)
    manifest = load_manifest(manifest_path)

    if args.last_scrape_date:
        last_scrape_date = datetime.strptime(args.last_scrape_date, "%Y-%m-%d")
    elif state.last_scrape_date:
        last_scrape_date = datetime.strptime(state.last_scrape_date, "%Y-%m-%d")
    else:
        last_scrape_date = datetime.min

    scraped_urls: set[str] = set(state.scraped_urls)

    opts = Options()

    opts.add_experimental_option( # type: ignore
        "prefs",
        {
            "download.default_directory": str(download_dir.resolve()),
            "download.prompt_for_download": False,
            "safebrowsing.enabled": True,
        },
    )

    driver = WebDriver(options=opts)
    wait = WebDriverWait(driver, 10)

    base_url = f"https://dromic.dswd.gov.ph/category/situation-reports/{args.year}/"
    driver.get(base_url)

    page = 1
    try:
        while page <= args.max_pages:
            log.info("Processing page %d", page)

            should_stop = handle_page(
                driver=driver,
                wait=wait,
                scraped_urls=scraped_urls,
                state=state,
                state_path=state_path,
                manifest=manifest,
                manifest_path=manifest_path,
                last_scrape_date=last_scrape_date,
                download_dir=download_dir,
            )

            if should_stop:
                log.info("Reached last scrape date — stopping")
                break

            page += 1

            if not goto_page(driver, wait, page):
                log.info("No more pages")
                break
    finally:
        driver.quit()
        log.info("Done. Manifest written to: %s", manifest_path)


if __name__ == "__main__":
    main()