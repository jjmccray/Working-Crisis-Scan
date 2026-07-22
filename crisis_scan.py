#!/usr/bin/env python3
"""
crisis_scan.py — Weekly crisis-signal scanner for new business intelligence.

Pulls new/updated records from public sources over a lookback window and
writes a single markdown digest an analyst can triage in ~30-45 minutes.

Sources (all free, no API key required):
  - FDA: Recalls, Market Withdrawals & Safety Alerts page (immediate signals)
    + openFDA enforcement API (complete classified weekly record)
  - CPSC: saferproducts.gov Recall API (with descriptive headers)
    + official CPSC downloadable CSV fallback
  - SEC EDGAR full-text search (8-K filings containing litigation/crisis language)
  - RSS: Marler Blog (food safety plaintiff bar), Top Class Actions, ClassAction.org

Design notes:
  - Each source is wrapped in its own try/except so one dead feed/API doesn't
    kill the whole run.
  - This script does NOT score or judge newsworthiness. It surfaces raw
    candidates; a human still does severity/velocity/response-gap/whitespace
    scoring (see the pipeline write-up). It DOES tag a few cheap keyword
    signals (multi-state, repeat-firm, litigation-type) to speed triage.
  - SEC EDGAR requires a descriptive User-Agent with a real contact — set
    SEC_CONTACT below before running, or requests will be blocked/throttled.
  - A running CSV log (candidates_log.csv) is appended to each run so you
    build a searchable history over time, and so you can spot repeat firms.

Usage:
    python crisis_scan.py --days 7 --watch "Apple,Foxconn" --out digest.md

Schedule it (don't run it by hand every Monday):
    # crontab -e
    0 7 * * 1 cd /path/to/crisis_scan && /usr/bin/python3 crisis_scan.py --days 7 >> cron.log 2>&1
"""

import argparse
import csv
import datetime as dt
import io
import json
import os
import re
import sys
import time
import urllib.parse

import requests
import feedparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------
# CONFIG — edit these for your firm
# --------------------------------------------------------------------------

SEC_CONTACT = "jovoni.johnsonmccray@scalestrategy.com"  # REQUIRED by SEC EDGAR fair-use policy — put a real address

# Keywords used to flag 8-Ks that plausibly signal a crisis (Item 1.05/8.01
# type disclosures). SEC full-text search matches on these terms.
SEC_LITIGATION_KEYWORDS = [
    "putative class action",
    "wrongful termination",
    "whistleblower",
    "consent decree",
    "material weakness",
    "internal investigation",
    "subpoena",
    "cybersecurity incident",
    "product recall",
    "enforcement action",
]

# Fix #1 — restrict SEC full-text search to specific 8-K Items instead of
# matching a keyword anywhere in the filing. Item 1.05 is specifically
# "Material Cybersecurity Incidents"; Item 4.02 is specifically
# "Non-Reliance on Previously Issued Financial Statements" (the right home
# for "material weakness" disclosures); everything else that plausibly
# signals a crisis gets filed under the catch-all Item 8.01 "Other Events".
# The search query below ANDs the Item heading together with the keyword,
# so a hit now has to (a) be filed under that item AND (b) mention the
# keyword — not just be a merger agreement that happens to contain the
# phrase "consent decree" in its reps and warranties.
SEC_ITEM_BY_KEYWORD = {
    "cybersecurity incident": "Item 1.05",
    "material weakness": "Item 4.02",
}
SEC_DEFAULT_ITEM = "Item 8.01"

# Fix #2 — exclude filings where the matched document is clearly an exhibit
# (a merger agreement, employment agreement, severance agreement, etc.)
# rather than the primary 8-K body. Exhibit filenames reliably follow a
# small set of patterns — but SEC filenames get truncated (32-char limits
# are common), so "agreement" shows up as "agreem", "severance" as "sever",
# etc. Matching on truncation-safe fragments + a regex for ex-then-digit
# (ex10-1, dex21, ex0201, xex1d1, ex2_1 — with or without a separator)
# catches this reliably; a plain substring list on full words does not.
SEC_EXHIBIT_NUMBER_PATTERN = re.compile(r"ex[-_]?\d")
SEC_NOISE_FILENAME_FRAGMENTS = [
    "exhibit", "agree", "separat", "sever", "employ",
    "underwrit", "releas", "amend",
]


def is_sec_exhibit_filename(filename):
    """True if this looks like an exhibit/agreement doc rather than the
    primary 8-K body — see Fix #2 above."""
    fname_l = (filename or "").lower()
    if SEC_EXHIBIT_NUMBER_PATTERN.search(fname_l):
        return True
    return any(frag in fname_l for frag in SEC_NOISE_FILENAME_FRAGMENTS)


# Optional standing watchlist — companies you always want flagged if they
# appear anywhere in this week's pull, regardless of source (e.g. Apple).
DEFAULT_WATCHLIST = ["Apple", "Foxconn"]

RSS_FEEDS = {
    "Marler Blog (food safety plaintiff bar)": "https://www.marlerblog.com/feed/",
    "Top Class Actions": "https://topclassactions.com/feed/",
    "ClassAction.org": "https://www.classaction.org/news/feed",
}

FDA_ENFORCEMENT_ENDPOINTS = {
    "FDA Food Recall": "https://api.fda.gov/food/enforcement.json",
    "FDA Drug Recall": "https://api.fda.gov/drug/enforcement.json",
    "FDA Device Recall": "https://api.fda.gov/device/enforcement.json",
}

# FDA Recalls, Market Withdrawals & Safety Alerts — immediate crisis signals
FDA_ALERTS_RSS = "https://www.fda.gov/AboutFDA/ContactFDA/StayInformed/RSSFeeds/MedWatch/rss.xml"

CPSC_RECALL_ENDPOINT = "https://www.saferproducts.gov/RestWebServices/Recall"

# CPSC official downloadable CSV of all recalls (fallback)
CPSC_CSV_URL = (
    "https://www.cpsc.gov/s3fs-public/"
    "recall-data/recalls_recall_listing.csv"
)

SEC_FULLTEXT_ENDPOINT = "https://efts.sec.gov/LATEST/search-index?"

REQUEST_TIMEOUT = 30

# --------------------------------------------------------------------------
# Shared HTTP session with retries
# --------------------------------------------------------------------------


def _make_session():
    """Return a requests.Session with retry logic and descriptive headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": f"CrisisScan/1.0 (contact: {SEC_CONTACT})",
        "Accept": "application/json",
    })
    retries = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def log(msg):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", file=sys.stderr)


def date_window(days):
    end = dt.date.today()
    start = end - dt.timedelta(days=days)
    return start, end


def tag_signals(text, watchlist):
    """Cheap keyword tagging to speed human triage. Not a scoring system."""
    text_l = (text or "").lower()
    tags = []
    if any(w in text_l for w in ["multistate", "multi-state", "nationwide"]):
        tags.append("multi-state")
    if any(w in text_l for w in ["class action", "class-action"]):
        tags.append("class-action")
    if any(w in text_l for w in ["whistleblower", "retaliation"]):
        tags.append("whistleblower")
    if any(w in text_l for w in ["death", "fatal", "died"]):
        tags.append("fatality-linked")
    for company in watchlist:
        if company.lower() in text_l:
            tags.append(f"WATCHLIST:{company}")
    return tags


# --------------------------------------------------------------------------
# FDA
# --------------------------------------------------------------------------

def fetch_fda_alerts_rss(start, end, watchlist):
    """
    Pull FDA Recalls, Market Withdrawals & Safety Alerts via RSS.
    This is the *immediate* signal source — it contains recalls dated today,
    before they appear in the openFDA enforcement API (which lags ~1-2 weeks).
    """
    results = []
    try:
        feed = feedparser.parse(FDA_ALERTS_RSS)
        for entry in feed.entries:
            # Parse published date
            published = entry.get("published_parsed") or entry.get("updated_parsed")
            if published:
                pub_date = dt.date(*published[:3])
                if not (start <= pub_date <= end):
                    continue
                date_str = pub_date.isoformat()
            else:
                date_str = ""

            title = entry.get("title", "Untitled")
            summary = entry.get("summary", "") or ""
            link = entry.get("link", "")

            # Try to extract company name from title
            company = "Unknown"
            if " — " in title:
                company = title.split(" — ")[-1].strip()
            elif " - " in title:
                company = title.split(" - ")[-1].strip()

            text_blob = f"{title} {summary}"
            results.append({
                "source": "FDA Alerts (RSS)",
                "date": date_str,
                "headline": title,
                "detail": (summary or "")[:300],
                "url": link,
                "tags": tag_signals(text_blob, watchlist),
            })
    except Exception as e:
        log(f"FDA Alerts RSS fetch failed: {e}")
    return results


def fetch_fda_enforcement_api(start, end, watchlist, session, limit=1000):
    """
    Pull from openFDA enforcement API.
    Instead of querying by date range (which can 500 when data hasn't been
    indexed yet), we pull the latest records sorted by report_date and filter
    locally. This avoids asking openFDA for dates that have not yet been indexed.
    """
    results = []
    params = {
        "sort": "report_date:desc",
        "limit": limit,
    }
    api_key = os.getenv("OPENFDA_API_KEY")
    if api_key:
        params["api_key"] = api_key

    for label, url in FDA_ENFORCEMENT_ENDPOINTS.items():
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            for rec in data.get("results", []):
                report_date_raw = rec.get("report_date", "")
                if not report_date_raw:
                    continue
                try:
                    report_date = dt.datetime.strptime(report_date_raw, "%Y%m%d").date()
                except ValueError:
                    continue
                if not (start <= report_date <= end):
                    continue

                firm = rec.get("recalling_firm", "Unknown firm")
                reason = rec.get("reason_for_recall", "")
                classification = rec.get("classification", "")
                product = rec.get("product_description", "")
                state = rec.get("state", "")
                text_blob = f"{firm} {reason} {product}"
                results.append({
                    "source": label,
                    "date": report_date_raw,
                    "headline": f"{firm} — {classification} recall ({state})",
                    "detail": (reason or "")[:300],
                    "url": "https://www.accessdata.fda.gov/scripts/ires/index.cfm",
                    "tags": tag_signals(text_blob, watchlist),
                })
        except Exception as e:
            log(f"FDA enforcement API fetch failed for {label}: {e}")
    return results


def fetch_fda_recalls(start, end, watchlist):
    """
    FDA pipeline:
    1. FDA Alerts RSS (immediate signals, no lag)
    2. openFDA enforcement API (complete classified record, filter locally)
    """
    all_results = []

    log("Fetching FDA Alerts RSS (immediate signals)...")
    rss_results = fetch_fda_alerts_rss(start, end, watchlist)
    all_results.extend(rss_results)

    session = _make_session()
    log("Fetching openFDA enforcement API (latest records, filtered locally)...")
    api_results = fetch_fda_enforcement_api(start, end, watchlist, session)
    all_results.extend(api_results)

    return all_results


# --------------------------------------------------------------------------
# CPSC
# --------------------------------------------------------------------------

def fetch_cpsc_api(start, end, watchlist, session):
    """
    Try the CPSC saferproducts.gov JSON API first.
    Uses descriptive User-Agent headers and LastPublishDate filters.
    """
    results = []
    try:
        params = {
            "format": "json",
            "LastPublishDateStart": start.strftime("%Y-%m-%d"),
            "LastPublishDateEnd": end.strftime("%Y-%m-%d"),
        }
        resp = session.get(
            CPSC_RECALL_ENDPOINT,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        for rec in data:
            title = rec.get("Title", "Untitled recall")
            desc = rec.get("Description", "") or ""
            date = rec.get("RecallDate", "")
            url = rec.get("URL", "")
            manufacturers = ", ".join(
                m.get("Name", "") for m in rec.get("Manufacturers", [])
            ) if rec.get("Manufacturers") else ""
            text_blob = f"{title} {desc} {manufacturers}"
            results.append({
                "source": "CPSC Recall (API)",
                "date": date,
                "headline": title,
                "detail": (desc or "")[:300],
                "url": url,
                "tags": tag_signals(text_blob, watchlist),
            })
    except Exception as e:
        log(f"CPSC API fetch failed: {e}")
    return results


def fetch_cpsc_csv(start, end, watchlist, session):
    """
    Fallback: download the official CPSC recalls CSV and filter locally.
    CPSC says its downloadable recall data updates weekly.
    """
    results = []
    try:
        resp = session.get(CPSC_CSV_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        # Parse CSV
        content = resp.content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            # Try to parse recall date
            recall_date_raw = row.get("RecallDate", "").strip()
            if not recall_date_raw:
                continue
            try:
                # CPSC CSV dates are typically MM/DD/YYYY
                recall_date = dt.datetime.strptime(recall_date_raw, "%m/%d/%Y").date()
            except ValueError:
                try:
                    recall_date = dt.datetime.strptime(recall_date_raw, "%Y-%m-%d").date()
                except ValueError:
                    continue
            if not (start <= recall_date <= end):
                continue

            title = row.get("Title", "Untitled recall").strip()
            desc = row.get("Description", "").strip()
            url = row.get("URL", "").strip()
            manufacturers = row.get("Manufacturers", "").strip()
            text_blob = f"{title} {desc} {manufacturers}"
            results.append({
                "source": "CPSC Recall (CSV fallback)",
                "date": recall_date_raw,
                "headline": title,
                "detail": (desc or "")[:300],
                "url": url,
                "tags": tag_signals(text_blob, watchlist),
            })
    except Exception as e:
        log(f"CPSC CSV fallback fetch failed: {e}")
    return results


def fetch_cpsc_recalls(start, end, watchlist):
    """
    CPSC pipeline:
    1. Try JSON API with descriptive headers + LastPublishDate filter
    2. Fallback to official downloadable CSV
    """
    session = _make_session()

    log("Fetching CPSC recalls via API...")
    api_results = fetch_cpsc_api(start, end, watchlist, session)
    if api_results:
        log(f"CPSC API returned {len(api_results)} records")
        return api_results

    log("CPSC API returned no results or failed; falling back to CSV...")
    csv_results = fetch_cpsc_csv(start, end, watchlist, session)
    if csv_results:
        log(f"CPSC CSV fallback returned {len(csv_results)} records")
    else:
        log("CPSC CSV fallback also returned no results")
    return csv_results


# --------------------------------------------------------------------------
# SEC EDGAR full-text search
# --------------------------------------------------------------------------

def fetch_sec_8k_litigation(start, end, watchlist):
    results = []
    skipped_exhibit_count = 0
    headers = {"User-Agent": f"CrisisScan/1.0 ({SEC_CONTACT})"}
    for kw in SEC_LITIGATION_KEYWORDS:
        item = SEC_ITEM_BY_KEYWORD.get(kw, SEC_DEFAULT_ITEM)
        try:
            # Fix #1: AND the Item heading together with the keyword so a
            # hit has to actually be disclosed under that item, not just
            # contain the phrase anywhere in an attached exhibit.
            q = urllib.parse.quote(f'"{item}" "{kw}"')
            url = (
                f"{SEC_FULLTEXT_ENDPOINT}q={q}&forms=8-K"
                f"&startdt={start.isoformat()}&enddt={end.isoformat()}"
            )
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            for hit in data.get("hits", {}).get("hits", []):
                src = hit.get("_source", {})
                raw_id = hit.get("_id", "")
                accession, _, filename = raw_id.partition(":")

                # Fix #2: skip exhibits (merger/employment/severance docs
                # etc.) — we only want the primary 8-K body.
                if is_sec_exhibit_filename(filename):
                    skipped_exhibit_count += 1
                    continue

                company = ", ".join(src.get("display_names", [])) or "Unknown filer"
                filed = src.get("file_date", "")
                cik = (src.get("ciks") or [""])[0].lstrip("0")
                accession_nodashes = accession.replace("-", "")
                if cik and accession_nodashes and filename:
                    filing_url = (
                        f"https://www.sec.gov/Archives/edgar/data/"
                        f"{cik}/{accession_nodashes}/{filename}"
                    )
                elif cik:
                    filing_url = (
                        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}"
                    )
                else:
                    filing_url = "https://www.sec.gov/cgi-bin/browse-edgar"

                text_blob = f"{company} {kw}"
                results.append({
                    "source": f"SEC 8-K ({item}: {kw})",
                    "date": filed,
                    "headline": f"{company} — {item} 8-K referencing '{kw}'",
                    "detail": accession,
                    "url": filing_url,
                    "tags": tag_signals(text_blob, watchlist),
                })
            time.sleep(0.3)  # be polite to EDGAR's rate limits
        except Exception as e:
            log(f"SEC fetch failed for keyword '{kw}': {e}")
    if skipped_exhibit_count:
        log(f"SEC: skipped {skipped_exhibit_count} exhibit-only hits (Fix #2 filter)")
    return results


# --------------------------------------------------------------------------
# RSS (plaintiff bar / class action trackers)
# --------------------------------------------------------------------------

def fetch_rss(start, end, watchlist):
    results = []
    for label, url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                published = entry.get("published_parsed") or entry.get("updated_parsed")
                if published:
                    pub_date = dt.date(*published[:3])
                    if not (start <= pub_date <= end):
                        continue
                    date_str = pub_date.isoformat()
                else:
                    date_str = ""
                title = entry.get("title", "Untitled")
                summary = entry.get("summary", "") or ""
                text_blob = f"{title} {summary}"
                results.append({
                    "source": label,
                    "date": date_str,
                    "headline": title,
                    "detail": summary[:300],
                    "url": entry.get("link", ""),
                    "tags": tag_signals(text_blob, watchlist),
                })
        except Exception as e:
            log(f"RSS fetch failed for {label}: {e}")
    return results


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def write_markdown(all_results, start, end, out_path):
    with open(out_path, "w") as f:
        f.write(f"# Crisis Scan Digest — {start.isoformat()} to {end.isoformat()}\n\n")
        f.write(f"*Generated {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}*\n\n")

        watchlist_hits = [r for r in all_results if any(t.startswith("WATCHLIST:") for t in r["tags"])]
        if watchlist_hits:
            f.write("## ⚠️ Watchlist hits\n\n")
            for r in watchlist_hits:
                f.write(f"- **[{r['source']}]** {r['headline']} ({r['date']}) — {r['url']}\n")
            f.write("\n")

        by_source = {}
        for r in all_results:
            by_source.setdefault(r["source"], []).append(r)

        f.write(f"## All candidates ({len(all_results)} total)\n\n")
        for source, items in sorted(by_source.items()):
            f.write(f"### {source} ({len(items)})\n\n")
            for r in sorted(items, key=lambda x: x["date"], reverse=True):
                tag_str = f" `{', '.join(r['tags'])}`" if r["tags"] else ""
                f.write(f"- **{r['headline']}**{tag_str}\n")
                f.write(f"  - {r['date']} — {r['url']}\n")
                if r["detail"]:
                    f.write(f"  - {r['detail']}\n")
            f.write("\n")

    log(f"Digest written to {out_path}")


def append_csv_log(all_results, csv_path):
    file_exists = os.path.isfile(csv_path)
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["pulled_at", "source", "date", "headline", "url", "tags"])
        if not file_exists:
            writer.writeheader()
        pulled_at = dt.datetime.now().isoformat()
        for r in all_results:
            writer.writerow({
                "pulled_at": pulled_at,
                "source": r["source"],
                "date": r["date"],
                "headline": r["headline"],
                "url": r["url"],
                "tags": "; ".join(r["tags"]),
            })
    log(f"Appended {len(all_results)} rows to {csv_path}")


# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Weekly crisis-signal scanner")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days")
    parser.add_argument("--out", default="digest.md", help="Output markdown path")
    parser.add_argument("--csv", default="candidates_log.csv", help="Running CSV log path")
    parser.add_argument("--watch", default=",".join(DEFAULT_WATCHLIST),
                         help="Comma-separated standing watchlist, e.g. 'Apple,Foxconn'")
    parser.add_argument("--skip-sec", action="store_true", help="Skip SEC EDGAR (slower, rate-limited)")
    args = parser.parse_args()

    watchlist = [w.strip() for w in args.watch.split(",") if w.strip()]
    start, end = date_window(args.days)
    log(f"Scanning {start} to {end} | watchlist: {watchlist}")

    all_results = []
    log("Fetching FDA recalls (RSS alerts + openFDA API)...")
    all_results += fetch_fda_recalls(start, end, watchlist)
    log("Fetching CPSC recalls (API + CSV fallback)...")
    all_results += fetch_cpsc_recalls(start, end, watchlist)
    if not args.skip_sec:
        log("Fetching SEC EDGAR full-text search (this is the slow one)...")
        all_results += fetch_sec_8k_litigation(start, end, watchlist)
    log("Fetching plaintiff-bar / class-action RSS feeds...")
    all_results += fetch_rss(start, end, watchlist)

    log(f"Total candidates pulled: {len(all_results)}")
    write_markdown(all_results, start, end, args.out)
    append_csv_log(all_results, args.csv)


if __name__ == "__main__":
    main()
