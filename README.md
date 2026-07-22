# Crisis Scan — weekly signal pull

## What this is
A script that pulls new recall, litigation, and regulatory-disclosure signals
from public sources over the last N days and writes one markdown digest
(`digest.md`) plus a running CSV history (`candidates_log.csv`). It does not
score or draft anything — that's the analyst's 30-45 minutes on Monday.

## Sources wired up
- FDA openFDA enforcement API (food, drug, device recalls)
- CPSC saferproducts.gov recall API
- SEC EDGAR full-text search — flags 8-Ks filed in the window containing
  litigation/crisis language (whistleblower, consent decree, subpoena, etc.)
- RSS: Marler Blog, Top Class Actions, ClassAction.org

## Before you run it
1. `pip install -r requirements.txt`
2. Open `crisis_scan.py` and set `SEC_CONTACT` to a real email — SEC EDGAR's
   fair-use policy requires a descriptive User-Agent with contact info, or
   requests get throttled/blocked.
3. Edit `DEFAULT_WATCHLIST` (currently `Apple, Foxconn`) or pass `--watch` at
   runtime — any hit mentioning a watchlist name gets pulled into a separate
   "Watchlist hits" section at the top of the digest.

## Run it
```bash
python crisis_scan.py --days 7 --out digest.md --watch "Apple,Foxconn"
```

## Important — about testing
I built and syntax-checked this script and confirmed it runs end-to-end
without crashing, but I could not verify live data against the actual FDA /
CPSC / SEC / RSS sources — this sandbox's network is locked to package
registries only (pip, npm, github), so every external call in my test run
returned a blocked-connection error, not a real API response. The script's
error handling is built to survive that (each source is wrapped in its own
try/except, so one dead feed doesn't kill the run) — that's what you saw in
testing. **Run it once on your own machine before relying on it**, and check
that each source's section actually populates with real hits. If a specific
source comes back empty or errors, that's the thing to debug first — most
likely causes are the SEC User-Agent (see above) or one of the RSS feed URLs
having moved.

## Schedule it (don't run by hand every Monday)
Cron example, runs 7am every Monday:
```
0 7 * * 1 cd /path/to/crisis_scan && /usr/bin/python3 crisis_scan.py --days 7 >> cron.log 2>&1
```
Or wire it into GitHub Actions / Zapier / a scheduled task if cron isn't
available in your environment. Either way, the output should land somewhere
a human checks Monday morning — a shared drive folder, an email step, or a
Slack webhook post are all easy additions if useful.

## Extending it
Reasonable next additions, roughly in order of value:
- CDC outbreak pages (no clean public API — would need light scraping)
- State AG press release feeds for states you operate in most
- A second watchlist tier for "friends of firm" clients/law firms, so you
  also catch when *their* names appear (useful for relationship-building,
  not just new business)
- Posting the digest straight to a Slack channel instead of a file
