# Crisis Scan — automated weekly signal pull + dashboard

## What this is
- `crisis_scan.py` — pulls FDA, CPSC, SEC EDGAR, and plaintiff-bar/
  class-action signals from the last N days into `digest.md` (human-
  readable), `digest.json` (structured, for the dashboard), and a running
  `candidates_log.csv` (searchable history across all runs). It does not
  score or draft anything — that's still the analyst's 30-45 minutes.
- `.github/workflows/crisis-scan.yml` — runs the script every Monday (and
  on-demand via a button on the dashboard or the Actions tab), commits the
  results back to the repo, and publishes the dashboard via GitHub Pages.
- `docs/index.html` — "Signal Desk", a single-page dashboard that reads
  `docs/data/digest.json` and shows this week's candidates, with Apple/
  watchlist hits pinned at the top, source filters, text search, and a
  history picker for past weeks.

## Sources wired up
- FDA openFDA enforcement API (food, drug, device recalls) — pulls the
  latest records and filters locally, since the dataset updates weekly and
  can lag a few days behind the live date
- CPSC saferproducts.gov recall API, with a bulk-CSV fallback if the API
  errors out
- SEC EDGAR full-text search — flags 8-Ks filed under specific Items
  (1.05 cybersecurity, 4.02 restatement, 8.01 other events) combined with
  a crisis keyword, and skips hits that are just merger/employment exhibits
- RSS: Marler Blog, Top Class Actions, ClassAction.org

## One-time setup

**1. Local run (you've already done this)**
```bash
pip install -r requirements.txt
python crisis_scan.py --days 7 --out digest.md --json digest.json --watch "Apple,Foxconn"
```
Before running: open `crisis_scan.py` and set `SEC_CONTACT` to a real
email — SEC EDGAR's fair-use policy requires a descriptive User-Agent, or
requests get throttled/blocked.

**2. Push this folder to a GitHub repo**
```bash
git init
git add .
git commit -m "Initial crisis scan setup"
git branch -M main
git remote add origin https://github.com/YOUR-USERNAME/YOUR-REPO.git
git push -u origin main
```

**3. Let the workflow push commits**
Settings → Actions → General → Workflow permissions → **"Read and write
permissions"** → Save. Without this, the scan runs fine but the step that
commits `docs/data/*` back to the repo fails with a 403.

**4. Turn on GitHub Pages**
Settings → Pages → Build and deployment → Source → **"GitHub Actions"**
(not "Deploy from a branch"). After the first successful run, the
dashboard is live at `https://YOUR-USERNAME.github.io/YOUR-REPO/`.

**5. Point the dashboard at your repo**
In `docs/index.html`, edit near the top of the `<script>` tag:
```js
const GITHUB_REPO = "YOUR-USERNAME/YOUR-REPO";
```
This only builds the "Run scan" button's link — not a credential.

**6. (Optional) Add an openFDA API key**
Settings → Secrets and variables → Actions → New repository secret →
`OPENFDA_API_KEY`. Raises your daily rate limit; works fine without it.
Free key: https://open.fda.gov/apis/authentication/

**7. Run it once manually to confirm everything's wired up**
Actions tab → "Weekly Crisis Scan" → Run workflow → Run workflow. Watch it
go green, then check the Pages URL from step 4 — you should see real data,
not the amber "showing sample data" banner.

## How "Run scan" works on the dashboard
It opens your repo's Actions page for this workflow, where you click
GitHub's own "Run workflow" button. A public webpage can't trigger a
workflow run directly without embedding a token in its JavaScript — which
anyone could view and use against your repo — so this is two clicks
instead of one, deliberately, to avoid putting repo access in client-side
code. A true one-click version is possible with a small serverless
function holding the token server-side; worth doing if this gets used
heavily, not worth the added infrastructure for a weekly job.

## Email (optional)
`.github/workflows/crisis-scan.yml` has a commented-out `email` job at the
bottom. To enable: uncomment it, add repo secrets `EMAIL_SERVER_ADDRESS`,
`EMAIL_SERVER_PORT`, `EMAIL_USERNAME`, `EMAIL_PASSWORD` (an app password,
not your real one), `EMAIL_TO`, then commit. The next scheduled run will
also email `digest.md` to your list.

## Important — what I could and couldn't verify
I compiled and ran the full script end-to-end, unit-tested the SEC
exhibit filter against real filenames from a prior run, and syntax-checked
the dashboard's HTML/JS. What I could **not** do from this sandbox:
- Hit the live FDA/CPSC/SEC/RSS APIs — this sandbox's network only reaches
  package registries, not those domains, so every external call in my
  tests returned a blocked-connection error rather than a real response.
  You've already confirmed the script itself runs on your machine, which
  is the part I couldn't verify directly.
- Actually run the GitHub Action or confirm the Pages deploy — no access
  to your repo or secrets from here.

Run the manual workflow trigger (step 7) before trusting the schedule, and
open the Pages URL afterward to confirm the dashboard is reading real data.

## Repo layout
```
crisis_scan/
├── crisis_scan.py
├── requirements.txt
├── README.md
├── .github/workflows/crisis-scan.yml
└── docs/
    ├── index.html              (the dashboard)
    └── data/                   (written by the workflow — don't hand-edit)
        ├── digest.md
        ├── digest.json
        ├── candidates_log.csv
        └── history/
            ├── index.json
            └── YYYY-MM-DD.json (one snapshot per run)
```

## Extending it
Roughly in order of value:
- CDC outbreak pages (no clean public API — would need light scraping)
- State AG press release feeds for the states you operate in most
- A second watchlist tier for friends-of-firm clients/law firms, so you
  also catch when *their* names appear — useful for relationship-building,
  not just new business
- The serverless-function upgrade mentioned above, for true one-click runs
