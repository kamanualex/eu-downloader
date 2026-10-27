# eurusd-downloader

Downloads raw EUR/USD 1-minute BID and ASK candles from Dukascopy's public datafeed, one year at a time, on GitHub Actions.

**No data is stored in this repository.** Each finished year is packed into a reproducible archive and attached to a release in a separate **private** research repository. Dukascopy's data appears to be licensed for personal use, so it is never published here. The workflow refuses to start if the target repository is not private.

- `phase1_dataset/fetch/dukascopy.py`: the downloader (Python standard library only). It retries transient failures, resumes across runs, stops cleanly at a time budget, and hashes every file into a manifest.
- `phase1_dataset/fetch/test_dukascopy.py`: self-tests, run before every download.
- `.github/workflows/download.yml`: the workflow (**Actions → download → Run workflow**).

## One-time setup (owner only)
1. **Token:** Profile picture → **Settings** → **Developer settings** → **Personal access tokens** → **Fine-grained tokens** → **Generate new token**.
   - **Repository access:** Only select repositories → the private research repository.
   - **Permissions → Repository permissions → Contents:** Read and write.
   - **Expiration:** 90 days.
2. **Copy the token**, which is shown only once.
3. **Save it in this repository:** **Settings** → **Secrets and variables** → **Actions** → **New repository secret**. Name: `DATA_REPO_TOKEN`.

## Running a year
**Actions → download → Run workflow**, with years `["2023"]` and workers `3`.

If the run ends with "NOT complete yet", run the same thing again. It resumes where it stopped.
