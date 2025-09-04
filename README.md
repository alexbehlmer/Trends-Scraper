Automated pipeline to pull Google Trends data for a list of keywords (companies/brands) and store results in a Google Sheet every day.

🚀 How it Works

Keywords

Stored in the Google Sheet under the kpis tab (column A, starting at row 2).

Example: Nike, Adidas, Estée Lauder, Amazon…

GitHub Actions

A scheduled workflow (.github/workflows/daily.yml) runs once per day.

It executes the script trends_to_sheets.py.

Script (trends_to_sheets.py)

Uses pytrends
 to query Google Trends.

Pulls daily interest values (interest_over_time) for each keyword.

Retries automatically with backoff if Google rate-limits (HTTP 429).

Writes results into the trends_daily tab in the Google Sheet:

date | keyword | geo | timeframe | interest_value
2025-09-04 | Nike   | US | today 3-m | 72


Google Sheet Structure

kpis → master list of keywords

trends_daily → updated daily by the workflow

Other tabs (e.g. signals_daily, dashboard_ready) prepare the data for dashboards or future integrations (social media, sentiment, etc.).

⚙️ Setup

Google Cloud

Enable Google Sheets API + Google Drive API.

Create a Service Account with Editor role.

Download JSON key and save it as a GitHub Secret (GOOGLE_SERVICE_ACCOUNT_JSON).

Share your Google Sheet with the service account email.

GitHub

Add these repo files:

requirements.txt

trends_to_sheets.py

.github/workflows/daily.yml

The workflow installs dependencies, runs the script, and pushes data to your sheet.

Schedule

By default, runs daily at 12:20 UTC (~8:20 AM ET).

You can edit the cron line in daily.yml to change the run time.
