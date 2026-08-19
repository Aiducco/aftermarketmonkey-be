# Turn 14 requires every request (catalog sync and order API alike) to carry a User-Agent that
# identifies AftermarketScout, so their team can attribute traffic during troubleshooting -- a
# contractual requirement from their API onboarding, not an anti-bot workaround (contrast with
# the browser-spoofing UA used for scraped feeds like rough_country/client.py).
USER_AGENT = "AfterMarketScout/1.0 (+https://aftermarketscout.com)"
