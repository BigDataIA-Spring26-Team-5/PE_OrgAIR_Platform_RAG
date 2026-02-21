# Digital Presence Signal — Results Summary

## Scores by Company

| Ticker | Company | Score | Live Techs | Wappalyzer Techs | Notes |
|--------|---------|-------|------------|------------------|-------|
| ADP | Automatic Data Processing | 81.5 | 4,058 | jQuery, GTM | Largest tech footprint — business services |
| WMT | Walmart Inc. | 78.1 | 2,958 | Cart, Envoy | Massive e-commerce infrastructure |
| PAYX | Paychex Inc. | 77.2 | 1,666 | Cloudflare, Drupal, Nginx, MariaDB, PHP | Rich stack, most Wappalyzer detections |
| TGT | Target Corporation | 74.6 | 1,131 | Cart Functionality | E-commerce but smaller than Walmart |
| JPM | JPMorgan Chase | 66.8 | 453 | Adobe AEM, Akamai, Java | Enterprise CMS, moderate web presence |
| UNH | UnitedHealth Group | 65.2 | 392 | Adobe AEM, Java, OneTrust | Healthcare — similar profile to JPM |
| HCA | HCA Healthcare | 64.4 | 238 | React, Akamai | Healthcare — lighter digital presence |
| CAT | Caterpillar Inc. | 62.4 | 212 | *(timed out)* | Manufacturing — minimal web focus |
| GS | Goldman Sachs | 61.1 | 208 | Akamai, CloudFront, AWS | Financial — lean web presence |
| DE | Deere & Company | 60.0 | 132 | Apache, jQuery, OneTrust | Manufacturing — smallest digital footprint |

## Sector Analysis

| Sector | Companies | Avg Score | Interpretation |
|--------|-----------|-----------|----------------|
| Services | ADP, PAYX | 79.4 | Business depends on web — highest digital maturity |
| Retail | WMT, TGT | 76.4 | E-commerce infrastructure drives high scores |
| Financial | JPM, GS | 64.0 | Enterprise-grade but lean public websites |
| Healthcare | UNH, HCA | 64.8 | Compliance-heavy, moderate web investment |
| Manufacturing | CAT, DE | 61.2 | Physical-product companies, simpler web needs |

## Data Sources

- **BuiltWith Free API** — technology group counts, live/dead tech counts per domain
- **Wappalyzer (python-Wappalyzer)** — specific technology name detection via HTTP scan
- **Storage** — Raw data in S3 (`signals/digital/{TICKER}/`), scores in Snowflake

## Score Components

| Component | Max | What It Measures |
|-----------|-----|-----------------|
| Sophistication | 40 | log2(live tech count) + AI tool bonus from Wappalyzer |
| Infrastructure | 30 | Group diversity + key infra groups (CDN, SSL, analytics, etc.) |
| Breadth | 30 | Category diversity + active maintenance ratio (live/total) |

## CS3 Mapping

| CS2 Signal | CS3 Dimension | Weight |
|------------|---------------|--------|
| digital_presence | Data Infrastructure | 0.60 |
| digital_presence | Technology Stack | 0.40 |