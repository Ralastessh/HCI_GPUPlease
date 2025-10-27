# HCI_GPUPlease
## Description
Course Project for HCI (Fall 2025)

---

## Project Structure
```
finance_crawling/
├── finance_test/                # Test scripts or validation runs
│   ├── spiders/
│   │   ├── __init__.py
│   │   └── finance_spider.py    # Core spider logic for crawling financial reports
│   ├── __init__.py
│   ├── items.py                 # Data model definitions
│   ├── middlewares.py           
│   ├── pipelines.py             
│   └── settings.py              # Scrapy settings configuration
├── report_period.csv            # Output file (scraped report data)
├── requirements.txt             
└──  scrapy.cfg                   # Scrapy project configuration
```

---

## How to Run
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the spider
scrapy crawl report -o report_period.csv
```

---

## 🧾 Example Output
| report_name | category | stock_name | title | firm_name |
|--------|-----------|------------|------|-----|
| 산업분석  | 석유화학 | - | 기름뿜뿜 Weekly... | https://... |
| 종목분석  | - | 농심 | 반등의 서막 | https://... |

---

## 🧰 Environment
- **Python** 3.9
- **Scrapy** 2.13.3
- ...
