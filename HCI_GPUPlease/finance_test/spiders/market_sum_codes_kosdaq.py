import re
from urllib.parse import urlparse, parse_qs, urljoin
import scrapy

BASE = "https://finance.naver.com/sise/sise_market_sum.naver"
CODE_RE = re.compile(r"^\d{6}$")

class MarketSumCodesKOSDAQSpider(scrapy.Spider):
    name = "market_sum_codes_kosdaq"
    allowed_domains = ["finance.naver.com", "naver.com"]

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://finance.naver.com/",
        },
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 0.3,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "FEED_EXPORT_ENCODING": "utf-8",
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, out: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.out = out or "codes_kosdaq.txt"
        self._codes = set()

    def start_requests(self):
        url = f"{BASE}?sosok=1&page=1"
        yield scrapy.Request(
            url, callback=self.parse_list, cb_kwargs={"page": 1}, dont_filter=True
        )

    def _grab_codes_on_page(self, response):
        count_before = len(self._codes)
        # 1) 전통적인 a.tltle (네이버가 자주 쓰는 클래스)
        for href in response.css("table.type_2 a.tltle::attr(href)").getall():
            qs = parse_qs(urlparse(urljoin(response.url, href)).query)
            code = (qs.get("code") or [None])[0]
            if code and CODE_RE.fullmatch(code):
                self._codes.add(code)

        # 2) 폴백: 2번째 컬럼의 <a>
        for a in response.css("#contentarea div.box_type_l table.type_2 tbody tr td:nth-child(2) > a"):
            href = a.attrib.get("href", "")
            if not href:
                continue
            qs = parse_qs(urlparse(urljoin(response.url, href)).query)
            code = (qs.get("code") or [None])[0]
            if code and CODE_RE.fullmatch(code):
                self._codes.add(code)

        self.logger.debug("codes on this page: +%d (total %d)", len(self._codes) - count_before, len(self._codes))

    def parse_list(self, response, page: int):
        # 코드 수집
        self._grab_codes_on_page(response)

        # 마지막 페이지 계산
        last_page = 1
        rr = response.css("a.pgRR::attr(href)").get()
        if rr:
            last_page = int(parse_qs(urlparse(urljoin(response.url, rr)).query).get("page", ["1"])[0])
        else:
            nums = []
            for href in response.css("a::attr(href)").getall():
                if "page=" in href:
                    try:
                        p = int(parse_qs(urlparse(urljoin(response.url, href)).query).get("page", ["1"])[0])
                        nums.append(p)
                    except Exception:
                        pass
            if nums:
                last_page = max(nums)

        # 다음 페이지로
        if page < last_page:
            next_url = f"{BASE}?sosok=1&page={page+1}"
            yield scrapy.Request(
                next_url, callback=self.parse_list, cb_kwargs={"page": page + 1}, dont_filter=True
            )

    def closed(self, reason):
        codes_sorted = sorted(self._codes)
        with open(self.out, "w", encoding="utf-8") as f:
            f.write("\n".join(codes_sorted))
        self.logger.info("Saved %d KOSDAQ codes to %s", len(codes_sorted), self.out)

