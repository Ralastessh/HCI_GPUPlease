# spiders/market_sum_codes_kosdaq.py
import re
from urllib.parse import urlparse, parse_qs, urljoin

import scrapy

BASE = "https://finance.naver.com/sise/sise_market_sum.naver"
CODE_RE = re.compile(r"^\d{6}$")


class MarketSumCodesKOSDAQSpider(scrapy.Spider):
    """
    네이버 금융 '시가총액' 목록에서 코스닥(sosok=1) 종목코드만 수집.
    결과는 텍스트 파일(기본: codes_kosdaq.txt)에 6자리 코드로 줄바꿈 저장.
    """
    name = "market_sum_codes_kosdaq"

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://finance.naver.com/",
        },
        "DOWNLOAD_DELAY": 0.2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "FEED_EXPORT_ENCODING": "utf-8",
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, out: str | None = None, *args, **kwargs):
        """
        -a out=파일명 으로 출력 경로 지정 가능 (기본: codes_kosdaq.txt)
        """
        super().__init__(*args, **kwargs)
        self.out = out or "codes_kosdaq.txt"
        self._codes = set()

    def start_requests(self):
        # 코스닥(sosok=1)만 시작
        url = f"{BASE}?sosok=1&page=1"
        yield scrapy.Request(
            url,
            callback=self.parse_list,
            cb_kwargs={"page": 1},
            dont_filter=True,
        )

    def parse_list(self, response, page: int):
        # 2번째 컬럼의 <a>에서 code 파라미터 추출
        for a in response.css("#contentarea > div.box_type_l > table.type_2 > tbody > tr > td:nth-child(2) > a"):
            href = a.attrib.get("href", "")
            if not href:
                continue
            qs = parse_qs(urlparse(urljoin(response.url, href)).query)
            code = (qs.get("code") or [None])[0]
            if code and CODE_RE.fullmatch(code):
                self._codes.add(code)

        # 마지막 페이지 계산 (pgRR '끝으로' 우선, 없으면 숫자 링크 최대값)
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

        # 다음 페이지가 있으면 계속 크롤링
        if page < last_page:
            next_url = f"{BASE}?sosok=1&page={page+1}"
            yield scrapy.Request(
                next_url,
                callback=self.parse_list,
                cb_kwargs={"page": page + 1},
                dont_filter=True,
            )

    def closed(self, reason):
        # 중복 제거된 6자리 코드들을 정렬해 txt로 저장
        codes_sorted = sorted(self._codes)
        with open(self.out, "w", encoding="utf-8") as f:
            f.write("\n".join(codes_sorted))
        self.logger.info("Saved %d KOSDAQ codes to %s", len(codes_sorted), self.out)
