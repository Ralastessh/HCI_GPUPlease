# spiders/naver_news_spider.py
import re
import scrapy
from urllib.parse import urljoin, urlparse, parse_qs
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from uuid import uuid5, NAMESPACE_URL


def _clean(s: str):
    if not s:
        return None
    s = s.replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", s) or None


def _now_kst_str() -> str:
    # KST: YYYY-MM-DD HH:MM:SS
    return datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")


def _to_yymmdd(text: str | None) -> str | None:
    """
    입력 예: '2025.10.24', '2025-10-24', '2025.10.24 09:10', ...
    출력 예: '25.10.24'
    """
    if not text:
        return None
    m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", text)
    if not m:
        return None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{y % 100:02d}.{mo:02d}.{d:02d}"


class NaverNewsSpider(scrapy.Spider):
    name = "naver_news"
    allowed_domains = ["news.naver.com", "n.news.naver.com", "naver.com"]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 0.6,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://news.naver.com/",
        },
        "FEED_EXPORT_ENCODING": "utf-8",
        "LOG_LEVEL": "INFO",
    }

    def __init__(
        self,
        sections="100,101,102,104",  # 정치, 경제, 사회, 세계(예시)
        max_headlines=100,
        max_latest=100,
        max_pages=10,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sections = [s.strip() for s in sections.split(",") if s.strip()]
        self.max_headlines = int(max_headlines)
        self.max_latest = int(max_latest)
        self.max_pages = int(max_pages)

        self.seen_urls = set()
        self.cnt_headline = {sec: 0 for sec in self.sections}
        self.cnt_latest = {sec: 0 for sec in self.sections}

        # 디버그용: 필요 시 html 덤프
        self.dump_dir = Path("dumps"); self.dump_dir.mkdir(exist_ok=True)

    # ────────────── helpers ──────────────
    def _extract_oid_aid(self, url: str):
        """
        지원:
        - https://n.news.naver.com/mnews/article/{oid}/{aid}
        - https://news.naver.com/article/{oid}/{aid}
        - https://news.naver.com/read.naver?oid={oid}&aid={aid}
        """
        try:
            p = urlparse(url)
            parts = [x for x in p.path.split("/") if x]
            # 경로형 /article/{oid}/{aid}
            if len(parts) >= 3 and parts[-3] == "article":
                oid, aid = parts[-2], parts[-1]
                if (oid or "").isdigit() and (aid or "").isdigit():
                    return oid, aid
            # 쿼리형
            qs = parse_qs(p.query)
            oid = qs.get("oid", [None])[0]
            aid = qs.get("aid", [None])[0]
            return oid, aid
        except Exception:
            return None, None

    def _canonical_link(self, oid: str | None, aid: str | None) -> str | None:
        if oid and aid:
            return f"https://news.naver.com/article/{oid}/{aid}"
        return None

    def _make_uuid(self, oid: str | None, aid: str | None, url: str) -> str:
        base = self._canonical_link(oid, aid) or url
        return str(uuid5(NAMESPACE_URL, base))

    # 섹션 시작
    def start_requests(self):
        for sec in self.sections:
            url = f"https://news.naver.com/section/{sec}"
            yield scrapy.Request(
                url,
                callback=self.parse_section,
                cb_kwargs={"section": sec, "page_idx": 1},
            )

    # 섹션 페이지 파싱
    def parse_section(self, response, section, page_idx):
        # 1) 헤드라인
        headline_links = response.css('ul[id^="_SECTION_HEADLINE_LIST_"] li .sa_text > a::attr(href)').getall()
        for href in headline_links:
            if self.cnt_headline[section] >= self.max_headlines:
                break
            abs_url = urljoin(response.url, href)
            if abs_url in self.seen_urls:
                continue
            self.seen_urls.add(abs_url)
            self.cnt_headline[section] += 1
            yield scrapy.Request(
                abs_url,
                callback=self.parse_article,
                cb_kwargs={"section": section, "list_type": "headline"},
            )

        # 헤드라인 '더보기'
        more_headline = response.css(
            "#newsct > div.section_component.as_section_headline._PERSIST_CONTENT "
            "> div.section_more._SECTION_HEADLINE_MORE_BUTTON_WRAP > a::attr(href)"
        ).get()
        if more_headline and self.cnt_headline[section] < self.max_headlines and page_idx < self.max_pages:
            more_url = urljoin(response.url, more_headline)
            yield scrapy.Request(
                more_url,
                callback=self.parse_section,
                cb_kwargs={"section": section, "page_idx": page_idx + 1},
            )

        # 2) 최신기사
        latest_links = response.css(
            "#newsct > div.section_latest div.section_latest_article._CONTENT_LIST._PERSIST_META "
            ".sa_text > a::attr(href)"
        ).getall()
        for href in latest_links:
            if self.cnt_latest[section] >= self.max_latest:
                break
            abs_url = urljoin(response.url, href)
            if abs_url in self.seen_urls:
                continue
            self.seen_urls.add(abs_url)
            self.cnt_latest[section] += 1
            yield scrapy.Request(
                abs_url,
                callback=self.parse_article,
                cb_kwargs={"section": section, "list_type": "latest"},
            )

        # 최신기사 '더보기'
        more_latest = response.css("#newsct > div.section_latest > div > div.section_more > a::attr(href)").get()
        if more_latest and self.cnt_latest[section] < self.max_latest and page_idx < self.max_pages:
            more_latest_url = urljoin(response.url, more_latest)
            yield scrapy.Request(
                more_latest_url,
                callback=self.parse_section,
                cb_kwargs={"section": section, "page_idx": page_idx + 1},
            )

    # 기사 페이지 파싱
    def parse_article(self, response, section, list_type):
        # 제목
        title = (
            _clean(response.css("#title_area > span::text").get())
            or _clean(response.css("#title_area::text").get())
            or _clean(response.css("h1, h2").xpath("string(.)").get())
        )

        # ── 타임스탬프: 1번째 span=입력(게시), 2번째 span=수정 ──
        ts_nodes = response.css(".media_end_head_info_datestamp_time")
        published_raw = None
        if len(ts_nodes) >= 1:
            published_raw = (
                ts_nodes[0].attrib.get("data-date-time")
                or ts_nodes[0].attrib.get("data-modify-date-time")
                or ts_nodes[0].xpath("string(.)").get()
            )
        article_published_at = _to_yymmdd(_clean(published_raw))  # 'YY.MM.DD'

        # 언론사
        press = _clean(
            response.css(
                "#ct > div.media_end_head.go_trans > "
                "div.media_end_head_top._LAZY_LOADING_WRAP > a > img:nth-child(1)::attr(title)"
            ).get()
        ) or _clean(response.css("div.media_end_head_top a img::attr(title)").get()) \
          or _clean(response.css('meta[property="og:article:author"]::attr(content)').get()) \
          or _clean(response.css('meta[name="twitter:creator"]::attr(content)').get()) \
          or _clean(response.css(".media_end_head_top_logo::text, .media_end_linked_more::text").get()) \
          or _clean(response.css(".media_end_head_top a::text").get())

        # 본문
        body = (
            response.css("#dic_area").xpath("string(.)").get()
            or response.css("#newsct_article").xpath("string(.)").get()
            or response.css("#contents").xpath("string(.)").get()
            or response.css("article").xpath("string(.)").get()
        )
        texts = _clean(body)

        # 링크/ID/UUID
        oid, aid = self._extract_oid_aid(response.url)
        article_id = aid or None
        link = self._canonical_link(oid, aid) or response.url
        uuid_val = self._make_uuid(oid, aid, response.url)

        # 스크랩 시각
        created_at = _now_kst_str()
        latest_scraped_at = created_at

        yield {
            "uuid": uuid_val,
            "article_id": article_id,
            "section": section,
            "list_type": list_type,           # 'headline' | 'latest'
            "press": press,
            "link": link,                     # 정규화된 데스크톱 기사 링크
            "texts": texts,
            "article_published_at": article_published_at,  # 예: '25.10.24'
            "created_at": created_at,                      # 예: '2025-10-27 20:52:49'
            "latest_scraped_at": latest_scraped_at,        # 예: '2025-10-27 20:52:49'
        }


'''
실행 예시:
scrapy crawl naver_news \
  -a sections=100,101,102,104 \
  -a max_headlines=10 \
  -a max_latest=10 \
  -a max_pages=3 \
  -O naver_news_100_101_102_104.csv


'''