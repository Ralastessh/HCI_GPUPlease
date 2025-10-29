# spiders/naver_news_spider.py
import re
import scrapy
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from uuid import uuid5, NAMESPACE_URL


def _clean(s: str | None) -> str | None:
    """공백/개행 정리."""
    if not s:
        return None
    s = s.replace("\xa0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s or None


def _now_kst() -> datetime:
    """KST datetime (aware)."""
    return datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Seoul"))


def _now_kst_str() -> str:
    """KST 'YYYY-MM-DD HH:MM:SS' 문자열."""
    return _now_kst().strftime("%Y-%m-%d %H:%M:%S")


def _to_date(text: str | None) -> date | None:
    """
    '2025.10.24', '2025-10-24', '2025/10/24 09:10' 등에서 date 반환.
    """
    if not text:
        return None
    m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", text)
    if not m:
        return None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return date(y, mo, d)


def _to_yymmdd_from_date(d: date | None) -> str | None:
    if not d:
        return None
    return f"{d.year % 100:02d}.{d.month:02d}.{d.day:02d}"


class NaverNewsSpider(scrapy.Spider):
    """
    네이버 뉴스 섹션(100,101,102,104) 1년치 수집용 스파이더.
    - 기본: mode=archive (날짜 아카이브 경로, 1년 범위 완주에 유리)
    - 발행일(article_published_at) 기준 컷오프 (since_days / since_date)
    - 자연 종료: 날짜/페이지에 더 이상 링크가 없으면 멈춤
    사용 예:
      scrapy crawl naver_news -a sections=100,101,102,104 -a mode=archive -a since_days=365 -O naver_news_1y_pub.csv -s JOBDIR=.crawlstate/naver_news_1y
    """

    name = "naver_news"
    allowed_domains = ["news.naver.com", "n.news.naver.com", "naver.com"]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 0.7,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "AUTOTHROTTLE_ENABLED": True,
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
        # -O로 출력할 거면 FEEDS는 생략해도 됩니다.
    }

    def __init__(
        self,
        sections="100,101,102,104",   # 정치, 경제, 사회, 세계
        max_headlines=1000,           # 섹션 모드에서만 의미 큼(여유값)
        max_latest=1000,              # 섹션 모드에서만 의미 큼(여유값)
        max_pages=None,               # None이면 자연 종료(상한 없음)
        since_days=365,               # 발행일 기준 최근 N일
        since_date=None,              # 'YYYY-MM-DD' 우선
        mode="archive",               # 'archive' | 'section'
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sections = [s.strip() for s in sections.split(",") if s.strip()]
        self.max_headlines = int(max_headlines)
        self.max_latest = int(max_latest)
        self.max_pages = int(max_pages) if max_pages is not None else None
        self.mode = (mode or "archive").lower()

        # 컷오프(발행일 기준)
        if since_date:
            d = _to_date(since_date)
            if not d:
                raise ValueError(f"Invalid since_date: {since_date} (ex. 2024-10-28)")
            self.pub_cutoff = d
        else:
            self.pub_cutoff = _now_kst().date() - timedelta(days=int(since_days))

        # 중복 방지: 원문 URL 기준(모바일/데스크톱 혼재 대비)
        self.seen_urls: set[str] = set()

        # 섹션 모드 카운터
        self.cnt_headline = {sec: 0 for sec in self.sections}
        self.cnt_latest = {sec: 0 for sec in self.sections}

    # ───────────────── start ─────────────────
    def start_requests(self):
        self.logger.info(
            "sections=%s mode=%s pub_cutoff=%s max_pages=%s",
            ",".join(self.sections),
            self.mode,
            self.pub_cutoff.isoformat() if hasattr(self.pub_cutoff, "isoformat") else self.pub_cutoff,
            self.max_pages,
        )

        if self.mode == "archive":
            today = _now_kst().date()
            for sec in self.sections:
                d = today
                while d >= self.pub_cutoff:
                    ymd = f"{d.year:04d}{d.month:02d}{d.day:02d}"
                    url = f"https://news.naver.com/main/list.naver?mode=LSD&mid=sec&sid1={sec}&date={ymd}&page=1"
                    yield scrapy.Request(
                        url,
                        callback=self.parse_archive_list,
                        cb_kwargs={"section": sec, "ymd": ymd, "page": 1},
                    )
                    d -= timedelta(days=1)
        else:
            # UI 섹션 경로(과거로 깊게 못 내려가는 한계가 있음)
            for sec in self.sections:
                url = f"https://news.naver.com/section/{sec}"
                yield scrapy.Request(
                    url,
                    callback=self.parse_section,
                    cb_kwargs={"section": sec, "page_idx": 1},
                )

    # ───────────────── archive list (날짜 아카이브) ─────────────────
    def parse_archive_list(self, response, section: str, ymd: str, page: int):
        # 셀렉터 다양성 대응(아카이브 구조)
        links = response.css(
            ".list_body .type06_headline li dt a::attr(href), "
            ".list_body .type06 li dt a::attr(href), "
            "#main_content .list_body li dt a::attr(href)"
        ).getall()
        links = [urljoin(response.url, h) for h in links]
        if not links:
            # 이 날짜 끝(자연 종료)
            return

        for abs_url in links:
            if abs_url in self.seen_urls:
                continue
            self.seen_urls.add(abs_url)
            yield scrapy.Request(
                abs_url,
                callback=self.parse_article,
                cb_kwargs={"section": section, "list_type": "archive"},
            )

        # 다음 페이지로(상한 없으면 계속, 있으면 상한까지)
        if self.max_pages is None or page < self.max_pages:
            next_url = re.sub(r"([?&]page=)(\d+)", lambda m: f"{m.group(1)}{int(m.group(2))+1}", response.url)
            yield scrapy.Request(
                next_url,
                callback=self.parse_archive_list,
                cb_kwargs={"section": section, "ymd": ymd, "page": page + 1},
            )

    # ───────────────── section page (UI 기반) ─────────────────
    def parse_section(self, response, section: str, page_idx: int):
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

        # 헤드라인 더보기
        more_headline = response.css(
            "#newsct div.as_section_headline._PERSIST_CONTENT "
            "div.section_more._SECTION_HEADLINE_MORE_BUTTON_WRAP > a::attr(href)"
        ).get()
        if more_headline and (self.max_pages is None or page_idx < self.max_pages):
            yield scrapy.Request(
                urljoin(response.url, more_headline),
                callback=self.parse_section,
                cb_kwargs={"section": section, "page_idx": page_idx + 1},
            )

        # 2) 최신
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

        # 최신 더보기
        more_latest = response.css("#newsct > div.section_latest > div > div.section_more > a::attr(href)").get()
        if more_latest and (self.max_pages is None or page_idx < self.max_pages):
            yield scrapy.Request(
                urljoin(response.url, more_latest),
                callback=self.parse_section,
                cb_kwargs={"section": section, "page_idx": page_idx + 1},
            )

    # ───────────────── article page ─────────────────
    def parse_article(self, response, section: str, list_type: str):
        # 제목
        title = (
            _clean(response.css("#title_area > span::text").get())
            or _clean(response.css("#title_area::text").get())
            or _clean(response.css("h1, h2").xpath("string(.)").get())
        )

        # 발행일(입력 시각) 파싱
        ts_nodes = response.css(".media_end_head_info_datestamp_time")
        published_raw = None
        if len(ts_nodes) >= 1:
            published_raw = (
                ts_nodes[0].attrib.get("data-date-time")
                or ts_nodes[0].attrib.get("data-modify-date-time")
                or ts_nodes[0].xpath("string(.)").get()
            )
        published_raw = _clean(published_raw)
        pub_dt = _to_date(published_raw)

        # 컷오프 적용(발행일 없거나 컷오프 이전이면 drop)
        if (pub_dt is None) or (pub_dt < self.pub_cutoff):
            return

        article_published_at = _to_yymmdd_from_date(pub_dt)  # 'YY.MM.DD'

        # 언론사
        press = (
            _clean(response.css("#ct .media_end_head_top a img::attr(title)").get())
            or _clean(response.css('meta[property="og:article:author"]::attr(content)').get())
            or _clean(response.css('meta[name="twitter:creator"]::attr(content)').get())
            or _clean(response.css(".media_end_head_top_logo::text, .media_end_linked_more::text").get())
            or _clean(response.css(".media_end_head_top a::text").get())
        )

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

        # 타임스탬프
        created_at = _now_kst_str()
        latest_scraped_at = created_at

        yield {
            "uuid": uuid_val,
            "article_id": article_id,
            "section": section,
            "list_type": list_type,  # 'archive' | 'headline' | 'latest'
            "press": press,
            "link": link,            # 정규화된 기사 링크
            "title": title,
            "texts": texts,
            "article_published_at": article_published_at,  # 'YY.MM.DD'
            "created_at": created_at,                      # 'YYYY-MM-DD HH:MM:SS' (KST)
            "latest_scraped_at": latest_scraped_at,
        }

    # ───────────────── URL helpers ─────────────────
    def _extract_oid_aid(self, url: str):
        try:
            p = urlparse(url)
            parts = [x for x in p.path.split("/") if x]
            # /article/{oid}/{aid}
            if len(parts) >= 3 and parts[-3] == "article":
                oid, aid = parts[-2], parts[-1]
                if (oid or "").isdigit() and (aid or "").isdigit():
                    return oid, aid
            # ?oid=...&aid=...
            qs = parse_qs(p.query)
            return qs.get("oid", [None])[0], qs.get("aid", [None])[0]
        except Exception:
            return None, None

    def _canonical_link(self, oid: str | None, aid: str | None) -> str | None:
        return f"https://news.naver.com/article/{oid}/{aid}" if (oid and aid) else None

    def _make_uuid(self, oid: str | None, aid: str | None, url: str) -> str:
        base = self._canonical_link(oid, aid) or url
        return str(uuid5(NAMESPACE_URL, base))
