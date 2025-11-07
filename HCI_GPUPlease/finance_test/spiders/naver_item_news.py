# spiders/naver_item_news.py
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs
from uuid import uuid5, NAMESPACE_URL
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo

import scrapy

# --------- 설정/상수 ----------
CODE_RE = re.compile(r"^\d{6}$")
BASE_NEWS_URL = "https://finance.naver.com/item/news_news.naver"

# Scrapy 출력 필드 순서
OUT_FIELDS = [
    "uuid", "press", "article_id", "title", "code", "link", "texts",
    "article_published_at", "created_at", "latest_scraped_at",
]

# --------- 유틸 ----------
def _build_list_url(code: str, page: int) -> str:
    # 1페이지는 page를 공란으로 두는 것이 네이버 쪽에서 더 안정적
    if page <= 1:
        return f"{BASE_NEWS_URL}?code={code}&page=&clusterId="
    return f"{BASE_NEWS_URL}?code={code}&page={page}&clusterId="

def _clean(s: str | None) -> str | None:
    if not s:
        return None
    s = s.replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", s) or None

def _one_line(s: str | None) -> str | None:
    """줄바꿈/탭 제거해서 완전 한 줄 문자열로."""
    if s is None:
        return None
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s or None

def _normalize_text_block(s: str | None) -> str | None:
    """여러 줄 본문을 '한 줄'로 정규화."""
    if not s:
        return None
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in s.splitlines()]
    return _one_line(" ".join([ln for ln in lines if ln]))

def _now_kst() -> datetime:
    return datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Seoul"))

def _now_kst_str() -> str:
    return _now_kst().strftime("%Y-%m-%d %H:%M:%S")

def _parse_ymd_to_yymmdd(text: str | None) -> str | None:
    """
    입력: '2025.10.24', '2025-10-24', '2025/10/24', '2025.10.24 09:10' 등
    출력: '25.10.24'
    """
    dt = _to_date(text)
    if not dt:
        return None
    return f"{dt.year%100:02d}.{dt.month:02d}.{dt.day:02d}"

def _to_date(text: str | None) -> date | None:
    """다양한 포맷의 날짜 문자열을 date로 파싱 (가능한 경우만)."""
    if not text:
        return None
    text = text.strip()
    # YYYY.MM.DD, YYYY-MM-DD, YYYY/MM/DD (+ time 허용)
    m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", text)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    # YY.MM.DD도 가끔 목록에 보일 수 있으니 보조 처리
    m2 = re.search(r"(\d{2})[.\-/](\d{1,2})[.\-/](\d{1,2})", text)
    if m2:
        y, mo, d = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
        y += 2000 if y < 70 else 1900  # 보수적 해석
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    return None

def _extract_oid_aid_from_url(url: str) -> tuple[str | None, str | None]:
    """
    /news_read.naver?office_id=277&article_id=0005709756
    또는 /article/277/0005709756 형태 모두 지원
    """
    try:
        p = urlparse(url)
        # 경로 기반(/article/oid/aid)이면 우선 사용
        parts = [x for x in p.path.split("/") if x]
        if len(parts) >= 3 and parts[-3] == "article":
            oid, aid = parts[-2], parts[-1]
            if (oid or "").isdigit() and (aid or "").isdigit():
                return oid, aid
        # 쿼리스트링 기반
        qs = parse_qs(p.query)
        oid = (qs.get("office_id") or qs.get("oid") or [None])[0]
        aid = (qs.get("article_id") or qs.get("aid") or [None])[0]
        return oid, aid
    except Exception:
        return None, None

def _canonical_article_url(oid: str | None, aid: str | None) -> str | None:
    if not (oid and aid):
        return None
    return f"https://news.naver.com/article/{oid}/{aid}"

# --------- 스파이더 ----------
class NaverItemNewsSpider(scrapy.Spider):
    """
    codes_kosdaq.txt(또는 -a codes_path=...)에서 6자리 종목코드를 읽어
    네이버 금융 종목 뉴스 목록(news_news.naver)을 순회.
    각 항목의 상세에서 oid/aid를 추출해 news.naver.com 정규화 URL로 들어가
    본문과 발행일을 수집. (옵션: -a since_days=365 로 최근 N일만 수집)
    """
    name = "naver_item_news"
    allowed_domains = ["finance.naver.com", "news.naver.com", "n.news.naver.com", "naver.com"]

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
            ),
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://finance.naver.com/",
        },
        "DOWNLOAD_DELAY": 0.4,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "FEED_EXPORT_ENCODING": "utf-8",
        "ROBOTSTXT_OBEY": False,
        "LOG_LEVEL": "INFO",
        # 기본 FEEDS: 외부에서 -s FEEDS={}로 끌 수 있음
        "FEEDS": {
            "item_news_%(time)s.csv": {
                "format": "csv",
                "encoding": "utf-8-sig",
                "fields": OUT_FIELDS,
            }
        },
    }

    def __init__(
        self,
        codes_path: str = "codes_kosdaq.txt",
        max_pages: int | None = None,
        since_days: int | None = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.codes_path = codes_path
        self.max_pages = int(max_pages) if max_pages else None
        self.since_days = int(since_days) if since_days else None
        self.cutoff_date: date | None = None
        if self.since_days:
            self.cutoff_date = (_now_kst() - timedelta(days=self.since_days)).date()
        self._codes: list[str] = []

    # ---------- load codes ----------
    def _read_codes(self) -> list[str]:
        p = Path(self.codes_path)
        if not p.exists():
            raise FileNotFoundError(f"codes file not found: {p.resolve()}")
        codes: list[str] = []
        for ln in p.read_text(encoding="utf-8").splitlines():
            c = ln.strip()
            if CODE_RE.fullmatch(c):
                codes.append(c)
        return sorted(set(codes))

    # ---------- entry ----------
    def start_requests(self):
        self._codes = self._read_codes()
        self.logger.info("Loaded %d codes from %s", len(self._codes), self.codes_path)
        if self.cutoff_date:
            self.logger.info("Cutoff date (since_days=%s): %s", self.since_days, self.cutoff_date.isoformat())
        for code in self._codes:
            url = _build_list_url(code, 1)
            yield scrapy.Request(
                url,
                callback=self.parse_list,
                cb_kwargs={"code": code, "page": 1},
                dont_filter=True,
            )

    # ---------- list page ----------
    def parse_list(self, response, code: str, page: int):
        rows = response.css("table.type5 > tbody > tr")
        found_any = False
        hit_older_than_cutoff = False

        for tr in rows:
            a = tr.css("td.title > a::attr(href)").get()
            if not a:
                continue
            href = urljoin(response.url, a)

            # 기사 링크만 통과
            if "/item/news_read.naver" not in href and "/news/news_read.naver" not in href:
                continue

            found_any = True
            title = _clean(tr.css("td.title > a::text").get())
            press = _clean(tr.css("td.info::text").get())
            list_date_raw = _clean(tr.css("td.date::text, span.date::text").get())

            # 목록 날짜로 선 차단(최신 → 오래된 순 정렬 가정)
            if self.cutoff_date:
                dt = _to_date(list_date_raw)
                if dt and dt < self.cutoff_date:
                    hit_older_than_cutoff = True
                    # 더 아래도 모두 오래된 가능성↑ → 이 코드의 페이지네이션 종료
                    continue

            # 상세(중간 페이지)로 진입
            yield scrapy.Request(
                href,
                callback=self.parse_article,
                headers={"Referer": response.url},
                cb_kwargs={
                    "code": code,
                    "title_from_list": title,
                    "press_from_list": press,
                    "list_date_raw": list_date_raw,
                },
            )

        # cutoff로 멈춰야 하면 페이지네이션 중단
        if hit_older_than_cutoff:
            return

        # 페이지네이션 제어
        if self.max_pages is not None and page >= self.max_pages:
            return

        # 마지막 페이지(끝으로 pgRR) 파악
        last_page = None
        rr = response.css("a.pgRR::attr(href)").get()
        if rr:
            q = parse_qs(urlparse(urljoin(response.url, rr)).query)
            last_page = int((q.get("page") or ["1"])[0])

        if found_any:
            if last_page is not None and page >= last_page:
                return
            next_page = page + 1
            next_url = _build_list_url(code, next_page)
            yield scrapy.Request(
                next_url,
                callback=self.parse_list,
                cb_kwargs={"code": code, "page": next_page},
                dont_filter=True,
            )

    # ---------- article page ----------
    def parse_article(
        self, response, code: str,
        title_from_list: str | None, press_from_list: str | None, list_date_raw: str | None
    ):
        # 1) canonical(뉴스 본문 페이지) 도달 보장
        oid, aid = _extract_oid_aid_from_url(response.url)
        canonical = _canonical_article_url(oid, aid)

        # finance.naver.com의 중간 페이지거나, 본문 셀렉터가 비어 있으면 news 본문으로 점프
        if (("news.naver.com" not in response.url) or not response.css("#dic_area")) and canonical:
            yield scrapy.Request(
                canonical,
                callback=self.parse_article,
                headers={"Referer": response.url},
                cb_kwargs={
                    "code": code,
                    "title_from_list": title_from_list,
                    "press_from_list": press_from_list,
                    "list_date_raw": list_date_raw,
                },
            )
            return

        # 2) 여기부터는 news.naver.com(또는 실제 본문 포함)에서 파싱
        link = canonical or response.url
        article_id = aid or None

        # 제목/언론사 보강 (한 줄화)
        title = _one_line(title_from_list or _clean(
            response.css("#title_area > span::text").get()
            or response.css("#title_area::text").get()
            or response.css("h1, h2").xpath("string(.)").get()
        ))
        press = _one_line(press_from_list or _clean(
            response.css("#ct .media_end_head_top a img::attr(title)").get()
            or response.css('meta[property="og:article:author"]::attr(content)').get()
            or response.css('meta[name="twitter:creator"]::attr(content)').get()
            or response.css(".media_end_head_top_logo::text, .media_end_linked_more::text").get()
            or response.css(".media_end_head_top a::text").get()
        ))

        # 본문 추출 (여러 폴백 → 한 줄화)
        raw = response.css("#dic_area").xpath("string(.)").get()
        texts = _normalize_text_block(raw or "")
        if not texts:
            for sel in ["#newsct_article", "#contents", "article", "[itemprop='articleBody']"]:
                raw = response.css(sel).xpath("string(.)").get()
                texts = _normalize_text_block(raw or "")
                if texts:
                    break
        texts = _one_line(texts)

        # 발행일: 목록 날짜 우선 → 없으면 본문 헤더 추출 (그리고 YY.MM.DD로 직렬화)
        article_published_at = _parse_ymd_to_yymmdd(list_date_raw)
        if not article_published_at:
            cand = (
                response.css(".media_end_head_info_datestamp_time::attr(data-date-time)").get()
                or response.css(".media_end_head_info_datestamp_time::text").get()
                or response.css("#ct .media_end_head_info_datestamp span::attr(data-date-time)").get()
                or response.css("#ct .media_end_head_info_datestamp span::text").get()
            )
            article_published_at = _parse_ymd_to_yymmdd(_clean(cand))
        article_published_at = _one_line(article_published_at)

        # cutoff 검사(목록에서 못 걸렀을 때 대비)
        if self.cutoff_date and article_published_at:
            dt = _to_date(article_published_at)  # 'YY.MM.DD'도 지원
            if dt and dt < self.cutoff_date:
                return

        created_at = _now_kst_str()
        uuid_val = str(uuid5(NAMESPACE_URL, link))

        yield {
            "uuid": uuid_val,
            "press": press,
            "article_id": article_id,
            "title": title,
            "code": code,
            "link": link,
            "texts": texts,
            "article_published_at": article_published_at,  # 예: '25.10.24'
            "created_at": created_at,                       # 예: '2025-11-07 12:34:56'
            "latest_scraped_at": created_at,
        }
