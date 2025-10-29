# spiders/naver_item_news.py
import re
import scrapy
from urllib.parse import urljoin, urlparse, parse_qs
from pathlib import Path
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo
from uuid import uuid5, NAMESPACE_URL


# ───────────────────────── common helpers ─────────────────────────
def _clean(s: str | None):
    if not s:
        return None
    s = s.replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", s) or None


def _now_kst() -> datetime:
    return datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Seoul"))


def _now_kst_str() -> str:
    return _now_kst().strftime("%Y-%m-%d %H:%M:%S")


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


def _to_date(text: str | None) -> date | None:
    """
    입력: '2025.10.24' / '2025-10-24' / '25.10.24' 등
    출력: datetime.date
    """
    if not text:
        return None
    # 4자리 연도
    m = re.search(r"(?P<y>\d{4})[.\-/](?P<m>\d{1,2})[.\-/](?P<d>\d{1,2})", text)
    if m:
        return date(int(m.group("y")), int(m.group("m")), int(m.group("d")))
    # 2자리 연도(네이버 뉴스는 2000년대 가정)
    m = re.search(r"(?P<y>\d{2})[.\-/](?P<m>\d{1,2})[.\-/](?P<d>\d{1,2})", text)
    if m:
        y = 2000 + int(m.group("y"))
        return date(y, int(m.group("m")), int(m.group("d")))
    return None


# ───────────────────────── spider ─────────────────────────
class NaverItemNewsSpider(scrapy.Spider):
    """
    codes_all.txt(한 줄당 6자리 종목코드)에서 코드를 읽어
    https://finance.naver.com/item/news_news.naver?code=... 리스트를 순회하고,
    각 행의 링크에서 OID/AID를 추출하여
    https://news.naver.com/article/{oid}/{aid} 데스크톱 기사 페이지에서 본문을 수집한다.

    정책:
      - 최근 since_days(기본 365일) 이전 기사만 수집
      - max_pages 상한 없음. 한 페이지에서라도 '최근 기사'가 하나도 없으면 해당 종목은 중단
    """
    name = "naver_item_news"
    allowed_domains = ["finance.naver.com", "news.naver.com", "n.news.naver.com", "naver.com"]

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
            "Referer": "https://finance.naver.com/",
        },
        "FEED_EXPORT_ENCODING": "utf-8",
        "LOG_LEVEL": "INFO",
    }

    # ───────── init ─────────
    def __init__(
        self,
        codes_file: str = "codes_all.txt",
        since_days: int = 365,
        *args, **kwargs
    ):
        """
        codes_file: 종목코드 파일 경로(UTF-8 권장, BOM 허용). 한 줄당 6자리 숫자.
        since_days: 오늘(KST) 기준 며칠 전까지 수집(기본 365일)
        """
        super().__init__(*args, **kwargs)
        self.codes_file = Path(codes_file)
        self.since_days = int(since_days)
        self.cutoff_date = (_now_kst() - timedelta(days=self.since_days)).date()

        self.codes = self._load_codes(self.codes_file)
        if not self.codes:
            raise ValueError(
                f"[naver_item_news] codes_file='{self.codes_file}'에서 종목코드를 하나도 못 읽었어요."
            )

        self.articles_seen: set[str] = set()  # 기사 URL 중복 제거
        self.dump_dir = Path("dumps")
        self.dump_dir.mkdir(exist_ok=True)

    # ───────── helpers ─────────
    @staticmethod
    def _load_codes(path: Path) -> list[str]:
        """
        파일 형식:
          - 한 줄당 6자리 코드 (예: 005930)
          - 주석 허용: # 으로 시작하는 줄
          - 공백/탭/빈줄 무시
        """
        codes: list[str] = []
        if not path.exists():
            return codes
        text = path.read_text(encoding="utf-8-sig")  # BOM 대응
        for ln in text.splitlines():
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            m = re.fullmatch(r"\d{6}", ln)
            if m:
                codes.append(m.group(0))
        # 입력 순서 유지하며 중복 제거
        seen, uniq = set(), []
        for c in codes:
            if c not in seen:
                seen.add(c)
                uniq.append(c)
        return uniq

    @staticmethod
    def _finance_list_url(code: str, page: int) -> str:
        return f"https://finance.naver.com/item/news_news.naver?code={code}&page={page}&clusterId="

    @staticmethod
    def _extract_oid_aid_from_href(href: str) -> tuple[str | None, str | None]:
        """
        /item/news_read.naver?article_id=0005678901&office_id=123&code=005930&page=1...
        또는 read.naver?oid=...&aid=... 형태 모두 대응
        """
        try:
            p = urlparse(href)
            qs = parse_qs(p.query)
            oid = qs.get("office_id", [None])[0]
            aid = qs.get("article_id", [None])[0]
            # 대체 파라미터
            if not (oid and aid):
                oid = qs.get("oid", [oid])[0]
                aid = qs.get("aid", [aid])[0]
            return oid, aid
        except Exception:
            return None, None

    @staticmethod
    def _canonical_article_url(oid: str | None, aid: str | None, fallback: str | None = None) -> str | None:
        if oid and aid:
            return f"https://news.naver.com/article/{oid}/{aid}"
        return fallback

    @staticmethod
    def _tidy_texts(response) -> str | None:
        """
        본문은 #dic_area(신규 레이아웃)를 우선, 실패 시 폴백을 순서대로 시도.
        줄바꿈을 살짝 유지한 뒤 공백 정리.
        """
        selectors = [
            "#dic_area *::text",
            "#dic_area::text",
            "#newsct_article *::text",
            "#newsct_article::text",
            "#contents *::text",
            "#contents::text",
            "article *::text",
            "article::text",
        ]
        for sel in selectors:
            parts = response.css(sel).getall()
            if parts:
                joined = "\n".join([p for p in parts if _clean(p)])
                return _clean(joined)
        any_text = response.xpath("string(//body)").get()
        return _clean(any_text)

    def _make_uuid(self, canonical_url_or_any: str) -> str:
        return str(uuid5(NAMESPACE_URL, canonical_url_or_any))

    # ───────── entry ─────────
    def start_requests(self):
        self.logger.info(
            f"[naver_item_news] codes_file='{self.codes_file}', "
            f"codes={len(self.codes)}개, cutoff={self.cutoff_date}"
        )
        for code in self.codes:
            yield scrapy.Request(
                self._finance_list_url(code, 1),
                callback=self.parse_finance_list,
                cb_kwargs={"code": code, "page": 1},
            )

    # ───────── list page ─────────
    def parse_finance_list(self, response, code: str, page: int):
        """
        네이버 금융 종목 뉴스 리스트(table.type5) 파싱.
        - 이 페이지에서 cutoff(최근 1년 등) 이후 기사가 하나라도 있으면 다음 페이지로 진행
        - 하나도 없으면 해당 종목은 중단
        """
        rows = response.css("table.type5 > tbody > tr")
        if not rows:
            return  # 빈 페이지 = 종료

        page_has_recent = False

        for tr in rows:
            href = tr.css('a[href*="news_read.naver"]::attr(href)').get() \
                   or tr.css('a[href*="read.naver"]::attr(href)').get()
            if not href:
                continue
            abs_href = urljoin(response.url, href)

            # 날짜(우측 정렬 셀/클래스가 다를 수 있어 후보를 여럿 시도)
            date_text = _clean(tr.css("td.date::text").get()) \
                     or _clean(tr.css("td[align='right']::text").get()) \
                     or _clean(tr.css("td:nth-last-child(1)::text").get())
            item_date = _to_date(date_text)

            # 날짜 컷: 최근 범위에 속하지 않으면 스킵
            if item_date and item_date >= self.cutoff_date:
                page_has_recent = True
            else:
                continue

            # 언론사 힌트(리스트에 보이는 값)
            press_hint = _clean(tr.css("td.info::text").get()) \
                      or _clean(tr.css("span.press::text").get()) \
                      or _clean(tr.css("td:nth-child(3)::text").get())

            # OID/AID → 데스크톱 기사 URL 정규화
            oid, aid = self._extract_oid_aid_from_href(abs_href)
            article_url = self._canonical_article_url(oid, aid, fallback=abs_href)
            if not article_url or article_url in self.articles_seen:
                continue
            self.articles_seen.add(article_url)

            yield scrapy.Request(
                article_url,
                callback=self.parse_article,
                cb_kwargs={
                    "code": code,
                    "source_list_url": response.url,
                    "press_hint": press_hint,
                    "article_published_at_hint": _to_yymmdd(date_text),
                },
            )

        # 다음 페이지로 갈지 결정 (max_pages 제한 없음)
        if page_has_recent:
            next_url = self._finance_list_url(code, page + 1)
            yield scrapy.Request(
                next_url,
                callback=self.parse_finance_list,
                cb_kwargs={"code": code, "page": page + 1},
            )
        # else: 최근 기사 하나도 없으므로 이 종목은 여기서 중단

    # ───────── article page ─────────
    def parse_article(self, response, code: str, source_list_url: str,
                      press_hint: str | None, article_published_at_hint: str | None):

        # 제목
        title = (
            _clean(response.css("#title_area > span::text").get())
            or _clean(response.css("#title_area::text").get())
            or _clean(response.css("h1, h2").xpath("string(.)").get())
        )

        # 입력(게시) 시각 → YY.MM.DD
        ts_nodes = response.css(".media_end_head_info_datestamp_time")
        published_raw = None
        if len(ts_nodes) >= 1:
            published_raw = (
                ts_nodes[0].attrib.get("data-date-time")
                or ts_nodes[0].attrib.get("data-modify-date-time")
                or ts_nodes[0].xpath("string(.)").get()
            )
        article_published_at = _to_yymmdd(_clean(published_raw)) or article_published_at_hint

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
          or _clean(response.css(".media_end_head_top a::text").get()) \
          or press_hint

        # 본문
        texts = self._tidy_texts(response)

        # OID/AID 및 링크 재확인
        def _extract_oid_aid(url: str):
            try:
                p = urlparse(url)
                parts = [x for x in p.path.split("/") if x]
                if len(parts) >= 3 and parts[-3] == "article":
                    return parts[-2], parts[-1]
                qs = parse_qs(p.query)
                return qs.get("oid", [None])[0], qs.get("aid", [None])[0]
            except Exception:
                return None, None

        oid, aid = _extract_oid_aid(response.url)
        article_id = aid or None
        link = self._canonical_article_url(oid, aid, fallback=response.url)

        uuid_val = self._make_uuid(link)
        created_at = _now_kst_str()

        yield {
            "uuid": uuid_val,
            "article_id": article_id,
            "stock_code": code,
            "press": press,
            "title": title,
            "link": link,                           # 정규화된 데스크톱 기사 링크
            "texts": texts,                         # 본문 텍스트
            "article_published_at": article_published_at,  # 'YY.MM.DD'
            "created_at": created_at,                        # 'YYYY-MM-DD HH:MM:SS' (KST)
            "latest_scraped_at": created_at,
            "source_list_url": source_list_url,     # 시작 리스트 URL(추적용)
        }
