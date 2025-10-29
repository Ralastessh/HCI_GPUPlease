# spiders/naver_spider.py
import scrapy, json, re
from urllib.parse import urljoin, urlparse, parse_qs
from scrapy.selector import Selector
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from uuid import uuid5, NAMESPACE_URL
from pathlib import Path

LIST_URL_TPL   = "https://finance.naver.com/item/board.naver?code={code}&page={page}"
DETAIL_URL_TPL = "https://finance.naver.com/item/board_read.naver?code={code}&nid={nid}&page={page}"

class NaverSpider(scrapy.Spider):
    name = "naver"
    allowed_domains = ["finance.naver.com", "naver.com", "m.stock.naver.com"]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 0.6,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "DEFAULT_REQUEST_HEADERS": {"Accept-Language": "ko-KR,ko;q=0.9"},
        "FEEDS": {
            "boards_%(time)s.json": {
                "format": "json",
                "encoding": "utf-8",
                "fields": ["id","code","title","link","uploaded_at","latest_scraped_at","texts"],
                "indent": 2,
                "item_export_kwargs": {"ensure_ascii": False},
            }
        },
        "LOG_LEVEL": "INFO",
    }

    def __init__(
        self,
        code=None,                   # 단일 코드
        codes=None,                  # "005930,000660,..." 쉼표구분
        codes_file="codes_all.txt",  # 파일에서 라인별 코드 로드 (기본)
        start_page=1,
        end_page=None,               # 명시 없으면 컷오프까지만 자동 탐색
        first_n=None,                # 파일 상단 N개 코드만 사용(테스트용)
        since_days=365,              # ← 기본 1년
        since_date=None,             # ← 예: 2024-10-28
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

        base_codes = []
        if code:
            base_codes = [str(code).strip()]
        elif codes:
            base_codes = [c.strip() for c in codes.split(",") if c.strip()]
        else:
            p = Path(codes_file)
            if not p.exists():
                raise FileNotFoundError(
                    f"codes_file not found: {p.resolve()}\n"
                    f" - 파일을 준비하거나, -a code=005930 또는 -a codes=... 로 지정하세요."
                )
            base_codes = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]

        self.codes = sorted(set(base_codes))
        if first_n:
            try:
                self.codes = self.codes[: int(first_n)]
            except Exception:
                pass

        self.start_page = int(start_page)
        self.end_page = int(end_page) if end_page else None

        # 컷오프 날짜(KST) 계산
        self.cutoff_date: date
        if since_date:
            self.cutoff_date = self._parse_to_date(since_date) or self._now_kst().date() - timedelta(days=int(since_days))
        else:
            self.cutoff_date = self._now_kst().date() - timedelta(days=int(since_days))

        # 종목별 중복 방지(nid)
        self.seen_nids = {c: set() for c in self.codes}

        self.UA = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        )

    # ───────────── Helpers ─────────────
    def _now_kst(self) -> datetime:
        return datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Seoul"))

    def _now_kst_str(self) -> str:
        return self._now_kst().strftime("%Y-%m-%d %H:%M:%S")

    def _parse_to_date(self, s: str | None) -> date | None:
        """
        '2025-10-24', '2025.10.24', '2025/10/24 09:10' 등 → date
        """
        if not s:
            return None
        s = " ".join(s.split())
        m = re.search(r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})", s)
        if not m:
            return None
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return date(y, mo, d)

    def _to_yymmdd(self, s: str | None) -> str | None:
        """출력용 'YY.MM.DD' 포맷으로 변환."""
        dt = self._parse_to_date(s)
        if not dt:
            return None
        return f"{dt.year%100:02d}.{dt.month:02d}.{dt.day:02d}"

    def _html_to_text(self, html: str | None) -> str | None:
        """iframe contentHtml 등에서 태그 제거 + 줄바꿈 보존."""
        if not html:
            return None
        html = re.sub(r'(?i)<br\s*/?>', '\n', html)
        html = re.sub(r'(?i)</(p|div|li|h[1-6]|section|article|tr|td|th)>', r'\n', html)
        html = re.sub(r'(?is)<(script|style).*?>.*?</\1>', '', html)
        sel = Selector(text=html)
        text = sel.xpath('string(.)').get() or ''
        lines = [re.sub(r'\s+', ' ', ln).strip() for ln in text.splitlines()]
        return "\n".join([ln for ln in lines if ln]) or None

    def _extract_values_from_swjson(self, sw_json_str: str | None) -> str | None:
        """contentJsonSwReplaced(문자열 JSON)에서 모든 "value"만 줄바꿈 결합."""
        if not sw_json_str:
            return None
        try:
            obj = json.loads(sw_json_str)
        except Exception:
            return None
        values = []
        def walk(node):
            if isinstance(node, dict):
                if "value" in node and isinstance(node.get("value"), str):
                    v = node["value"].strip()
                    if v:
                        values.append(v)
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for v in node:
                    walk(v)
        walk(obj)
        return "\n".join(values) or None

    # ───────────── 1) 시작 요청(첫 페이지만) ─────────────
    def start_requests(self):
        self.logger.info("Loaded %d codes to crawl", len(self.codes))
        self.logger.info("Cutoff date (KST): %s", self.cutoff_date.isoformat())
        for code in self.codes:
            first_url = LIST_URL_TPL.format(code=code, page=self.start_page)
            yield scrapy.Request(
                first_url,
                headers={"User-Agent": self.UA, "Referer": "https://finance.naver.com/"},
                callback=self.parse_list,
                cb_kwargs={"code": code, "page": self.start_page},
            )

    # ───────────── 2) 목록에서 nid + 목록행 날짜 추출 → 상세 ─────────────
    def parse_list(self, response, code, page):
        any_newer_or_equal = False   # 컷오프 이상 글을 하나라도 봤는가
        all_old_or_undated = True    # 페이지가 전부 컷오프 이전(또는 날짜 없음)인가

        for tr in response.css("#content > div.section.inner_sub > table.type2 > tbody > tr"):
            a = tr.css("td.title > a")
            if not a:
                continue
            href = a.attrib.get("href", "")
            if "board_read.naver" not in href:
                continue

            # 목록행 날짜(span) → 컷오프 비교
            uploaded_raw = tr.css("td:nth-child(1) > span::text").get()
            dt = self._parse_to_date(uploaded_raw)
            pass_cutoff = (dt is None) or (dt >= self.cutoff_date)

            if pass_cutoff:
                any_newer_or_equal = True
                all_old_or_undated = False if dt is not None else all_old_or_undated
            else:
                # 컷오프 이전이면 이 글은 스킵
                continue

            # nid 추출
            qs = parse_qs(urlparse(urljoin(response.url, href)).query)
            nid = (qs.get("nid") or [None])[0]
            if not nid or nid in self.seen_nids[code]:
                continue
            self.seen_nids[code].add(nid)

            detail_url = DETAIL_URL_TPL.format(code=code, nid=nid, page=page)
            yield scrapy.Request(
                detail_url,
                headers={"User-Agent": self.UA, "Referer": response.url},
                callback=self.parse_detail,
                cb_kwargs={
                    "code": code,
                    "detail_link": detail_url,
                    "uploaded_at": self._to_yymmdd(uploaded_raw),   # 출력용 YY.MM.DD
                },
            )

        # ── 페이지네이션: 컷오프 기반 조기 종료
        # end_page가 지정되어 있으면 그 범위까지만, 아니면 컷오프를 기준으로 자동 진행
        next_page = page + 1
        if self.end_page is not None and next_page > self.end_page:
            return

        # 이 페이지에서 컷오프 이상 글을 하나도 못 봤고,
        # (날짜가 있는 행 기준) 전부 컷오프 이전이었다면 → 더 볼 필요 없음(종목 종료)
        if not any_newer_or_equal and all_old_or_undated:
            return

        next_url = LIST_URL_TPL.format(code=code, page=next_page)
        yield scrapy.Request(
            next_url,
            headers={"User-Agent": self.UA, "Referer": response.url},
            callback=self.parse_list,
            cb_kwargs={"code": code, "page": next_page},
        )

    # ───────────── 3) 상세: 본문 or iframe 재요청 ─────────────
    def parse_detail(self, response, code: str, detail_link: str, uploaded_at: str | None):
        # 제목
        title = (
            response.css("#content > div.section.inner_sub > table.view > tbody > tr:nth-child(1) > th:nth-child(1)::text").get()
            or response.css("#content .section.inner_sub .view strong::text").get()
            or response.css("#content .section.inner_sub h3::text").get()
        )
        title = " ".join((title or "").split()) or None

        # 페이지 내 직접 텍스트 시도
        texts = None
        for sel in [
            "#content > div.section.inner_sub > table.view > tbody > tr:nth-child(3) > td",
            "#body",
            "#content .section.inner_sub .view #body",
            "#content .section.inner_sub .view td",
        ]:
            txt = response.css(sel).xpath("string(.)").get()
            txt = " ".join((txt or "").split())
            if txt:
                texts = txt
                break

        if texts:
            yield {
                "id": str(uuid5(NAMESPACE_URL, detail_link)),
                "code": code,
                "title": title,
                "link": detail_link,
                "uploaded_at": uploaded_at,                 # 'YY.MM.DD'
                "latest_scraped_at": self._now_kst_str(),   # 'YYYY-MM-DD HH:MM:SS'
                "texts": texts,
            }
            return

        # iframe(src) 추출 → m.stock Next.js 혹은 일반 HTML 본문
        iframe_src = (
            response.css("#pc-iframe-content::attr(src)").get()
            or response.css("#pc-iframe-content iframe::attr(src)").get()
            or response.css(".view iframe::attr(src)").get()
        )
        if iframe_src:
            iframe_url = urljoin(response.url, iframe_src)
            yield scrapy.Request(
                iframe_url,
                headers={"User-Agent": self.UA, "Referer": response.url, "Accept-Language": "ko-KR,ko;q=0.9"},
                callback=self.parse_iframe,
                cb_kwargs={
                    "code": code,
                    "title": title,
                    "detail_link": detail_link,
                    "uploaded_at": uploaded_at,            # 'YY.MM.DD'
                },
            )
        else:
            yield {
                "id": str(uuid5(NAMESPACE_URL, detail_link)),
                "code": code,
                "title": title,
                "link": detail_link,
                "uploaded_at": uploaded_at,
                "latest_scraped_at": self._now_kst_str(),
                "texts": None,
            }

    # ───────────── 4) iframe: Next.js(contentJsonSwReplaced 우선) ─────────────
    def parse_iframe(self, response, code: str, title: str, detail_link: str, uploaded_at: str | None):
        # 4-1) 모바일(Next.js)일 경우: __NEXT_DATA__에서 contentJsonSwReplaced 우선
        next_data = response.css("#__NEXT_DATA__::text").get()
        if next_data:
            try:
                data = json.loads(next_data)
                queries = (data.get("props", {})
                               .get("pageProps", {})
                               .get("dehydratedState", {})
                               .get("queries", []))
                sw_json_str = None
                content_html = None
                for q in queries:
                    key = q.get("queryKey", [])
                    if key and isinstance(key[0], dict) and key[0].get("url") == "/discussion/detail":
                        result = (q.get("state") or {}).get("data", {}).get("result", {})
                        sw_json_str = result.get("contentJsonSwReplaced")  # 문자열 JSON
                        content_html = result.get("contentHtml")
                        break

                texts = self._extract_values_from_swjson(sw_json_str) or self._html_to_text(content_html)

                yield {
                    "id": str(uuid5(NAMESPACE_URL, detail_link)),
                    "code": code,
                    "title": title,
                    "link": detail_link,
                    "uploaded_at": uploaded_at,            # 'YY.MM.DD'
                    "latest_scraped_at": self._now_kst_str(),
                    "texts": texts,
                }
                return
            except Exception:
                pass  # 일반 HTML로 폴백

        # 4-2) 일반 HTML(iframe 문서)
        for sel in ["#body", "body", "td, div"]:
            txt = response.css(sel).xpath("string(.)").get()
            txt = " ".join((txt or "").split())
            if txt:
                yield {
                    "id": str(uuid5(NAMESPACE_URL, detail_link)),
                    "code": code,
                    "title": title,
                    "link": detail_link,
                    "uploaded_at": uploaded_at,
                    "latest_scraped_at": self._now_kst_str(),
                    "texts": txt,
                }
                return

        yield {
            "id": str(uuid5(NAMESPACE_URL, detail_link)),
            "code": code,
            "title": title,
            "link": detail_link,
            "uploaded_at": uploaded_at,
            "latest_scraped_at": self._now_kst_str(),
            "texts": None,
        }
