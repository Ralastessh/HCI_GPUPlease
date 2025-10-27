import scrapy
import re
from datetime import datetime, timedelta
from finance_test.items import NewsItem
from finance_test.items import ReportItem


class ReportSpider(scrapy.Spider):
    # '네이버증권 리서치' 메인 화면
    name = 'report'
    start_urls = ['https://finance.naver.com/research/']

    # 크롤링 설정(봇 차단 우회, csv 속성 순서 지정)
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/124.0.0.0 Safari/537.36',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        },
        'FEED_EXPORT_FIELDS': [
            'report_name',
            'category',
            'stock_name',
            'title',
            'firm_name',
            'link',
            'texts',
            'article_published_at',
            'created_at',
            'latest_scraped_at',
            'original_id'
        ]
    }

    # Spider 부모 클래스 내장 메소드
    
    # 리서치 목록(ex. 시황정보 리포트, 투자정보 리포트, ...) 별로 탐색
    def parse(self, response):
        sections = response.xpath('//ul[@class="nav1"]/li')
        for sec in sections:
            link = sec.xpath('./a/@href').get()
            report_name = sec.xpath('./a/strong/span[@class="blind"]/text()').get()
            if report_name:
                report_name = report_name.strip().split()[0]
            if link:
                # 하위 페이지 탐색
                yield response.follow(link, self.parse_report_list, meta={'report_name': report_name})

    # 종목 별 리포트 리스트
    def parse_report_list(self, response):
        report_name = response.meta.get('report_name')
        current_page_match = re.search(r'page=(\d+)', response.url)
        current_page = int(current_page_match.group(1)) if current_page_match else 1

        rows = response.xpath('//table[@class="type_1"]//tr[td[@class="date"]]')
        # 기준일(ex. days=1 -> 하루치) 설정
        cutoff_date = datetime.now() - timedelta(days=1)
        stop_crawling = False

        for row in rows:
            date_str = row.xpath('./td[@class="date"]/text()').get()
            try:
                article_date = datetime.strptime(date_str.strip(), "%y.%m.%d")
            except Exception:
                continue

            # 1년보다 이전이면 중단 플래그
            if article_date < cutoff_date:
                stop_crawling = True
                break

            # csv에 포함될 속성 값 크롤링
            item = ReportItem()
            item['report_name'] = report_name
            a_tags = [a.strip() for a in row.xpath('.//a/text()').getall() if a.strip()]
            item['stock_name'] = None
            item['category'] = None
            item['title'] = None

            # 리포트 종류별 a태그 위치 예외 처리
            if report_name == '종목분석':
                item['stock_name'] = a_tags[0]
                item['title'] = a_tags[1]
                '''if len(a_tags) >= 2:
                    
                elif len(a_tags) == 1:
                    item['title'] = a_tags[0]'''

            elif report_name == '산업분석':
                # 산업분석 리포트의 첫 번째 td에는 산업 카테고리가 있고, 두 번째 td 내부 a 태그가 제목에 해당함.
                item['category'] = row.xpath('normalize-space(./td[1]//text())').get()
                title_candidates = [a.strip() for a in row.xpath('./td[2]//a/text()').getall() if a.strip()]
                if title_candidates:
                    item['title'] = title_candidates[0]

            else:
                # 일반 리포트는 첫 번째 a 태그가 제목
                if a_tags:
                    item['title'] = a_tags[0]

            item['link'] = response.urljoin(row.xpath('.//a[contains(@href, "_read.naver")]/@href').get())
            match = re.search(r'nid=(\d+)', item['link'])
            item['original_id'] = match.group(1) if match else None
            # '종목분석'과 '산업분석' 리포트의 제목 지정 예외 처리(제목보다 우선순위 속성 있기 때문)
            item['firm_name'] = (row.xpath('string(./td[3])').get().strip() or row.xpath('string(./td[2])').get().strip())      
            item['article_published_at'] = date_str
            item['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            item['latest_scraped_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yield response.follow(item['link'], self.parse_report_detail, meta={'item': item})

        # 오래된 데이터가 나오면 이후 페이지 탐색 중단
        if not stop_crawling:
            next_page = response.xpath('//td[@class="pgR"]/a/@href').get()
            if next_page:
                yield response.follow(next_page, self.parse_report_list, meta={'report_name': report_name})

    # 리포트 스크립트
    def parse_report_detail(self, response):
        item = response.meta['item']
        content = response.xpath('//td[@class="view_cnt"]//text()').getall()
        if not content:
            content = response.xpath('//div[@class="view_cnt"]//text() | //div[contains(@class,"report_view")]//text()').getall()
        item['texts'] = ' '.join([c.strip() for c in content if c and c.strip()])
        yield item