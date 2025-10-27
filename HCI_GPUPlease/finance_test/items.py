# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsItem(scrapy.Item):
    article_id = scrapy.Field()
    media_id = scrapy.Field()
    media_name = scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()

    # 시간 관련 필드
    article_published_at = scrapy.Field()
    created_at = scrapy.Field()
    latest_scraped_at = scrapy.Field()

    # 추가 정보
    original_id = scrapy.Field()
    is_origin = scrapy.Field()
    ticker = scrapy.Field()
    sentiment = scrapy.Field()

class ReportItem(scrapy.Item):
    category = scrapy.Field()
    stock_name = scrapy.Field()
    report_name = scrapy.Field()
    firm_name= scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()
    texts = scrapy.Field()

    article_published_at = scrapy.Field()
    created_at = scrapy.Field()
    latest_scraped_at = scrapy.Field()

    original_id = scrapy.Field()
