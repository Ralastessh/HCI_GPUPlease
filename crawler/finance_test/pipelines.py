# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class FinanceTestPipeline:
    def process_item(self, item, spider):
        return item

# 중복 항목 제거
class DedupePipeline:
    def __init__(self):
        self.seen_ids = set()

    def process_item(self, item, spider):
        oid = item.get('original_id')
        if oid in self.seen_ids:
            raise DropItem(f"중복 아이템 제거: {oid}")
        self.seen_ids.add(oid)
        return item