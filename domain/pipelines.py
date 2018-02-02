# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from domain.MongoCache import MongoCache

class DomainPipeline(object):
    def __init__(self):
        self.mongoCache = MongoCache()
        
    def __del__(self):
        print('pipeline end')

    def process_item(self, item, spider):
        if spider.name == 'url':
            self.mongoCache.setHouse(item)
        elif spider.name == 'rent':
            self.mongoCache.setHouse(item)
        return item
    
    def close_spider(self, spider):
        self.mongoCache.excuteAllBulk()