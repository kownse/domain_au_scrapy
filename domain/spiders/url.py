# -*- coding: utf-8 -*-
from domain.spiders.domain_base import DomainBaseSpider
from domain.items import HouseItem
from scrapy.contrib.loader import ItemLoader
from scrapy.contrib.loader.processor import MapCompose#, Join

#from urllib.parse import urljoin
from domain.MongoCache import MongoCache

class UrlSpider(DomainBaseSpider):
    name = 'url'

    def __init__(self):
        self.mongoCache = MongoCache()

    def parse_item(self, response):
        l = ItemLoader(item=HouseItem(), response=response)
        l.add_value('url', response.url)
        l.add_xpath('address', '//div[@class="inner-wrap cfix"]/div[@class="left-wrap"]/h1/text()', MapCompose(str.strip))
        l.add_xpath('title', '//div[@class="inner-wrap cfix"]/div[@class="left-wrap"]/span/text()', MapCompose(str.strip))

        flines = response.xpath('//div[@class="inner-wrap cfix"]/div[@class="listing-features alt"]/span//span[@class="copy"]/descendant::text()').extract()
        flines = [line.strip() for line in flines]
        flines = list(filter(lambda a : a not in ['', '\n'], flines))

        feature_name = {'Bed', 'Bath', 'Parking'}
        for i,line in enumerate(flines):
            line = line.replace('s', '')
            if line in feature_name:
                value = 0
                try:
                    value = int(flines[i - 1])
                except ValueError:
                    value = 0
                l.add_value(line, value)

        #l.add_xpath('desc', '//*[@id="main"]/div/div/p/text()')
        desc = []
        desc_xpaths = ['//div[@id="description"]//text()',\
            '//div[@class="property-description truncate_textblock"]//text()']
        
        for xpath in desc_xpaths:
            desc = response.xpath(xpath).extract()
            if len(desc) > 0:
                break
            
        def stripjoin(lines, skip = ''):
            return skip.join(list(filter(lambda a : a not in ['', '\n'], [line.strip() for line in lines])))

        l.add_value('desc', stripjoin(desc, '\n'))
        l.add_xpath('type', '//*[@id="main"]/div/div/ul/li/strong/text()', MapCompose(str.strip))
        
        agetn_co = response.xpath('//a[@class="name track-agent-details"]/text()').extract()
        if len(agetn_co) > 0:
            l.add_value('agent_co',  agetn_co[0].strip())
        else:
            l.add_value('agent_co',  'unknown')
        
        return l.load_item()
