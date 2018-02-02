# -*- coding: utf-8 -*-
import scrapy
import re
from scrapy.http import Request
from abc import ABCMeta, abstractmethod

class DomainBaseSpider(scrapy.Spider, metaclass=ABCMeta):
    name = 'base'
    allowed_domains = ['www.domain.com.au']
    seen = set()
    url_temp = 'https://www.domain.com.au/sale/{0}/?ssubs=1&page=1'
    areas = ['hornsby-nsw-2077',\
        'pymble-nsw-2073',\
        'macquarie-park-nsw-2113',\
        'baulkham-hills-nsw-2153', \
        'castle-hill-nsw-2154',\
        'parramatta-nsw-2150',\
        'dee-why-nsw-2099',\
        'narrabeen-nsw-2101',\
        'chatswood-nsw-2067',\
        'ryde-nsw-2112',\
        'castlecrag-nsw-2068',\
        'northbridge-nsw-2063',\
        'cremorne-nsw-2090',\
        'mosman-nsw-2088',\
        'northwood-nsw-2066', \
        'north-sydney-nsw-2060',\
        'cammeray-nsw-2062',\
        'gladesville-nsw-2111',\
        'marsfield-nsw-2122',\
        'epping-nsw-2121',\
        'forestville-nsw-2087',\
        'artarmon-nsw-2064',\
        'mosman-nsw-2088', \
        'st-leonards-nsw-2065', \
        'parramatta-nsw-2150',\
        'terrey-hills-nsw-2084',\
        'kurraba-point-nsw-2089',\
        'balgowlah-nsw-2093',\
        'east-ryde-nsw-2113',\
        'davidson-nsw-2085',\
        'st-ives-nsw-2075',\
        'st-ives-chase-nsw-2075',\
        'wahroonga-nsw-2076',\
        'manly-nsw-2095',\
        'brookvale-nsw-2100',\
        'mona-vale-nsw-2103',\
        'avalon-beach-nsw-2107',\
        'mount-kuring-gai-nsw-2080',\
        'berowra-heights-nsw-2082',\
        'glenhaven-nsw-2156',\
        'bella-vista-nsw-2153',\
        'rouse-hill-nsw-2155',\
        'quakers-hill-nsw-2763',\
        'blacktown-nsw-2148',\
        'mount-druitt-nsw-2770',\
        'penrith-nsw-2750',\
        'cranebrook-nsw-2749',\
        'marsden-park-nsw-2765',\
        'londonderry-nsw-2753',\
        'richmond-nsw-2753',\
        'riverstone-nsw-2765',\
        'kurrajong-nsw-2758',\
        'blaxland-nsw-2774',\
        'bossley-park-nsw-2176',\
        'auburn-nsw-2144',\
        'lidcombe-nsw-2141',\
        'merrylands-nsw-2160',\
        'sydney-nsw-2000',\
        'strathfield-nsw-2135',\
        'ashfield-nsw-2131',\
        'bondi-nsw-2026',\
        'randwick-nsw-2031',\
        'eastlakes-nsw-2018',\
        'mascot-nsw-2020',\
        'lakemba-nsw-2195',\
        'bankstown-nsw-2200',\
        'fairfield-nsw-2165',\
        'cabramatta-nsw-2166',\
        'liverpool-nsw-2170',\
        'badgerys-creek-nsw-2555',\
        'miranda-nsw-2228',\
        'cronulla-nsw-2230',\
        'surry-hills-nsw-2010',\
        'little-bay-nsw-2036',\
        'maroubra-nsw-2035',\
        'kurnell-nsw-2231',\
        'hurstville-nsw-2220',\
        'kogarah-nsw-2217',\
        'newtown-nsw-2042',\
        'earlwood-nsw-2206',\
        'punchbowl-nsw-2460',\
        'chester-hill-nsw-2162',\
        'smithfield-nsw-2164',\
        'seven-hills-nsw-2147',\
        'wetherill-park-nsw-2164',\
        'rose-bay-nsw-2029',\
        'watsons-bay-nsw-2030',\
        'kemps-creek-nsw-2178',\
        'luddenham-nsw-2745',\
        'wallacia-nsw-2745',\
        'greendale-nsw-2550',\
        'greendale-nsw-2745',\
        'werombi-nsw-2570',\
        'theresa-park-nsw-2570',\
        'the-oaks-nsw-2570',\
        'prestons-nsw-2170',\
        'austral-nsw-2179',\
        'leppington-nsw-2179',\
        'padstow-nsw-2211',\
        'sydney-olympic-park-nsw-2127',\
        'erskine-park-nsw-2759',\
        'menai-nsw-2234',\
        'lucas-heights-nsw-2234',\
        'heathcote-nsw-2233',\
        'campbelltown-nsw-2560',\
        'narellan-nsw-2567',\
        'ingleburn-nsw-2565',\
        'jamisontown-nsw-2750',\
        'wollongong-nsw-2500',\
        'bulli-nsw-2516',\
        'port-kembla-nsw-2505',\
        'gosford-nsw-2250',\
        'cattai-nsw-2756',\
        'cecil-hills-nsw-2171',\
        'maraylya-nsw-2765',\
        'maroota-nsw-2756',\
        'lower-portland-nsw-2756',\
        'forest-glen-nsw-2157',\
        'glenworth-valley-nsw-2250',\
        'mardi-nsw-2259'\
        ]
        
    def __del__(self):
        self.log('spider end')

    def start_requests(self):
        for su in [self.url_temp.format(area) for area in self.areas]:
            yield Request(su)

    def getPageFromUrl(self, url):
        page = 1
        try:
            page = int(re.search(r'page=([0-9]+)', url).group(1))
        except:
            self.log('error when get page from ' + url)
        return page
        

    def getMaxPage(self, pages_urls):
        maxpage = 1
        for url in pages_urls:
            page = self.getPageFromUrl(url)
            maxpage = page if page > maxpage else maxpage
        return maxpage
    
    @abstractmethod
    def parse_item(self, response):
        pass

    def parse(self, response):
        pages_urls = response.xpath('//div[@class="paginator__pages"]/a/@href').extract()
        maxpage = self.getMaxPage(pages_urls)
        curpage = self.getPageFromUrl(response.url)
        
        
        if curpage < maxpage:
            self.log("request page" + str(curpage + 1))
            yield Request(response.url[:-len(str(curpage))] + str(curpage+1))

        # show progress            
        tags = re.findall(r'([a-z\-0-9]+)/\?ssubs=', response.url)
        if len(tags) > 0:
            index = self.areas.index(tags[0])
            print('[%s]\t\t[%d/%d]\t[%d/%d]' % (tags[0], index, len(self.areas), curpage, maxpage))

        nodes = response.xpath('//div[@class="listing-result__right"]')
        for n in nodes:
            url = n.xpath('a/@href').extract()[0]
            todaydownloaded = self.mongoCache.checkTodayDownload(url)
            #downloaded = self.mongoCache.checkUrlDownloaded(url)
            hasagent = self.mongoCache.checkHasAgentCo(url)
            
            if todaydownloaded:
                self.log('today downloaded: ' + url)
                continue
            elif hasagent:
            
                title = n.xpath('div[1]/p[1]/text()').extract()
                title = [line.strip() for line in title]
                title = list(filter(lambda a : a not in ['', '\n'], title))
                title = ''.join(title)
                
                if len(title) == 0:
                    print('error when get title')
                else:
                    self.mongoCache.updateTitleAndDate(url, title)
            else:
                h = hash(url)
                if h not in self.seen:
                    self.seen.add(h)
                    self.log(url)
                    yield Request(url, callback=self.parse_item)
                    self.log('new url: ' + url)
                else:
                    self.log('seen: ' + url)   
