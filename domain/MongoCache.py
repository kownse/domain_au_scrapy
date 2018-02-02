import pickle
import zlib
import pymongo
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from collections import defaultdict

DB_HOUSE_DAILY = 'house_daily'
DB_DESC = 'descs'
DB_TITLE_HISTORY = 'title_history'
DB_GEOLOC = 'geoloc'

class MongoCache:
    __instance = None
    __inited = False
    
    def __new__(cls, ip = '127.0.0.1', db = 'house'):
        if MongoCache.__instance is None:
            MongoCache.__instance = object.__new__(cls)
        return MongoCache.__instance
    
    def __init__(self, ip='127.0.0.1', db = 'house'):   
        if MongoCache.__instance.__inited == True:
            return
        
        MongoCache.__instance.__inited = True
        self.curdate = str(datetime.now().date())
        mongo_url = 'mongodb://kownse:123nb411@%s:27017/%s' % (ip, db)
        self.client = MongoClient(mongo_url)
        self.db = self.client[db]
        self.lastDaily = pd.DataFrame(list(self.getAllHouseDaily()))
        
        self.bulkCntDict = defaultdict(int)
        self.bulkDict = {}
        
        self.startOrderedBulk(DB_HOUSE_DAILY)
        self.startOrderedBulk(DB_DESC)
        self.startOrderedBulk(DB_TITLE_HISTORY)
        self.startOrderedBulk(DB_GEOLOC)
        self.dictcnt = defaultdict(int)
        
        if len(self.lastDaily) > 0:
            self.lastDaily['url_hash'] = self.lastDaily['url'].map(lambda x : hash(x))
            self.lastDaily.set_index('url_hash', inplace = True)
            if 'agent_co' not in self.lastDaily.columns:
                self.lastDaily['agent_co'] = np.NaN
            self.lastDaily = self.lastDaily[['title','date','update_date', 'agent_co']]
        else:
            self.lastDaily = pd.DataFrame(columns=['title','date','update_date', 'agent_co'])
            
    
        

    def __del__(self):
        self.client.close()
        
    def startOrderedBulk(self, db):
        print('renew bulk: ' + db)
        self.bulkDict[db] = self.db[db].initialize_ordered_bulk_op()
        self.bulkCntDict[db] = 0

    def excuteBulk(self, bulk, cnt, name = ''):
        print(name + ' bulkcnt: ' + str(cnt))
        if bulk is not None and cnt > 0:
            try:
                result = bulk.execute()
                print(result)
            except BulkWriteError as bwe:
                print(bwe.details)
                
    def checkExcuteBulk(self, db):
        if self.bulkCntDict[db] >= 1000:
            self.excuteBulk(self.bulkDict[db], self.bulkCntDict[db], db)
            self.startOrderedBulk(db)

    def increaseCheckBulk(self, db):
        self.bulkCntDict[db] += 1
        self.checkExcuteBulk(db)
                
    def excuteAllBulk(self):
        for key in [DB_HOUSE_DAILY, DB_DESC, DB_TITLE_HISTORY, DB_GEOLOC]:
            self.excuteBulk(self.bulkDict[key], self.bulkCntDict[key], key)
        print(self.dictcnt.items())
    
    def checkDictValue(self, dic, name, default):
        return dic[name] if name in dic else default
    
    def checkDownloadedByDate(self, url, date):
        if self.checkUrlDownloaded(url) and\
           self.lastDaily.loc[hash(url), 'update_date'] == date:
            return True
        else:
            return False

    def checkTodayDownload(self, url):
        return self.checkDownloadedByDate(url, self.curdate)
    
    def checkUrlDownloaded(self, url):
        return hash(url) in self.lastDaily.index
    
    def checkHasAgentCo(self, url):
        if self.checkUrlDownloaded(url):
            agent_co = self.lastDaily.loc[hash(url), 'agent_co']
            return not pd.isnull(agent_co) and len(agent_co) > 0
        return False
    
    def setHouseDescToDB(self, originalInfo):
        if 'desc' in originalInfo:
            desc = {'url':originalInfo['url'], 'desc':zlib.compress(pickle.dumps(originalInfo['desc']))}
            self.bulkDict[DB_DESC].insert(desc)
            self.increaseCheckBulk(DB_DESC)
    
    def updateTitleAndDate(self, url, title):
        url_hash = hash(url)
        rec = self.lastDaily.loc[url_hash]
        len_old = len(rec['title'])
        len_new = len(title)
        if len_new == 0:
            #print('new title empty, drop')
            
            self.dictcnt['new title empty'] += 1
            return
        elif len_old == 0:
            #print('old title empty')
            rec['title'] = title
            self.bulkDict[DB_HOUSE_DAILY].find({'url':url}).update({'$set': {'title':title, 'update_date':self.curdate}})
            
            self.dictcnt['old title empty'] += 1
        elif rec['title'] != title:
            self.bulkDict[DB_TITLE_HISTORY].insert({'url':url, 'title':rec['title'], 'update_date':rec['update_date']})
            self.increaseCheckBulk(DB_TITLE_HISTORY)
            
            rec['title'] = title
            self.bulkDict[DB_HOUSE_DAILY].find({'url':url}).update({'$set': {'title':title, 'update_date':self.curdate}})
            self.increaseCheckBulk('house_daily')
            self.increaseCheckBulk(DB_HOUSE_DAILY)
            
            #print('title updated: ' + title)
            self.dictcnt['title updated'] += 1
        else:
            self.bulkDict[DB_HOUSE_DAILY].find({'url':url}).update({'$set': {'update_date':self.curdate}})
            self.increaseCheckBulk(DB_HOUSE_DAILY)
            
            self.dictcnt['update date'] += 1
        rec['update_date'] = self.curdate
    
    def setHouseInfoToDB(self, dbInfo, originalInfo, tardate):
        if self.checkDownloadedByDate(dbInfo['url'], tardate) is True:
            print('already: ' + dbInfo['url'])
            self.dictcnt['downloaded in setHouse'] += 1
        else:
            self.bulkDict[DB_HOUSE_DAILY].find({'url':dbInfo['url']}).upsert().update({'$set': dbInfo})
            self.increaseCheckBulk(DB_HOUSE_DAILY)
            
            if self.checkUrlDownloaded(dbInfo['url']) is False:
                self.setHouseDescToDB(originalInfo)
                self.dictcnt['add desc'] += 1
            
            self.lastDaily.loc[hash(dbInfo['url'])] = {'title':dbInfo['title'],\
                               'date':dbInfo['date'],\
                               'update_date':dbInfo['update_date'],\
                               'agent_co':dbInfo['agent_co']}
            self.dictcnt['setHouse'] += 1
                
    def setLoc(self, address, lat, lon):
        self.bulkDict[DB_GEOLOC].find({'address':address}).upsert().update({'$set':{'address':address, 'lat':lat, 'lon': lon}})
        self.increaseCheckBulk(DB_GEOLOC)
        self.dictcnt['add location'] += 1

    def setHouse(self, webinfo):
        url = self.checkDictValue(webinfo, 'url', '')
        url_hash = hash(url)
        title = self.checkDictValue(webinfo, 'title', '')
        if len(url) <= 0:
            self.dictcnt['url empty'] += 1
            return
        
        info = {}
        if self.checkHasAgentCo(url):
            rec = self.lastDaily.loc[url_hash]
            if rec['update_date'] == self.curdate:
                #print('redirect to already: ' + url)
                self.dictcnt['redirect already'] += 1
                return
            
            self.updateTitleAndDate(url, title)
        else:
            info['date'] = self.curdate
            info['url'] = url
            info['title'] = title
            info['Beds'] = self.checkDictValue(webinfo, 'Bed', 0)
            info['Bath'] = self.checkDictValue(webinfo, 'Bath', 0)
            info['Parking'] = self.checkDictValue(webinfo, 'Parking', 0)
            info['address'] = self.checkDictValue(webinfo, 'address', '')
            info['type'] = self.checkDictValue(webinfo, 'type', '')
            info['update_date'] = self.curdate
            info['agent_co'] = self.checkDictValue(webinfo, 'agent_co', '')
    
            self.setHouseInfoToDB(info, webinfo, self.curdate)
    
    def setHouseFromCSV(self, dfinfo):
        info = dict(dfinfo[['Bath', 'Beds', 'Parking', 'address', 'date', \
                            'title', 'type', 'url']])
        info['date'] = str(info['date'].date()) if not type(info['date'])==str else info['date']
        self.setHouseInfoToDB(info, dfinfo, info['date'])
        
    def getAllHouseDaily(self):
        return self.db.house_daily.find().sort('date', pymongo.DESCENDING)
    
    def getAllGeoLocation(self):
        return self.db.geoloc.find()
    
    def getTitleHistory(self):
        return self.db.title_history.find()
    
    def setOperationLog(self, tag, date, duration):
        info = {'_id': date, 'tag':tag, 'duration':duration}
        self.db.oplogs.insert(info)
        


def timer_func(tag, func, param=None):
    begin = datetime.now()
    if param is not None:
        func(param)
    else:
        func()

    end = datetime.now()
    last = end - begin
    print('%s takes %d secs, end at %s' % (tag, last.seconds, end))
    cache = MongoCache()
    cache.setOperationLog(tag, begin, last.seconds)