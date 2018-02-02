import os
import sched
import time
from datetime import datetime, timedelta
from domain.MongoCache import timer_func

schedule = sched.scheduler(time.time, time.sleep)

def crawlHouse():
    os.system('rm log')
    #os.system('python3 -m scrapy crawl url --logfile')
    timer_func('dl_domain', os.system, 'python3 -m scrapy crawl url --logfile log')

def perform_crawl(hour, min):
    now = datetime.now()
    onedayafter = timedelta(days=1)
    tmp = now + onedayafter
    tormorrow = datetime(tmp.year, tmp.month, tmp.day, hour, min)
    tonext = tormorrow - now
    inc = tonext.seconds + tonext.days * 86400
    schedule.enter(inc, 0, perform_crawl, (hour, min))

    crawlHouse()

    print('next crawl in %d secs' % inc)
    print(tormorrow)

def timing_crawl(hour, min):
    # 第一次是在当天的23:50
    now = datetime.now()
    tormorrow = datetime(now.year, now.month, now.day, hour, min)
    tonext = tormorrow - now
    sec = tonext.seconds

    schedule.enter(sec, 0, perform_crawl, (hour, min))
    #crawlHouse()

    print('next crawl in %d secs' % sec)
    print(tormorrow)

    schedule.run()

if __name__ == "__main__":
    timing_crawl(1, 00)
