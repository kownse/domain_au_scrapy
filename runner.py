from scrapy.cmdline import execute
import os

print(os.getcwd())
execute(['scrapy','crawl', 'url'])