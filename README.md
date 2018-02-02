# domain_au_scrapy
Crawl domain.com.au everyday using scrapy + mongodb + schedule.
 1. It is based on scrapy framework.
 2. It uses mongodb as database.
 3. It uses python schedule to simplely follow a daily routine.
 4. It has been configed friendly to web server since 1 request per second.

## About timing
The first crawling takes up **6 hours** since there are average 15000+ house on sell on domain.com.au.
The followed crawling will take **only 1 hour** since no need to crawl every detail page.

## For what
This project is helpful to providing real estate market informations in Sydney for house seeker.
Also it is good data source for data science studying.
