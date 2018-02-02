# domain_au_scrapy

## What is this
Crawl domain.com.au everyday using scrapy + mongodb + schedule.
 1. It is based on scrapy framework.
 2. It uses mongodb as database.
 3. It uses python schedule to simplely follow a daily routine.
 4. It has been configed friendly to web server since 1 request per second.
 5. It is written in python 3.x.

## Get Started
 To get started, you need:
 1. A running mongod service on your local(127.0.0.1)
 2. Change the username and password in /domain/MongoCache.py to fit your own case.
 3. For one time crawling, just run: 
  ```shell
  python3 runner.py
  ```
 4.For scheduled crawling:
  open scheduler.py, scroll to the bottom, change parameters in the call to timing_crawl
  ```python
  if __name__ == "__main__":
     timing_crawl(hour, min)
  ```
  Then,
  ```shell
  python3 scheduler.py
  ```

## About timing
The first crawling takes up **6 hours** since there are average 15000+ house on sell on domain.com.au.
The followed crawling will take **only 1 hour** since no need to crawl every detail page.

## For what
 - This project is helpful to providing real estate market informations in Sydney for house seekers.
 - it is good data source for data science studying.
 
## Analysis
 - A summary analysis based on this work can be [visited here](https://kownse.github.io/sydneyhouse)
   ![Analysis](https://kownse.github.io/img/sydney_house_o.png "screenshot of price analysis grouped by areas")
 - For more detailed analysis results, please contact me by [email(kownse@gmail.com)](mailto:kownse@gmail.com).
