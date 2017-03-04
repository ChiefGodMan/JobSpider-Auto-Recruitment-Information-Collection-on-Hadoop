This tutorial is tell you how to use the scrapy code:

1. change to the project root directory:
   $ cd /{path to }/NewsmthScrapy

2. run the scrapy spider(be careful that the spider name is the NewsmthScrapy/spiders/NewsmthSpider-->>NewsmthSpider class's name value):
   $ scrapy crawl NewsmthSpider
   the crawl output file is NewsmthScrapy/output/SpiderName.json

3. run jsonDataProcess py file to extract the output file to multiple files for each line.
   $ python3.5 NewsmthScrapy/jsonDataProcess.py

