# -*- coding: utf-8 -*-
import scrapy
import time
import sys
import re
#reload(sys)
#sys.setdefaultencoding("utf-8")
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.linkextractors import LinkExtractor
from NewsmthScrapy.items import NewsmthscrapyItem
from scrapy.spiders import CrawlSpider
from scrapy.loader import ItemLoader

"""This spider aiming at crawling the www.newsmth.net forum career-campus article
    author: ailias
"""


class NewsmthSpider(scrapy.Spider):
    name = "NewsmthSpider"
    allowed_domains = ["newsmth.net"]
    start_urls = ['http://www.newsmth.net/nForum/board/Career_Campus']
    link_extractor = {
        'page_next': LinkExtractor(allow=r'/nForum/board/Career\_Campus\?p=[12]?\d$'),
        'content': LinkExtractor(allow=r'/nForum/article/Career\_Campus/\d+$')
    }
    _x_query = {
        'title': "//title/text()",
        'poster': "//span[@class='a-u-name']/a/text()",
        'time': "//p/text()",
        'content': "//p/text()"
    }

    next_page_dict = dict() #save the next_page number

    def parse(self, response):
        for link in self.link_extractor['page_next'].extract_links(response):
            if len(self.next_page_dict) <5:
                print(link.url)
                page_num = re.search(r'\d{0,1}', link.url)
                yield Request(url=link.url, callback=self.parse_page)
                self.next_page_dict[page_num.group(0)] = page_num.group(0)
            else:
                print("full")

    def parse_page(self, response):
        for link in self.link_extractor['page_next'].extract_links(response):
            if len(self.next_page_dict) <5:
                print(link.url)
                page_num = re.search(r'\d{0,1}', link.url)
                yield Request(url=link.url, callback=self.parse_page)
                self.next_page_dict[page_num.group(0)] = page_num.group(0)
            else:
                print("full")
        for link in self.link_extractor['content'].extract_links(response):
            yield Request(url=link.url, callback=self.parse_content)

    def parse_content(self, response):
        newsmth_loader = ItemLoader(item=NewsmthscrapyItem(), response=response)
        # search the date string
        pub_time_str = response.xpath(self._x_query['time']) \
            .re(r'[a-zA-Z]{3}\s+[a-zA-Z]{3}\s+\d{1,2}\s+\d{2}\:\d{2}\:\d{2}\s+\d{4}')
        # replace the known string
        pub_time_str = pub_time_str[0].replace(u'\xa0', ' ')
        # convert the publish string time to time type
        pub_time = time.strptime(pub_time_str, "%a %b %d %H:%M:%S %Y")
        pub_time_str = time.strftime("%Y-%m-%d", pub_time)
        # specify the end scrapy time string
        start_time_str = time.strftime("%Y-%m-%d")
        # converet the end time to time type
        #start_time = time.strftime(start_time_str, "%Y %m %d %H:%M:%S")
        if pub_time_str >= start_time_str:  # if the publish time is bigger than end_time, save the content.
            url = str(response.url)
            print("save " + pub_time_str + "@" + url)
            newsmth_loader.add_value('url', url)
            newsmth_loader.add_xpath('title', self._x_query['title'])
            newsmth_loader.add_xpath('poster', self._x_query['poster'])
            newsmth_loader.add_xpath('content', self._x_query['content'])
            newsmth_loader.add_value('time', pub_time_str)
            return newsmth_loader.load_item()
        else:
            print(pub_time_str)

if __name__ =="__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl(NewsmthSpider)
    process.start()
