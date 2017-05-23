# -*- coding: utf-8 -*-
import scrapy
import time
import sys
import re
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.linkextractors import LinkExtractor
from NewsmthScrapy.items import JobScrapyItem
from scrapy.spiders import CrawlSpider
from scrapy.loader import ItemLoader
from NewsmthScrapy.comm import *

"""This spider aiming at crawling the www.newsmth.net forum career-campus article
    author: ailias
"""


class PkuSpider(scrapy.Spider):
    name = "PkuSpider"
    allowed_domains = ["bbs.pku.edu.cn"]
    start_urls = ['https://bbs.pku.edu.cn/v2/thread.php?bid=845&mode=topic']
    link_extractor = {
        'page_next': LinkExtractor(allow=r'\?bid=845&mode=topic(&page=\d)?'),
        'content': LinkExtractor(allow=r'post-read\.php\?bid=845&threadid=\d+$')
    }
    _x_query = {
        'title': "//title/text()",
        'poster': "//p[@class='username']/a/text()",
        'time': "//span[@class='title']/span/text()",
        'content': "//div[@class='content']"
    }
    crawl_page_num =5
    next_page_dict = dict()  # save the next_page number

    def parse(self, response):
        for link in self.link_extractor['page_next'].extract_links(response):
            page_num = re.search(r'\d{1}$', link.url)
            if page_num ==None :
                page_num=1
            else:
                page_num =page_num.group(0)
            if len(self.next_page_dict) < self.crawl_page_num and self.next_page_dict.get(page_num) == None:
                print(link.url)
                yield Request(url=link.url, callback=self.parse_page)
                self.next_page_dict[page_num] = page_num
            # else:
            #     print("full")

    def parse_page(self, response):
        for link in self.link_extractor['page_next'].extract_links(response):
            page_num = re.search(r'\d{1}$', link.url)
            if page_num ==None :
                page_num=1
            else:
                page_num =page_num.group(0)
            if len(self.next_page_dict) < self.crawl_page_num and self.next_page_dict.get(page_num) == None:
                print(link.url)
                yield Request(url=link.url, callback=self.parse_page)
                self.next_page_dict[page_num] = page_num
            # else:
            #     print("full")
        for link in self.link_extractor['content'].extract_links(response):
            yield Request(url=link.url, callback=self.parse_content)

    def parse_content(self, response):
        pku_loader = ItemLoader(item=JobScrapyItem(), response=response)
        # search the date string
        pub_time_str = response.xpath(self._x_query['time']).re(r'\d{4}-\d{2}-\d{2}')
        pub_time_str=pub_time_str[0]
        #print(pub_time_str)
        content=''
        for p in response.xpath(self._x_query['content']):
            for text in p.xpath('//p/text()').extract():
                content+=text.replace('\n',' ')
                content+='\n'
        #print(content)
        # specify the end scrapy time string
        start_time_str=getYesterDay()
        #start_time_str = time.strftime("%Y-%m-%d")
        #start_time_str = time.strftime("%Y-%m-%d", (2017, 3, 3, 0, 0, 0, 0, 0, 0))
        if pub_time_str == start_time_str:  # if the publish time is bigger than end_time, save the content.
            url = str(response.url)
            print("save " + pub_time_str + "@" + url)
            pku_loader.add_value('url', url)
            pku_loader.add_xpath('title', self._x_query['title'])
            pku_loader.add_xpath('poster', self._x_query['poster'])
            # pku_loader.add_xpath('content', self._x_query['content'])
            pku_loader.add_value('content',content)
            pku_loader.add_value('time', pub_time_str)
            return pku_loader.load_item()
        # else:
        #     print(pub_time_str)


if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl(PkuSpider)
    process.start()
