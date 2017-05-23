# -*- coding: utf-8 -*-
import scrapy
import re
import time
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.linkextractors import LinkExtractor
from NewsmthScrapy.items import JobScrapyItem
from scrapy.spiders import CrawlSpider
from scrapy.loader import ItemLoader
from NewsmthScrapy.comm import *

class YssySpider(scrapy.Spider):
    name = "NjuSpider"
    allowed_domains = ["bbs.nju.edu.cn"]
    start_urls = ['http://bbs.nju.edu.cn/bbsdoc?board=JobExpress']
    link_extractor = {
        'page_next': LinkExtractor(allow=r'/bbsdoc\?board=JobExpress\&start=\d+&type=doc'),
        'content': LinkExtractor(allow=r'/bbscon\?board=JobExpress\&file=M\.\d+\.A&num=\d+')
    }
    _x_query = {
        'title': "//td",
        'time': "//td",
        'poster': "//td",
        'content': "//td",
    }
    crawl_page_num = 1
    next_page_dict = dict()

    def parse(self, response):
        for link in self.link_extractor['page_next'].extract_links(response):
            page_num = re.search(r'\d+', link.url)
            if len(self.next_page_dict) < self.crawl_page_num and self.next_page_dict.get(page_num) == None:
                print(link.url)
                yield Request(url=link.url, callback=self.parse_page)
                self.next_page_dict[page_num.group(0)] = page_num.group(0)
            else:
                print("full")

    def parse_page(self, response):
        for link in self.link_extractor['page_next'].extract_links(response):
            page_num = re.search(r'\d+', link.url)
            if len(self.next_page_dict) < self.crawl_page_num and self.next_page_dict.get(page_num) == None:
                print(link.url)
                yield Request(url=link.url, callback=self.parse_page)
                self.next_page_dict[page_num.group(0)] = page_num.group(0)
            else:
                print("full")
        for link in self.link_extractor['content'].extract_links(response):
            # print(link.url)
            yield Request(url=link.url, callback=self.parse_content)

    def parse_content(self, response):
        nju_loader = ItemLoader(item=JobScrapyItem(), response=response)
        # search the date string
        pub_time_str = response.xpath(self._x_query['time']) \
            .re(r'[a-zA-Z]{3}\s+[a-zA-Z]{3}\s+\d{1,2}\s+\d{2}\:\d{2}\:\d{2}\s+\d{4}')
        # replace the known string
        pub_time_str = pub_time_str[0].replace(u'\xa0', ' ')
        # convert the publish string time to time type
        #pub_time_str = time.strftime("%Y-%m-%d")
        pub_time = time.strptime(pub_time_str, "%a %b %d %H:%M:%S %Y")
        pub_time_str = time.strftime("%Y-%m-%d", pub_time)

        # start_time_str = time.strftime("%Y-%m-%d")
        #start_time_str = time.strftime("%Y-%m-%d", (2017, 3, 3, 0, 0, 0, 0, 0, 0))
        #start_time_str = time.strftime("%Y-%m-%d")
        start_time_str= getYesterDay()
        if pub_time_str == start_time_str:  # if the publish time is bigger than end_time, save the content.
            url = str(response.url)
            print("save " + pub_time_str + "@" + url)
            nju_loader.add_value('url', url)
            title = response.xpath('title').re(u'标\s+题:\s+$')
            print("title")
            print(title)
            nju_loader.add_value('title',title)
            #nju_loader.add_xpath('title', title)
            for p in response.xpath(self._x_query['poster']):
                poster = p.xpath('/text()').extract()
                print("poster")
                print(poster)
                nju_loader.add_value('poster',poster)
                break
            #nju_loader.add_xpath('poster', self._x_query['poster'])
            content = ''
            for p in response.xpath(self._x_query['content']):
                for text in p.extract():
                    content += text
                    #content += '\n'
            print("content")
            print(content)
            # nju_loader.add_xpath('content', self._x_query['content'])
            nju_loader.add_value('content', content)
            nju_loader.add_value('time', pub_time_str)
            return nju_loader.load_item()
        else:
            print(pub_time_str)
