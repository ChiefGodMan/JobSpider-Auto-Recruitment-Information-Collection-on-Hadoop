# -*- coding: utf-8 -*-
import scrapy
import re
import time
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.linkextractors import LinkExtractor
from NewsmthScrapy.items import NewsmthscrapyItem
from NewsmthScrapy.items import YssyscrapyItem
from scrapy.spiders import CrawlSpider
from scrapy.loader import ItemLoader


class YssySpider(scrapy.Spider):
    name = "YssySpider"
    allowed_domains = ["bbs.sjtu.edu.cn"]
    start_urls = ['https://bbs.sjtu.edu.cn/bbsdoc,board,JobInfo.html']
    link_extractor = {
        'page_next': LinkExtractor(allow='/bbsdoc,board,JobInfo,page,\d{4,}\.html'),
        'content': LinkExtractor(allow='/bbscon,board,JobInfo,file,M\.\d+\.A\.html')
    }
    _x_query = {
        'title': "//title/text()",
        'time':"//pre/text()",
        'poster': "//pre/a/text()[0]",
        'content': "//pre/font/text()",
    }

    next_page_dict=dict()

    def parse(self, response):
        for link in self.link_extractor['page_next'].extract_links(response):
            if len(self.next_page_dict) < 1:
                print(link.url)
                page_num = re.search(r'\d{4,}', link.url)
                yield Request(url=link.url, callback=self.parse_page)
                self.next_page_dict[page_num.group(0)]=page_num.group(0)
            else:
                print("full")

    def parse_page(self, response):
        for link in self.link_extractor['page_next'].extract_links(response):
            if len(self.next_page_dict) < 1:
                print(link.url)
                page_num = re.search(r'\d{4,}', link.url)
                yield Request(url=link.url, callback=self.parse_page)
                self.next_page_dict[page_num.group(0)] = page_num.group(0)
            else:
                print("full")
        for link in self.link_extractor['content'].extract_links(response):
            #print(link.url)
            yield Request(url=link.url, callback=self.parse_content)

    def parse_content(self, response):
        yssy_loader = ItemLoader(item=YssyscrapyItem(), response=response)
        pub_time_str = response.xpath(self._x_query['time']) \
            .re(u'\d{4}年\d{2}月\d{2}日')# because there exist chinese, so use unicode encoding
        pub_time_str = pub_time_str[0].replace(u'年', '-').replace(u'月', '-').replace(u'日', '')
        # start_time_str = time.strftime("%Y-%m-%d")
        start_time_str = time.strftime("%Y-%m-%d", (2017, 3, 3, 0, 0, 0, 0, 0, 0))
        if pub_time_str >= start_time_str:  # if the publish time is bigger than end_time, save the content.
            url = str(response.url)
            print("save " + pub_time_str + "@" + url)
            yssy_loader.add_value('url', url)
            yssy_loader.add_xpath('title', self._x_query['title'])
            yssy_loader.add_xpath('poster', self._x_query['poster'])
            yssy_loader.add_xpath('content', self._x_query['content'])
            yssy_loader.add_value('time', pub_time_str)
            return yssy_loader.load_item()
        else:
            print(pub_time_str)