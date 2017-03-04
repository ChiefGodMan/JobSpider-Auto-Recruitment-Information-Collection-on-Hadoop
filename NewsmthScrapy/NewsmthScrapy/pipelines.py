# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import signals
from NewsmthScrapy.items import NewsmthscrapyItem
from twisted.enterprise import adbapi
from scrapy.exporters import XmlItemExporter,CsvItemExporter,JsonItemExporter

class NewsmthscrapyPipeline(object):
    #def __init__(self):
    #    self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.file = open('NewsmthScrapy/output/%s_output.json' % spider.name, 'wb')
        #self.files[spider] = file
        #self.expoter = XmlItemExporter(file,encoding='utf-8')
        #self.expoter = CsvItemExporter(file,encoding='utf-8')
        self.expoter = JsonItemExporter(self.file, encoding='utf-8')
        self.expoter.start_exporting()

    def spider_closed(self, spider):
        self.expoter.finish_exporting()
        #file = self.files.pop(spider)
        self.file.close()

    def process_item(self, item, spider):
        self.expoter.export_item(item=item)
        return item


class YssyscrapyPipeline(object):

    @classmethod
    def from_crawler(cls,crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.file = open('NewsmthScrapy/output/%s_output.json' % spider.name, 'wb')
        self.expoter = JsonItemExporter(self.file, encoding='utf-8')
        self.expoter.start_exporting()

    def spider_closed(self, spider):
        self.expoter.finish_exporting()
        #file = self.files.pop(spider)
        self.file.close()

    def process_item(self, item, spider):
        self.expoter.export_item(item=item)
        return item