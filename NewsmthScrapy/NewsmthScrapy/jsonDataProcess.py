# -*- coding: utf-8 -*-
import json
import os
import random
import re
import time
import sys
from comm import *


class ParseJsonData():
    def readJsonFile(self, name):
        #start_time_str = time.strftime("%Y-%m-%d")
        start_time_str =getYesterDay()
        files_dir = "NewsmthScrapy/files/"+start_time_str
        if not os.path.isdir(files_dir):
            os.makedirs(files_dir)
        json_file = open(name)
        datas = json.load(json_file)
        json_file.close()
        for data in datas:
            if data.get('url') and data.get('time') and data.get('title') and data.get('content'):
                if data.get('title', "No Title") == "No Title":
                    print(data)
                elif len(''.join(data['content'])) > 50:
                    title = re.sub(r'[、，：；。？！“”‘’《》→（）／【】［］￥—|〡－\.\,\;\?\/\\\<\>\(\)\[\]\{\}\!\~`@#\$\%\^\&\*\_\+\-\=\:]','',data['title'][0])\
                        .replace(' ','').replace(' ','').replace(u'求职信息发布JobPost版北大未名BBS','').replace(u'饮水思源','')
                    url = re.sub(r'[、，：；。？！“”‘’《》→（）／【】￥—|〡－\.\,\;\?\/\\\<\>\(\)\[\]\{\}\!\~`@#\$\%\^\&\*\_\+\-\=\:]','.',data['url'][0])
                    #print("%s:%s" %(title,url))
                    fname = start_time_str+"="+url+"#"+title
                    content = data['title'][0] + '\n'.join(data['content'])
                    file = open(files_dir +"/"+ fname, 'w')
                    file.write(content)
                    file.close()
                else:
                    print("Too short to the file:" + data['url'][0])


if __name__ == "__main__":
    jsonData = ParseJsonData()
    files = os.listdir('NewsmthScrapy/output')
    for file in files:
        print("Start processing the json file:%s" %file)
        jsonData.readJsonFile('NewsmthScrapy/output/%s' %file)
        print("The json file has been processed.")

