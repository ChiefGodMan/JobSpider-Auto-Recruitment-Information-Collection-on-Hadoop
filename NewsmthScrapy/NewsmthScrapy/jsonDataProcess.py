# -*- coding: utf-8 -*-
import json
import os
import random
import re
import time
import sys
#reload(sys)
#sys.setdefaultencoding("utf-8")


class ParseJsonData():
    def readJsonFile(self, name):
        start_time_str = time.strftime("%Y-%m-%d")
        files_dir = "NewsmthScrapy/files/"+start_time_str
        if not os.path.isdir(files_dir):
            os.makedirs(files_dir)
        json_file = open(name)
        datas = json.load(json_file)
        json_file.close()
        start_time_str = time.strftime("%Y-%m-%d")
        for data in datas:
            if data.get('url') and data.get('time') and data.get('title') and data.get('content'):
                if data.get('title', "No Title") == "No Title":
                    print(data)
                elif len(''.join(data['content'][5:])) > 50:
                    title = re.sub(r'[、，：；。？！“”‘’《》→（）【】￥—|〡－\.\,\;\?\/\\\<\>\(\)\[\]\{\}\!\~`@#\$\%\^\&\*\_\+\-\=\:]','',data['title'][0]).replace(' ','')
                    print(title)
                    fname = start_time_str+"="+data['url'][0].replace(':', '-').replace('/','+')+"#"+title
                    content = data['title'][0] + "\n\n" + '\n'.join(data['content'][5:])
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

