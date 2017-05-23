import re
import time
import time
from datetime import datetime,timedelta

# def matchUrl():
#     #re_pat = 'http://www\.newsmth\.net/nForum/\#\!board/Career\_Campus\?p=\d+'
#     re_pat = r'[a-zA-Z]{3}\s+[a-zA-Z]{3}\s+\d{1,2}\s+\d{2}\:\d{2}\:\d{2}\s+\d{4}'
#     match_obj = re.search(re_pat, '发信站: 水木社区 (Sat Jan  7 14:15:01 2017), 站内  ')
#     print(match_obj.group(0))
#
#
# matchUrl()
#
#
# date_str="Sat Jan  7 14:15:01 2017"
# pub_time =time.strptime(date_str,"%a %b %d %H:%M:%S %Y")
# if time.localtime() >pub_time:
#     print("less")



# def yssyDate():
#     re_pat=r'\d{4}年\d{2}月\d{2}日'
#     match_obj = re.search(re_pat, '发信人: tristas(tristas), 信区: JobInfo 标  题: 蚂蚁集团-OceanBase团队2017年校招 发信站: 饮水思源 (2017年03月03日19:02:59 星期五)')
#     print(match_obj.group(0))
#
# yssyDate()
# start_time_str = time.strftime("%Y-%m-%d",(2017,3,3,0,0,0,0,0,0))
# print(start_time_str)

#
# def NjuDate():
#     re_pat = r'[a-zA-Z]{3}\s+[a-zA-Z]{3}\s+\d{1,2}\s+\d{2}\:\d{2}\:\d{2}\s+\d{4}'
#     match_obj = re.search(re_pat, '发信站: 南京大学小百合站 (Thu Mar  2 22:37:41 2017)')
#     print(match_obj.group(0))
#     pub_time_str = match_obj.group(0).replace(u'\xa0', ' ')
#     # convert the publish string time to time type
#     pub_time = time.strptime(pub_time_str, "%a %b %d %H:%M:%S %Y")
#     pub_time_str = time.strftime("%Y-%m-%d", pub_time)
#     print(pub_time_str)
#
# NjuDate()


# def PkuDate():
#     re_pat = r'\d{4}-\d{2}-\d{2}'
#     match_obj = re.search(re_pat, '发表于2017-03-03 09:59:57 来自123.113.150.56')
#     print(match_obj.group(0))
#
# PkuDate()


# def PkuDate():
#     re_pat = r'/bbscon\?board=JobExpress&file=M.\d+.A&num=\d+'
#     match_obj = re.search(re_pat, 'http://bbs.nju.edu.cn/bbscon?board=JobExpress&file=M.1488465461.A&num=11966')
#     print(match_obj.group(0))
#
# PkuDate()


def getYesterDay():
    yesterday= datetime.now()-timedelta(1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    return yesterday_str

yes = getYesterDay()
print(yes)