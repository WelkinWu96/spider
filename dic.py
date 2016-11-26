# -*- coding: UTF-8 -*-
import urllib2
import re
import Queue
from bs4 import BeautifulSoup
import bs4
import chardet

import MySQLdb
from datetime import datetime

dictionary_DY=r"https://www.douyu.com/directory"
dictionary_PANDA=r"http://www.panda.tv/cate"
dictionary_HUYA=r"http://www.huya.com/g"
dictionary_QM=r"http://www.quanmin.tv/game"
arr=[]
pre_time=pre_time=datetime.now()
pre_time_str=pre_time.strftime('%y-%m-%d %H:%M:%S')

#
#获取直播页数方法
#

def getpage_num_dy(url):
    res=urllib2.urlopen(url)#打开直播列表页
    html=res.read()
    a=re.compile(r"count:\s\"\d+\"")#网页脚本中以"COUNT：数字"的方式返回页数
    num_pattern=re.compile(r"\d+")
	
    
    s= a.findall(html)
    if s:
        page_num=num_pattern.findall(s[0])
        return int(page_num[0])
    else:
        return 0
	#若找到符合正则的返回页数
	#若找不到返回0

#
#获取所有索引下的直播人数方法
#	
def getdic_live_dy():
    res=urllib2.urlopen(dictionary_DY)
    html=res.read()
    soup=BeautifulSoup(html,"html.parser")#使用parser解析器
    # 生成队列将各目录下直播信息逐一获取
    dic_que=Queue.Queue()

    for url in soup.find_all("a",class_="thumb"):
        dic={"name":unicode(url.p.string).encode("utf-8"),"url":url['href'].encode("utf-8")}
        dic_que.put(dic)
    print "-----------读取目录成功  正在爬取内容----------"
    # 目录页中目录信息存于class="thumb"的超链接标签中 获取目录名字及URL存入队列
    
    # 直至队列为空↓
    while not dic_que.empty():
        dic2=dic_que.get()
        dic_url="http://www.douyu.com"+dic2['url']
        max_live_page=getpage_num_dy(dic_url)
		#得到直播页数 通过发送get请求获得请求值
        # 若直播页数大于1，则遍历页数
        if max_live_page>0:
            live_page_num=1
            while (live_page_num<=max_live_page):
                page_url=dic_url+"?page="+str(live_page_num)+"&isAjax=1"#例"www.douyu.com/game/lol?page=2&isAjax=1"
                res = urllib2.urlopen(page_url)
                html = res.read()
                soup=BeautifulSoup(html,"html.parser")
                info=soup.find_all("span",class_="dy-num fr")
                # 获取所有含有直播人数的标签
                for tag in info:
                    # 判断是否存在有效值↓
                    if isinstance(tag,bs4.element.Tag):
                        live_name=tag.find_previous_sibling("span").get_text().encode("utf-8")
                        live_num=unicode(tag.string).encode("utf-8")
                        # 若直播人数含有"万" 将其改为数值↓
                        if live_num.find("万")>0:
                            live_num=str(float(live_num.rstrip("万").encode("utf-8"))*10000)
                        arr.append((live_name,live_num,dic2['name'],dic2['url'],"douyu",pre_time_str))            
                print "success"+str(live_page_num)
                live_page_num=live_page_num+1

#
#主函数    
#
# main
getdic_live_dy()
print "----------------爬取成功 正在写入数据库-----------------"
conn=MySQLdb.connect(host='localhost',user='root',passwd='welkin',db='spider_data',port=3306,charset='utf8')
cur=conn.cursor()
sql = "insert into data (name,num,type,type_url,platform,time) values(%s,%s,%s,%s,%s,%s)"
try:
    cur.executemany(sql, arr)
except Exception as e:
    print ("执行MySQL: %s 时出错：%s" % (sql, e))
finally:
    cur.close()
    conn.commit()
    conn.close()
now_time=datetime.now()
time_diff=(now_time-pre_time).seconds
print ("---------写入成功---------写入 %s 条数据-------耗时 %s ---秒----------" % (len(arr),time_diff))

