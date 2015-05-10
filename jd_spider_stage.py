#!/usr/bin/python
#-*- coding: utf-8 -*-
#encoding=utf-8
import urllib2
import urllib
import codecs
import re
import os
from BeautifulSoup import BeautifulSoup

import threading
from Queue import Queue
import time

q = Queue()
exitFlag = 0
qLock = threading.Lock()
threads = []
threadID = 1

jd_start_url = "http://channel.jd.com/electronic.html"
jd_item_url = "http://item.jd.com/%d.html"
jd_consult_url = "http://club.jd.com/allconsultations/%d-%d-1.html"
jd_headers = {"Origin":"http://www.jd.com/",
              "Referer":"http://www.jd.com/",
              "Content-type": "application/x-www-form-urlencoded; charset=UTF-8",
              "Accept": "*/*",
               "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:37.0) Gecko/20100101 Firefox/37.0",
               "Cookie":"__jda=122270672.1231994341.1431153572.1431218361.1431221231.6; __jdv=122270672|direct|-|none|-; __jdu=1231994341; ipLocation=%u5317%u4EAC; areaId=1; ipLoc-djd=1-72-2799-0; __jdc=122270672; user-key=c6e4b38e-59d7-48d4-8d1b-f597704633d0; cn=1; __jdb=122270672.12.1231994341|6.1431221231"
               }

result_path = os.getcwd() + "/www.jd.com/"

__metaclass__ = type

class UrlThread(threading.Thread):
    def __init__(self, threadID, queue):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.q = queue
        
    def run(self):
        print "starting %d" % self.threadID
	jda = JdAnysis(self.threadID)
        while True:
            qLock.acquire()
            if not q.empty():
		tmp = q.get()
                qLock.release()
		jda.get_product_consults(tmp)
            else:
                qLock.release()
            time.sleep(1)
	    
	print "Exiting %d" % self.threadID

class JdAnysis:
    def __init__(self, tid = 0):
	self.tid = tid
        pass
    
    def get_product_consults(self, product_id):
        page_id = 1    
        result_file = "%s/%d.txt"%(result_path,product_id)
	if os.path.exists(result_file):
	    return
	product_url = jd_item_url % product_id
	try:
	    product_html = urllib2.urlopen(product_url).read().decode('gbk').encode("utf-8")
	except UnicodeDecodeError:
	    print u"GBK/Unicode编解码错误!"
	    return
	product_soup = BeautifulSoup(product_html)
	product_name = product_soup.find('h1').string
	
	print u"线程[%d]正在处理 %d[%s]" % ( self.tid, product_id, product_name )
        f = codecs.open(result_file, 'wb',encoding = 'utf-8')   
	f.write(u"产品名称：" + product_name + u"\n")
        while  True:
            product_consult_url = jd_consult_url % ( product_id, page_id )
            #print ("=============> DOING... " + product_consult_url)
	    try:
		request = urllib2.Request(product_consult_url, headers = jd_headers)
		consult_html = urllib2.urlopen(request).read().decode('gbk').encode("utf-8")
	    except UnicodeDecodeError:
		print u"GBK/Unicode编解码错误!"
		f.close()
		return		
            consult_soup = BeautifulSoup(consult_html)
            self.get_page_consult(consult_soup, f) 
            pagination = consult_soup.find('div', attrs = {"class":"Pagination"})
            if not pagination.findAll('a',attrs = {"class":"next"}) :
                break;
            else:
                page_id = page_id + 1;
                f.flush()
	print u"线程[%d]处理完毕 %d[%s]" % ( self.tid, product_id, product_name )
        get_product_ids(product_consult_url)
        f.close()
        
    def get_page_consult(self, page_soup, store = 0):
        liResult = page_soup.findAll('div', attrs = {"class":"Refer_List" }) 
        for consult in liResult:
            consult_EntryArray = consult.findAll('div', attrs = {"class": ["refer refer_bg", "refer"]})
            for consult_item in consult_EntryArray:
                ask_anwser = consult_item.findAll('dl', attrs = {"class": ["ask", "answer"]})
                if ask_anwser and ask_anwser[0].dd.a.string and ask_anwser[1].dd.string :
                    ask = ask_anwser[0].dd.a.string.strip()
                    anw = ask_anwser[1].dd.string.strip()   
                    tail_t = anw.find(u"感谢您对京东的支持！祝您购物愉快！")
                    if tail_t > 0:
                        anw = anw[:tail_t]
                    strs = ask + u"\n=>" + anw + u"\n"               
                    if(store):
                        store.write(strs)
                    else:
                        print strs              

def get_product_ids(url):
    global q
    global qLock
    request = urllib2.Request(url, headers = jd_headers)
    url_html = urllib2.urlopen(request).read().decode('gbk').encode("utf-8")
    url_soup = BeautifulSoup(url_html)
    url_products = url_soup.findAll('a', attrs = {"href": re.compile(r"^http://item.jd.com/\d+.html")})
    for url_item in url_products:
        #print url_item.get("href")
	url_num = int(url_item.get("href").split('.')[2].split('/')[1])
	result_file = "%s/%d.txt"%(result_path,url_num)
	if not os.path.exists(result_file):
	    print u"添加产品%d" % url_num
	    qLock.acquire()
	    q.put(url_num)
	    qLock.release()

if __name__ == '__main__':
    
    if not os.path.exists(result_path):
        os.mkdir(result_path)

    get_product_ids(jd_start_url)
    
    print u"起始队列大小:%d" % q.qsize()
    
    for i in range(10):
        t = UrlThread(i, q)
        t.start()
        threads.append(t)
         
    while not q.empty():
	time.sleep(5)
	print u"==>目前队列大小:%d\n" % q.qsize()
    
    exitFlag = 1
    
    for t in  threads:
        t.join()
        
    print u"抓取结束..."
    