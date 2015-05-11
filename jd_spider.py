#!/usr/bin/python
#-*- coding: utf-8 -*-
#encoding=utf-8
from jd_db import Jd_Db
import jd_config
import jd_utils
import jd_logger

import urllib2
import urllib
import codecs
import re
import os
import threading
import time
from BeautifulSoup import BeautifulSoup

exitFlag = 0

jd_item_url = "http://item.jd.com/%d.html"
jd_consult_url = "http://club.jd.com/allconsultations/%d-%d-1.html"
jd_headers = {"Origin":"http://www.jd.com/",
              "Referer":"http://www.jd.com/",
              "Content-type": "application/x-www-form-urlencoded; charset=UTF-8",
              "Accept": "*/*",
               "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:37.0) Gecko/20100101 Firefox/37.0",
               "Cookie":"__jda=122270672.1231994341.1431153572.1431218361.1431221231.6; __jdv=122270672|direct|-|none|-; __jdu=1231994341; ipLocation=%u5317%u4EAC; areaId=1; ipLoc-djd=1-72-2799-0; __jdc=122270672; user-key=c6e4b38e-59d7-48d4-8d1b-f597704633d0; cn=1; __jdb=122270672.12.1231994341|6.1431221231"
               }

result_path = jd_config.JDSPR_RESULT

__metaclass__ = type

jdspr_log = jd_logger.Jd_Logger("JD_SPR")

class UrlExtendThread(threading.Thread):
    def __init__(self, threadID):
        threading.Thread.__init__(self)
        self.threadID = threadID
        
    def run(self):
        print "Starting %d ...\n" % self.threadID
	jdb = Jd_Db(jd_config.SQLITE_DB)
        while True:
	    if jdb.db_unprocess_count() > 300:
		#print u"系统负载重，暂停展开网页...\n"
		continue
	    full_url = jdb.db_query_extend()
            if full_url:
		get_product_ids(full_url, jdb)
		time.sleep(7)
            else:
		time.sleep(2)
	print "Exiting %d ..." % self.threadID

class UrlThread(threading.Thread):
    def __init__(self, threadID):
        threading.Thread.__init__(self)
        self.threadID = threadID
        
    def run(self):
        print "Starting %d ...\n" % self.threadID
	jda = JdAnysis(self.threadID)
	jdb = Jd_Db(jd_config.SQLITE_DB)
        while True:
	    full_url = jdb.db_query_process()
            if full_url:
		jda.get_product_consults(full_url)
            else:
		time.sleep(2)
	    
	print "Exiting %d ..." % self.threadID

class JdAnysis:
    def __init__(self, tid = 0):
	self.tid = tid
        pass
    
    def get_product_consults(self, product_url):
	page_id = 1    
	product_id = int(product_url.split('.')[2].split('/')[1])
        result_file = "%s/%d.txt"%(result_path,product_id)
	if os.path.exists(result_file):
	    return
	product_url = jd_item_url % product_id
	try:
	    request = urllib2.Request(product_url, headers = jd_headers)
	    product_html = jd_utils.encoding(urllib2.urlopen(request).read())
	except UnicodeDecodeError:
	    print u"GBK/Unicode编解码错误!"
	    return
	except Exception:
	    print u"未知错误!"
	    f.close()
	    return	
	product_soup = BeautifulSoup(product_html)
	
	product_name = product_soup.find('h1')
	if not product_name:
	    return;
	
	print u"线程[%d]正在处理 %d" % ( self.tid, product_id )
	f = codecs.open(result_file, 'wb',encoding = 'utf-8')   
	f.write(u"产品名称：" + product_name.string + u"\n")
	
        while  True:
            product_consult_url = jd_consult_url % ( product_id, page_id )
            #print ("=============> DOING... " + product_consult_url)
	    try:
		request = urllib2.Request(product_consult_url, headers = jd_headers)
		consult_html = jd_utils.encoding(urllib2.urlopen(request).read())
	    except UnicodeDecodeError:
		print u"GBK/Unicode编解码错误!"
		f.close()
		return	
	    except urllib2.HTTPError, e:
		print u"HTTP错误!"
		f.close()
		return
	    except Exception:
		print u"未知错误!"
		f.close()
		return
            consult_soup = BeautifulSoup(consult_html)
            self.get_page_consult(consult_soup, f) 
            pagination = consult_soup.find('div', attrs = {"class":"Pagination"})
            if pagination and pagination.findAll('a',attrs = {"class":"next"}) :
		page_id = page_id + 1;
                f.flush()
	    else:
		break
		
	print u"线程[%d]处理完毕 %d" % ( self.tid, product_id )
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

def get_product_ids(url, jdb):
    try:
	request = urllib2.Request(url, headers = jd_headers)
	url_html = jd_utils.encoding(urllib2.urlopen(request).read())
    except UnicodeDecodeError:
	print u"GBK/Unicode编解码错误!"
	return	    
    except urllib2.HTTPError, e:
	print u"HTTP错误!"
	return    
    url_soup = BeautifulSoup(url_html)
    url_extend = url_soup.findAll('a', attrs = {"href": re.compile(r"^http://\w+.jd.com/.+\.(htm|html)$")})
    for url_item in url_extend:
        url_str = url_item.get("href")
	m = re.match(r'^http://item.jd.com/\d+.html$', url_str)
	if m:
	    jdb.db_insert_product(m.string)	
	else:
	    if not re.match(r'^http://help.jd.com', url_str):
		jdb.db_insert_no_product(url_str)
	
