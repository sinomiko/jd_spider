#!/usr/bin/python
#-*- coding: utf-8 -*-
#encoding=utf-8

import jd_db
import jd_config
import jd_spider
import jd_logger


import os
import time

from jd_spider import UrlExtendThread
from jd_spider import UrlThread

threads_extend = []
threads_product = []


if __name__ == '__main__':
    
    main_log = jd_logger.Jd_Logger("MAIN")
    
    if not os.path.exists(jd_config.JDSPR_RESULT):
	main_log.info("初始化","创建结果保存目录")
        os.mkdir(jd_config.JDSPR_RESULT)
	
    jdb = jd_db.Jd_Db(jd_config.SQLITE_DB)

    jd_spider.get_product_ids(jd_config.JDSPR_START_URL, jdb)

    main_log.info("初始化","开启URL抓取线程")
    for i in range(2):
	t = UrlExtendThread(i)
	#t.setDaemon(True) #如果设置后台，输出就前台无法显示了
	t.start()
	time.sleep(2)
	threads_extend.append(t)

    main_log.info("初始化","开启数据抓取线程")
    for i in range(10, 18):
	t = UrlThread(i)
	t.start()
	time.sleep(2)
	threads_product.append(t)
    
    while True:
        time.sleep(10)
	jdb.db_statistics()
	print u"线程状态：",
	for item in threads_extend:
	    if item.isAlive():
		print 'A ',
	    else:
		print 'D ',
	print ' | ',
	for item in threads_product:
	    if item.isAlive():
		print 'A ',
	    else:
		print 'D ',
	print
    
        
    print u"程序结束..."
    
