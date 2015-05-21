#!/usr/bin/python
#-*- coding: utf-8 -*-
#encoding=utf-8

import jd_db
import jd_config
import jd_spider
import jd_logger


import os
import time
import threading

from jd_spider import UrlExtendThread
from jd_spider import UrlThread

threads_extend = []
threads_product = []

gdb_lock = threading.RLock()


if __name__ == '__main__':
    
    if not os.path.exists(jd_config.PRJ_PATH):
        os.makedirs(jd_config.PRJ_PATH)
        
    main_log = jd_logger.Jd_Logger("MAIN")
    
    if not os.path.exists(jd_config.JDSPR_RESULT):
        os.makedirs(jd_config.JDSPR_RESULT)
	
    jdb = jd_db.Jd_Db(jd_config.SQLITE_DB)

    #jd_spider.get_product_ids(jd_config.JDSPR_START_URL, jdb)

    main_log.info("初始化","开启URL抓取线程")
    for i in range(5):
        t = UrlExtendThread(i)
        t.start()
        time.sleep(2)
        threads_extend.append(t)

    #while True:
    #    time.sleep(10)
    #    with jd_spider.gdb_lock:
    #        jdb.db_statistics()

    main_log.info("初始化","开启数据抓取线程")
    for i in range(10, 16):
        t = UrlThread(i)
        t.start()
        time.sleep(2)
        threads_product.append(t)
		
    while True:
        time.sleep(30)
        with jd_spider.gdb_lock:
            jdb.db_statistics()
        print ("线程状态：", end = '')
        for item in threads_extend:
            if item.isAlive():
                print ('A ', end = '')
            else:
                print ('D ', end = '')
        print (' | ', end = '')
        for item in threads_product:
            if item.isAlive():
                print ('A ', end = '')
            else:
                print ('D ', end = '')
        print("")		
        
    print ("程序结束...")
    
