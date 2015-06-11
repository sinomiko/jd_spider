#!/usr/bin/python
#-*- coding: utf-8 -*-
#encoding=utf-8

import jd_config
import jd_spider
import jd_logger

import os
import time
import threading

from jd_comments import CommentThread

threads_comment = []

if __name__ == '__main__':
    
    main_log = jd_logger.Jd_Logger("MAIN")
    
    if not os.path.exists(jd_config.JDSPR_RESULT_LOCAL):
        os.makedirs(jd_config.JDSPR_RESULT_LOCAL)        
	
    
    main_log.info("初始化","开启商品评论抓取线程")
    for i in range(20, 32):
        t = CommentThread(jd_config.SERVER_ADDR, jd_config.SERVER_PORT, i)
        t.start()
        time.sleep(3)
        threads_comment.append(t)
		
    while True:
        time.sleep(10)
        for item in threads_comment:
            if item.isAlive():
                print ('A ', end = '')
            else:
                print ('D ', end = '')
        print("")        
        
    print ("程序结束...")
    
