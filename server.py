#!/usr/bin/python
#-*- coding: utf-8 -*-
#encoding=utf-8

import jd_db
import jd_config
import jd_spider
import jd_logger

import socket
import socketserver


import os
import time
import threading

from jd_spider import UrlExtendThread
from jd_spider import ConosultThread

# Server Side
HOST = jd_config.SERVER_ADDR
PORT = jd_config.SERVER_PORT

threads_extend = []
threads_conosult = []

gdb_lock = threading.RLock()

# Base Class
class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

class DistributeThreadHandler(socketserver.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.cur_thread = threading.current_thread().getName()
        self.jdb = jd_db.Jd_Db(jd_config.SQLITE_DB)
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)    

    def handle(self):
        # 多个返回可能会被合并到一个数据包中
        req_datas = self.request.recv(2048).decode()
        jreq_datas = eval(req_datas)
        for req_item in jreq_datas:
            if req_item['CLIENT'] != 0 and req_item['TYPE'] == 'REQ_URL':
                with jd_spider.gdb_lock:
                    full_url = self.jdb.db_query_comment()
                print("{%s}分配[%d]-%s" % (self.cur_thread, req_item['CLIENT'], full_url))
                rep_url = {'CLIENT':req_item['CLIENT'],'TYPE':'REP_URL','DATA':{'PURL':full_url, 'PATH':jd_config.JDSPR_RESULT_SERVER}}
                jrep_url = repr(rep_url) + ','
                self.request.sendall(jrep_url.encode())        
            elif req_item['CLIENT'] != 0 and req_item['TYPE'] == 'FINISH':
                # 暂不处理
                print('{%s}处理结果:产品[%s]，数量[%d]，LUCK[%d]，PATH[%s]'% (self.cur_thread, req_item['DATA']['PURL'], \
                                    req_item['DATA']['CNT'], req_item['DATA']['LUCK'], req_item['DATA']['PATH']))
                if req_item['DATA']['PURL'] and req_item['DATA']['CNT']:
                    with jd_spider.gdb_lock:
                        self.jdb.db_update_comment(req_item['DATA']['PURL'], comment = 2)
                    local_f = "%s/%d_comm.txt" %(jd_config.JDSPR_RESULT_SERVER, req_item['DATA']['PID'])
                    local_path = jd_config.JDSPR_RESULT + req_item['DATA']['PATH']
                    if os.path.exists(local_f):
                        if not os.path.exists(local_path):
                            os.makedirs(local_path)
                        cmd = 'mv "%s" "%s" ' %(local_f, local_path)
                        os.system(cmd)
            else:
                print("UKNOWN CLIENT REQUEST!")
                
        return


if __name__ == '__main__':
    
    if not os.path.exists(jd_config.PRJ_PATH):
        os.makedirs(jd_config.PRJ_PATH)
        
    main_log = jd_logger.Jd_Logger("MAIN")
    
    if not os.path.exists(jd_config.JDSPR_RESULT):
        os.makedirs(jd_config.JDSPR_RESULT)
        
    if not os.path.exists(jd_config.JDSPR_RESULT_SERVER):
        os.makedirs(jd_config.JDSPR_RESULT_SERVER)
	
    jdb = jd_db.Jd_Db(jd_config.SQLITE_DB)

    jd_spider.get_product_ids(jd_config.JDSPR_START_URL, jdb, 0)

    print("初始化 -- 开启URL抓取线程")
    for i in range(2):
        t = UrlExtendThread(i)
        t.start()
        time.sleep(2)
        threads_extend.append(t)

    print("初始化 -- 开启商品咨询抓取线程")
    for i in range(10, 15):
        t = ConosultThread(i)
        t.start()
        time.sleep(2)
        threads_conosult.append(t)
        
    print("初始化 -- 开启商品评论分发线程")
    server = ThreadedTCPServer((HOST, PORT), DistributeThreadHandler)
    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    thread_distr = threading.Thread(target=server.serve_forever)
    thread_distr.start()    
    print("启动完成 -- 开启商品评论分发线程")
    
    
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
        for item in threads_conosult:
            if item.isAlive():
                print ('A ', end = '')
            else:
                print ('D ', end = '')
        print (' | ', end = '')	
        
        if thread_distr.isAlive():
            print ('A ', end = '')
        else:
            print ('D ', end = '')
        print("")        
        
    print ("程序结束...")
    
