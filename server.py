#!/usr/bin/python
#-*- coding: utf-8 -*-
#encoding=utf-8

import jd_db
import jd_config
import jd_spider
import jd_logger

import socket


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

class DistributeThread(threading.Thread):
    def __init__(self, host, port, tid):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.threadID = tid
        self.sk = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
        self.jdb = jd_db.Jd_Db(jd_config.SQLITE_DB)
    
    def process_request(self, conn, addr):
        # 多个返回可能会被合并到一个数据包中
        req_datas = conn.recv(2048).decode()
        jreq_datas = eval(req_datas)
        for req_item in jreq_datas:
            if req_item['CLIENT'] != 0 and req_item['TYPE'] == 'REQ_URL':
                with jd_spider.gdb_lock:
                    full_url = jdb.db_query_comment()
                print("分配[%d]-%s" % (req_item['CLIENT'], full_url))
                rep_url = {'CLIENT':req_item['CLIENT'],'TYPE':'REP_URL','DATA':{'PURL':full_url, 'PATH':jd_config.JDSPR_RESULT_SERVER}}
                jrep_url = repr(rep_url) + ','
                conn.sendall(jrep_url.encode())        
            elif req_item['CLIENT'] != 0 and req_item['TYPE'] == 'FINISH':
                # 暂不处理
                print('处理结果:产品[%s]，数量[%d]，LUCK[%d]，PATH[%s]'% (req_item['DATA']['PURL'], \
                                    req_item['DATA']['CNT'], req_item['DATA']['LUCK'], req_item['DATA']['PATH']))
                if req_item['DATA']['PURL'] and req_item['DATA']['CNT']:
                    with jd_spider.gdb_lock:
                        jdb.db_update_comment(req_item['DATA']['PURL'], comment = 2)
                    local_f = "%s/%d_comm.txt" %(jd_config.JDSPR_RESULT_SERVER, req_item['DATA']['PID'])
                    local_path = jd_config.JDSPR_RESULT + req_item['DATA']['PATH']
                    if os.path.exists(local_f):
                        if not os.path.exists(local_path):
                            os.makedirs(local_path)
                        cmd = 'mv "%s" "%s" ' %(local_f, local_path)
                        os.system(cmd)
            else:
                print("UKNOWN CLIENT REQUEST!")
        conn.close()
        
    def run(self):
        print ("启动商品评论服务器分发线程 %d ...\n" % self.threadID)
        self.sk.bind((self.host,self.port))
        self.sk.listen(1)
        
        jdb = jd_db.Jd_Db(jd_config.SQLITE_DB)
        
        while True:
            conn, addr = self.sk.accept()
            #print("SERVER[%d], request from %s" % (self.threadID, addr))
            self.process_request(conn, addr)


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
    thread_distr = DistributeThread(HOST, PORT, 99)
    thread_distr.start()
    
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
    
