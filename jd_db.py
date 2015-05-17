#!/usr/bin/python
#-*- coding: utf-8 -*-
#encoding=utf-8

import os
import time
import sqlite3
import hashlib

import jd_config
import jd_utils
import jd_logger

db_log = jd_logger.Jd_Logger("数据库")

class Jd_Db:
    def __init__(self, db_name):
        self.db_path_name = jd_config.SQLITE_PATH + db_name
        if not os.path.exists(self.db_path_name):
            db_log.info("初始化","数据库文件不存在，创建数据库中")
            if not os.path.exists(jd_config.SQLITE_PATH):
                os.makedirs(jd_config.SQLITE_PATH)
            self.db_init(db_name)
        else:
            pass
    
    def db_init(self, name):
        try:
            conn = sqlite3.connect(self.db_path_name, timeout = 300)
            sql_creat_table = '''
                        create table if not exists jd_info(
                        id integer primary key autoincrement, 
                        http_url varchar(256) DEFAULT NULL,
                        acc_time varchar(50) DEFAULT NULL,
                        is_product tinyint DEFAULT 0,
                        is_extended tinyint DEFAULT 0,
                        is_processed tinyint DEFAULT 0
                        );'''
            conn.execute(sql_creat_table)
        except Exception as e:
            print ('---'+str(e)+'--')
        finally:
            conn.close()
    
    def db_query_process(self):
        sql_query_data = ''' select http_url from jd_info where is_product = 1 and is_processed = 0''' 
        one = 0
        try:
            conn = sqlite3.connect(self.db_path_name, timeout = 1000)
            results = conn.execute(sql_query_data)
            one = results.fetchone()[0]
            if one:
                sql_update_data = '''update jd_info set acc_time = '%s', is_processed = 1 where http_url = '%s' ''' %(jd_utils.current_time(), one)
                conn.execute(sql_update_data)
                conn.commit()
        except Exception as e:
            print ('---'+str(e)+'--')
        finally:
            conn.close()
        return one
    
    def db_query_extend(self):
        sql_query_data = ''' select http_url from jd_info where is_extended = 0''' 
        one = 0
        try:
            conn = sqlite3.connect(self.db_path_name, timeout = 1000)
            results = conn.execute(sql_query_data)
            one = results.fetchone()[0]
            if one:
                sql_update_data = '''update jd_info set acc_time = '%s', is_extended = 1 where http_url = '%s' ''' %(jd_utils.current_time(), one)
                conn.execute(sql_update_data)
                conn.commit()
        except Exception as e:
            print ('---'+str(e)+'--')
        finally:
            conn.close()
        return one
            
    def db_query_count(self, product = 0, extend = 0, process = 0):
        sql_query_data = ''' select count(*) from jd_info where is_product = '%d' and is_extended = '%d' and is_processed = '%d' ''' % (product, extend, process)
        try:
            conn = sqlite3.connect(self.db_path_name, timeout = 1000)
            total = conn.execute(sql_query_data)
            total_count = total.fetchone()[0]
        except Exception as e:
            print ('---'+str(e)+'--')
        finally:
            conn.close()
        return total_count
        
    def db_insert_product(self, http_url):
        self.do_db_insert(http_url, product = 1)
        
    def db_insert_no_product(self, http_url):
        self.do_db_insert(http_url, product = 0)
        
    def do_db_insert(self, http_url, product = 0, extend = 0, process = 0):
        sql_query_data = ''' select count(*) from jd_info where http_url = '%s' ''' % http_url
        try:
            conn = sqlite3.connect(self.db_path_name, timeout = 1000)
            total = conn.execute(sql_query_data)
            if total.fetchone()[0] > 0:
                return
            sql_insert_data = '''insert into jd_info(http_url,acc_time,is_product,is_extended,is_processed) values ('%s', '%s', '%d', '%d', '%d')'''%(http_url,jd_utils.current_time(),product,extend,process)
            conn.execute(sql_insert_data)
            conn.commit()
        except Exception as e:
            print ('---'+str(e)+'--')
        finally:
            conn.close()
    
    def db_update_process(self, http_url, process = 1):    
        sql_insert_data = '''update jd_info set acc_time = '%s', is_processed = '%d' where http_url = '%s' ''' %(jd_utils.current_time(), process, http_url)
        try:
            conn = sqlite3.connect(self.db_path_name, timeout = 1000)
            conn.execute(sql_insert_data)
            conn.commit()
        except Exception as e:
            print ('---'+str(e)+'--')
        finally:
            conn.close()
        pass
    
    def db_update_extend(self, http_url, extend = 1 ):    
        sql_insert_data = '''update jd_info set acc_time = '%s', is_extended = '%d' where http_url = '%s' ''' %(jd_utils.current_time(), extend , http_url)
        try:
            conn = sqlite3.connect(self.db_path_name, timeout = 1000)
            conn.execute(sql_insert_data)
            conn.commit()
        except Exception as e:
            print ('---'+str(e)+'--')
        finally:
            conn.close()
            
    def do_db_dump(self):
        sql_dump_data = '''select * from jd_info'''
        try:
            conn = sqlite3.connect(self.db_path_name, timeout = 1000)
            cursor = conn.execute(sql_dump_data)
            for row in cursor:
                print (row[0],row[1],row[2],row[3],row[4],row[5])
        except Exception as e:
            print ('---'+str(e)+'--')
        finally:
            conn.close()
            
    def db_unprocess_count(self):
        total_unprocess = 0
        sql_query_unprocessed = ''' select count(*) from jd_info where is_product = 1 and is_processed = 0''' 
        try:
            conn = sqlite3.connect(self.db_path_name, timeout = 1000)
            total_unprocess = conn.execute(sql_query_unprocessed).fetchone()[0]
        except Exception as e:
            print ('---'+str(e)+'--')
        finally:
            conn.close()
        return total_unprocess    
        
    def db_statistics(self):
        sql_query_total = ''' select count(*) from jd_info ''' 
        sql_query_product = ''' select count(*) from jd_info where is_product = 1''' 
        sql_query_product_processed = ''' select count(*) from jd_info where is_product = 1 and is_processed = 1''' 
        sql_query_product_unprocessed = ''' select count(*) from jd_info where is_product = 1 and is_processed = 0''' 
        sql_query_extended = ''' select count(*) from jd_info where is_extended = 1''' 
        sql_query_unextended = ''' select count(*) from jd_info where is_extended = 0''' 
        conn = sqlite3.connect(self.db_path_name, timeout = 1000)
        try:
            total = conn.execute(sql_query_total).fetchone()[0]
            total_pr = conn.execute(sql_query_product).fetchone()[0]
            total_pr_proc = conn.execute(sql_query_product_processed).fetchone()[0]
            total_pr_unproc = conn.execute(sql_query_product_unprocessed).fetchone()[0]
            total_ext = conn.execute(sql_query_extended).fetchone()[0]
            total_unext = conn.execute(sql_query_unextended).fetchone()[0]
        except Exception as e:
            print ('---'+str(e)+'--')
        finally:
            conn.close()
        str_url = "URL总数：%d，URL已展开：%d，URL未展开：%d" % (total, total_ext, total_unext)
        str_prd = "产品总数：%d，已处理：%d，未处理：%d" % (total_pr, total_pr_proc, total_pr_unproc)
        print (str_url)
        print (str_prd)
        db_log.info("数据库统计",str_url)
        db_log.info("数据库统计",str_prd)
       
if __name__ == '__main__':
    db = Jd_Db("test.db")
    db.db_insert_product("http://item.baidu.com/12345.html")    
    db.db_insert_no_product("http://www.baidu.com")    
    db.do_db_dump()    
    alt1 = db.db_query_process()
    db.db_statistics()    
    if alt1:
        db.db_update_process(alt1)
    db.do_db_dump()    
    alt2 = db.db_query_extend()
    db.db_statistics()
    if alt2:
        db.db_update_extend(alt2)
    db.do_db_dump()    
    db.db_statistics()