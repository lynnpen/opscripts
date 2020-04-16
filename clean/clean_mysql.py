#!/usr/bin/env python
import pymysql
import datetime
import logging
import time

logging.basicConfig(format='%(message)s', filename='clean_mysql/dump.log',level=logging.DEBUG)
c = pymysql.connect(host = '127.0.0.1',
        port = 3306,
        user = 'root',
        password = 'xxxx',
        db = 'dbname',
        charset = 'utf8',
        cursorclass=pymysql.cursors.DictCursor)

the_time = datetime.datetime(2017, 01, 04, 00, 00, 00)
          
try:     
    with c.cursor() as cursor:
        while True: 
            sql = 'select * from sessions limit 1000'
            cursor.execute(sql)
            result = cursor.fetchall()
            logging.debug(result)
            for i in result:
                days_minus = the_time - i['atime']
                if days_minus.days >= 0:
                    sql = 'delete from sessions where session_id = %s'
                    r = cursor.execute(sql, (i['session_id']))
                    #print str(i['atime'])
            c.commit()
            time.sleep(1)
    
finally:
    c.close()
