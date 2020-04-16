#!/usr/bin/env python
# coding: utf8
import redis, time, threading, os

address = [
        '127.0.0.1:6379',
        ]

count_dict = {}
log_dir = '/data/clean_redis/'
general_fd = open(log_dir + 'clean_redis.log','a')

password = ''
db = 9
lone = 100  # 天

lone_secon = 60 * 60 * 24 * lone
now_time = int(time.time())

if not os.path.isdir(log_dir):
    os.mkdir(log_dir)

def main(add,sequence=0):
    host,port = add.split(':')
    conn = redis.Redis(host=host,port=port,db=db,password=password)
    count = 0
    while True:
        with open(log_dir + add + '.seq','w') as log_seq:
            log_seq.write(str(sequence))

        sequence,key = conn.scan(sequence,count=5000)
        for one in key:
            try:
                key_idtime = conn.object('idletime',one)
                #if (key_idtime - lone_secon) > 0 and (one.startswith('qfpush:') or one.startswith('_paycell_member_') or one.startswith('RISK_')):
                if (key_idtime - lone_secon) > 0:
                    #print "idtime(days): %-7.1f 天, key: %s, host: %s, port: %s" % (key_idtime / 60.0 / 60 /24 ,one,host,port),
                    s = conn.delete(one)
                    #s = 1
                    if s >= 1:
                        with open(log_dir + add + '.log','a') as log_fd:
                            log_fd.write("idtime(days): %-7.1f 天, key: %s, host: %s, port: %s, delete successful\n" % (key_idtime / 60.0 / 60 /24 ,one,host,port))
                    
                    count += 1
                else:
                     pass
            except Exception,e:
                general_fd.write(add + ': 49 line:\n' + e.message + '\n')
                general_fd.flush()

        if sequence == 0 or (not key):
            count_dict[add] = count
            break

thread_list = []
for add in address:
    #.seq文件用于记录redis scan有进度，可以在停止脚本以后，下次继续之前的进度。
    log_seq = log_dir + add + '.seq'
    try:
        if os.path.isfile(log_seq):
            sequence = int(open(log_seq).read().strip() or 0)
        else:
            sequence = 0

    except Exception,e:
        sequence = 0
        general_fd.write(add + ':67 line: 读取' + log_seq + '发现错误, now seq is 0' + '\n' + e.message + '\n')
        general_fd.flush()

    general_fd.write(add + ': sequence begin from ' + str(sequence) + '\n')
    general_fd.flush()
        
    thread_list.append(threading.Thread(target=main,args=(add,sequence)))
    
for thre in thread_list:
    thre.start()

for host_port,count in count_dict.items():
    print 'address: %s,  count: %s' % (host_port,count)
