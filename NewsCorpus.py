#coding=utf-8
__author__ = 'SIZHUYUE'
import os
import signal
import codecs
import sys
import datetime
import time
import glob
import ConfigParser
import logging
import random
import json
from videoinfo import *

log_format = '%(filename)s [%(asctime)s] [%(levelname)s] %(message)s'
logging.basicConfig(filename="NewsCorpus.log",filemode='w',format=log_format,datefmt='%Y-%m-%d %H:%M:%S %p',level=logging.INFO)

class NewsCorpus:
    def __init__(self):
        self.vidlist = []
        self.newscorpus=dict()
        self.dir=''
        self.dir_bkp=''
        self.latestinterval=0
        self.saveInterval=0
        self.insert_req_num=0
        self.insert_data_num=0
        self.insert_costing=0
        self.select_req_num=0
        self.select_data_num=0
        self.select_costing=0

    def loadConf(self):
        logging.info('loadding conf file')#logging start time
        cf = ConfigParser.ConfigParser()
        cf.read("NewsCorpus.conf")
        self.dir=cf.get("dirInfo", "dir")
        self.dir_bkp=cf.get("dirInfo","dir_bkp")
        self.latestInterval=cf.getint("timeInfo","latestInterval")
        self.saveInterval=cf.getint("timeInfo","saveInterval")
        logging.info('\ndir：%s\ndir_bkp: %s\nlatestInterval: %s\naveInterval: %s'%(self.dir,self.dir_bkp,self.latestInterval,self.saveInterval))#logging config params

    def datetime_timestamp(self,dt):
        #dt为字符串
        #将"201203280653"转化为时间戳
        s = time.mktime(time.strptime(dt, '%Y%m%d%H%M'))
        return int(s)

    #######################把文件名后缀改成processing#######################
    def to_processing(self,requestfile):
        postfix_old=os.path.splitext(requestfile)[1]#源文件的后缀名,可能是.insert，可能是.select
        postfix_new='.processing'
        newfile=''
        if os.path.exists(requestfile):#如果原来的文件存在
            newfile=requestfile.replace(postfix_old,postfix_new)
            os.rename(requestfile, newfile)
            logging.info('%s -----> %s'%(requestfile,newfile))#logging
        else:
             logging.error('%s not exists!'%requestfile)
        return newfile

    #######################把文件名后缀从processing改成done#######################
    def to_complete(self,requestfile):
        postfix_new='.done'
        newfile=''
        if os.path.exists(requestfile):#如果原来的文件存在
            newfile=requestfile.replace('.processing',postfix_new)#把processing替换成done
            logging.info('%s -----> %s'%(requestfile,newfile))
            os.rename(requestfile,newfile)
        return newfile
    
    ########################判断最新的数据是否存在#################################
    def latest_bkp(self,reqfile_prefix,endtime):
        end_string=endtime.strftime('%Y%m%d')
        filepath='%s.%s.%s'%(reqfile_prefix,end_string,'done')
        counter=0
        while not os.path.exists(filepath):#如果最新数据不存在，则查前一天的
            endtime=endtime-datetime.timedelta(days=1)
            end_string=endtime.strftime('%Y%m%d')
            filepath='%s.%s.%s'%(reqfile_prefix,end_string,'done')
            print filepath
            counter+=1
            if counter>=self.latestInterval:
                filepath='-1'
                break
        if filepath=='-1':
            logging.warning("Something is wrong with back up data!")
        return filepath
        

    #######################程序开始时需要先把最新的数据load进来#######################
    def loadcorpus(self,dir):
        endtime=datetime.datetime.now()
        reqfile_prefix='%szixunfile'%dir
        res=self.latest_bkp(reqfile_prefix,endtime)
        if res=='-1':
            print 'no latest file (in 7days) '
            return -1
        print res
        corpus_old=codecs.open(res,mode='r',encoding='utf-8')
        while True:
            line=corpus_old.readline()
            line=line.strip()
            if not line:
                break
            fields=line.split('\t')
            if len(fields)<6:
                continue
            userid=fields[0]
            vid=fields[1]
            title=fields[2]
            tags=fields[3]
            vv=fields[4]
            createtime=fields[5]
            video=VideoInfo(userid,vid,title,tags,vv,createtime)
            self.newscorpus[vid]=video
        corpus_old.close()
        logging.info('load %s success!'%res)
        logging.info('%d news are loaded to memory!'%len(self.newscorpus))
    ###################################扫描文件####################################
    def scanfile(self,directory,prefix=None,postfix=None):
        files_list=[]
        for file in glob.glob(directory+'*'+postfix):
            files_list.append(file)
        return files_list

    #######################向内存中插入目前爬到的数据#######################
    def insert(self,requestfile):
        logging.info('begin to insert %s to Corpus!'%requestfile)
        start=datetime.datetime.now()
        self.insert_req_num+=1#request number of insert
        requestfile=self.to_processing(requestfile)
        if requestfile=='':
            logging.error("convert postfix of insert file to '.processing' failed!")#logging
            return
        insert_file=codecs.open(requestfile,mode='r',encoding='utf-8')
        while True:
            try:
                line=insert_file.readline()
            except Exception, e:
                logging.error('invalid line, codecs err')#logging
                print e
                continue
            if not line:
                break
            parsedRes = json.loads(line,encoding='utf-8')
            videos = parsedRes['videos']
            for video in videos:
                vid = video['id']
                self.newscorpus[vid]=video
                if vid not in self.vidlist:
                    self.vidlist.append(vid)
                self.insert_data_num += 1
        insert_file.close()
        logging.info('insert success! inserted num {}'.format(self.insert_data_num))
        logging.info('video corpus size {}'.format(len(self.newscorpus)))
        self.to_complete(requestfile)
        end=datetime.datetime.now()
        self.insert_costing+=(end-start).microseconds

    '''
     def insert(self,requestfile):
        logging.info('begin to insert %s to Corpus!'%requestfile)
        start=datetime.datetime.now()
        self.insert_req_num+=1#request number of insert
        requestfile=self.to_processing(requestfile)
        if requestfile=='':
            logging.error("convert postfix of insert file to '.processing' failed!")#logging
            return
        insert_file=codecs.open(requestfile,mode='r',encoding='utf-8')
        while True:
            try:
                line=insert_file.readline()
            except:
                logging.error('invalid line, codecs err')#logging
                continue
            if not line:
                break

            line=line.strip()
            fields=line.split('\t')
            if len(fields)<7:
                continue
            userid=fields[0]
            vid=fields[1]
            title=fields[2]
            tags=fields[3]
            vv=fields[5]
            createtime=fields[6]
            video=VideoInfo(userid,vid,title,tags,vv,createtime)
            if len(fields)>7:
                video.thumbnail = fields[7]
            self.newscorpus[vid]=video
            if vid not in self.vidlist:
                self.vidlist.append(vid)
            self.insert_data_num += 1
        insert_file.close()
        logging.info('insert success! inserted num {}'.format(self.insert_data_num))
        logging.info('video corpus size {}'.format(len(self.newscorpus)))
        self.to_complete(requestfile)
        end=datetime.datetime.now()
        self.insert_costing+=(end-start).microseconds
    '''
    ##########################对文件名进行处理，获取其开始和结束日期##############
    def process_request(self,requestfile):
        start=datetime.datetime.now()
        filename=os.path.basename(requestfile)
        logging.info('process %s to get starttime and endtime'%filename)#logging
        filename_part=filename.split('.')#为了获得开始和结束的时间
        if len(filename_part)<4:
            logging.error('can not split filename to 4 parts!')
            return
        starttime=filename_part[1]
        endtime=filename_part[2]
        starttime=self.datetime_timestamp(starttime)#转成时间戳
        endtime=self.datetime_timestamp(endtime)#转成时间戳
        res_stat=self.select(starttime,endtime,requestfile)
        logging.info('%s proceesing status:%s'%(requestfile,res_stat))
        end=datetime.datetime.now()
        self.select_costing+=(end-start).microseconds
        return res_stat#return processing status: SUCCEED, FAILED??

###################从内存中选取需要的时间段内的数据#######################
    def select(self, start_time, end_time, requestfile):
        self.select_req_num+=1
        requestfile=self.to_processing(requestfile)
        if requestfile=='':
            logging.error("convert postfix of %s to '.processing' failed!"%requestfile)#logging
            return 'failed'
        select_file=codecs.open(requestfile,mode='w',encoding='utf-8')
        writeNumber=0
        for video in self.newscorpus:
            info=self.newscorpus[video]
            if int(info.createtime)>=start_time and int(info.createtime)<=end_time:
                select_file.write(u'%s\t%s\t%s\t%s\t%s\t%s\n'%(info.userid,info.vid,info.title,info.tag,info.vv,info.createtime))
                writeNumber+=1
        select_file.close()
        self.to_complete(requestfile)
        logging.info('starttime :%s ,endtime : %s ,select %d news to the file'%(start_time,end_time,writeNumber))#log starttime, endtime, result number
        self.select_data_num+=writeNumber
        return 'success'
    
    #######################kill掉，清空内存#######################
    def stop(self):
        myselfList = os.popen("ps -ef|grep NewsCorpus.py|grep -v grep|awk '{print $2}'").readlines()
        for pid in myselfList:
            os.kill(int(pid),signal.SIGKILL)

    def alarm(self):
        os.popen("sh /opt/sizhuyue/python/news_index/alarm.sh")

    ###################每天会生成一份备份文件，保存n天之内的数据##########################
    def savefile(self,requestfile):
        logging.info('begin to back up the corpus.......')#logging 
        endtime=datetime.datetime.now()#对开始和结束时间进行处理
        starttime=endtime-datetime.timedelta(days=self.saveInterval)
        end_stamp=time.mktime(endtime.timetuple())
        start_stamp=time.mktime(starttime.timetuple())
        end_string=endtime.strftime('%Y%m%d')
        requestfile=self.to_processing(requestfile)
        if requestfile=='':
            logging.error("convert postfix of back up file to '.processing' failed!")#logging
            return
        writeNumber=0
        save_file=codecs.open(requestfile,mode='w',encoding='utf-8')
        for video in self.newscorpus:
            info=self.newscorpus[video]
            if int(info.createtime)>=int(start_stamp) and int(info.createtime)<=int(end_stamp):
                save_file.write(u'%s\t%s\t%s\t%s\t%s\t%s\n'%(info.userid,info.vid,info.title,info.tag,info.vv,info.createtime))
                writeNumber+=1
        save_file.close()
        logging.info('back up success! #news: %d'%writeNumber)
        self.to_complete(requestfile)

    def getRandomVideos(self, num=10):
        res = []
        if len(self.newscorpus) ==0 :
            return res
        res_idx = random.sample(self.vidlist,min(len(self.vidlist),num))
        print res_idx
        for vid in res_idx:
            if vid in self.newscorpus:
                res.append(self.newscorpus[vid])
            else:
                print 'invalid vid {}'.format(vid)
        print 'result in getRandomVideos:'
        print res
        print 'return res'
        return res
        #return self.newscorpus.items()

if __name__=='__main__':
    logging.info('start to run NewsCorpus.py.......')
    if len(sys.argv)>1:
        cmd = sys.argv[1]
        if sys.argv[1]=='start':
            nc=NewsCorpus()
            nc.loadConf()
            nc.loadcorpus(nc.dir_bkp)
            while True:
                
                filename_list=nc.scanfile(nc.dir,postfix=".insert")
                if len(filename_list)>0:
                    nc.insert(filename_list[0])

                filename_list=nc.scanfile(nc.dir,postfix='.select')
                if len(filename_list)>0:
                    select_stat=nc.process_request(filename_list[0])

                filename_list=nc.scanfile(nc.dir_bkp,postfix='.stop')
                if len(filename_list)>0:
                    nc.savefile(filename_list[0])
                    logging.info('break to save file')
                    break
                
                time.sleep(1)#Min: sleep to avoid cpu exhausting...
            #log summary
            #summary:
            #1. insert request number 请求了几次
            #2. insert data number 共插入多少条数据
            #    avg insert time costing 平均耗时
            #3. select request number 请求了几次
            #4. select data number 共插入多少条数据
            #    avg select time costing 平均耗时
            logging.info('log summary:')
            logging.info('insert request number: %d'%nc.insert_req_num)
            logging.info('insert data number: %d'%nc.insert_data_num)
            if nc.insert_req_num!=0:
                logging.info('avg insert time costing: %f'%(nc.insert_costing/nc.insert_req_num))
            logging.info('select request number : %d'%nc.select_req_num)
            logging.info('select data number: %d'%nc.select_data_num)
            if nc.select_req_num!=0:
                logging.info('avg select time costing: %f'%(nc.select_costing/nc.select_req_num))
            nc.alarm()
            os.popen("python /opt/sizhuyue/python/news_index/NewsCorpus.py start")

    else:
        print"usage:"
        print"python NewsCorpus.py\tstart\t//start with loading latest data"
        #print"python NewsCorpus.py\tstop\t'save file in memory and kill itself'"
