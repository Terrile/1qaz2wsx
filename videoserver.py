#coding=UTF-8
__author__ = 'Administrator'
from flask import Flask
from NewsCorpus import NewsCorpus
import json
app = Flask(__name__)

newsCorpus = NewsCorpus()
#init
def init():
    print 'init'
    newsCorpus.loadConf()
    newsCorpus.loadcorpus(newsCorpus.dir_bkp)
    print 'init finished'

#需要外部定时任务不断的调用这个接口才能达到不断插入数据的效果
@app.route("/insert")
def insert():
    filename_list=newsCorpus.scanfile(newsCorpus.dir,postfix=".insert")
    if len(filename_list)>0:
        newsCorpus.insert(filename_list[0])
    return "Finished %d "%filename_list.__len__()

@app.route("/request")
def request():
    print 'receive request'
    video_list = newsCorpus.getRandomVideos()
    #json_string = json.dumps([ob.__dict__ for ob in video_list])
    #json_string = json.dumps(video_list)
    res = {}
    res['\"V9LG4B3A0\"'] = video_list
    json_string = json.dumps(res)
    print json_string
    return json_string

@app.route("/")
def hello():
    return "Hello World!"

if __name__ == "__main__":
    init()
    app.run(host='0.0.0.0',port=8083)
