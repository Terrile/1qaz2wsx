__author__ = 'Administrator'
#coding=UTF-8
from flask import *
from flask import Flask
import codecs

app = Flask(__name__)
globalExpand = dict()

def loadContext(filepath):
    file = codecs.open(filepath,mode='r',encoding='utf8')
    lines = file.readlines()
    file.close()
    expand = dict()
    for line in lines:
        line = line.strip('\n')
        parts = line.split('\t')
        if len(parts)==0:
            continue
        if len(parts)==1:
            expand[parts[0]] = 'no result'
        else:
            expand[parts[0]] = parts[1]
    return expand

expand1 = loadContext('querycontext.txt')
expand2 = loadContext('querycontext.wq')
expand3 = loadContext('querycontext.merged')

for query in expand3:
    line = '<b>Merged:</b> </br>'+expand3[query]+'</br></br/> <b>Query2Vec: </b></br>'+expand2[query]+' </br> </br><b>ContextFreq:</b> </br>'+expand1[query]
    globalExpand[query] = line
expand1.clear()
expand2.clear()
expand3.clear()

def getExpand(query):
    if query in globalExpand:
        return globalExpand[query]
    else:
        return 'not found'

@app.route("/")
def hello():
    query = request.args.get('query')
    if not query:
        return 'query missed'
    result = getExpand(query)
    return result

if __name__ == "__main__":
#app.run(port=8081)
    app.run(host='0.0.0.0',port=8081)
