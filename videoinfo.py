class VideoInfo:
    def __init__(self,userid=None,vid=None,title=None,tag=None,vv=None,createtime=None):
        self.userid=userid
        self.vid=vid
        self.title=title
        self.tag=tag
        self.playCount=vv
        self.createtime=createtime
        self.cover = ''
    def __str__(self):
        return '{}:{}:{}:{}:{}'.format(self.vid,self.title,self.cover,self.createtime,self.tag)


'''
class VideoInfo:
    def __init__(self,userid=None,vid=None,title=None,tag=None,vv=None,createtime=None):
        self.userid=userid
        self.vid=vid
        self.title=title
        self.tag=tag
        self.vv=vv
        self.createtime=createtime
        self.thumbnail = ''
    def __str__(self):
        return '{}:{}:{}:{}:{}'.format(self.vid,self.title,self.thumbnail,self.createtime,self.tag)
'''