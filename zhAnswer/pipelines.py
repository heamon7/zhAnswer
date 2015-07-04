# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
from scrapy.exceptions import DropItem

from zhQuestion import settings
import time
import re
import redis
import happybase








class AnswerInfoPipeline(object):
    def __init__(self):

        self.redis4 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=4)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)

        connection = happybase.Connection(settings.HBASE_HOST)
        self.questionTable = connection.table('question')
        self.answerTable = connection.table('answer')

    def process_item(self, item, spider):
        if item['spiderName'] == 'answerInfo':

            questionId = str(item['questionId'])

            if questionId:
                self.redis11.sadd(str(questionId),str(item['answerDataToken']))


                currentTimestamp = int(time.time())

                result = self.redis4.lindex(str(questionId),0)
                if result:
                    recordTimestamp =result
                else:
                    recordTimestamp=''


                #无论之前有无记录，都会更新redis里的数据
                p4 = self.redis4.pipeline()
                p4.lpush(str(item['answerDataToken'])
                             # ,int(questionId)
                             ,str(item['answerDataId'])
                             ,str(item['answerVoterCount'])
                             # 其实commentCount也可以去掉
                             ,str(item['answerCommentCount'])
                             # ,str(isTopQuestion)
                             ,str(item['questionId'])


                             # ,str(item['questionShowTimes'])

                             # ,str(item['topicRelatedFollowerCount'])
                             # ,str(item['visitsCount'])
                             ,str(recordTimestamp))

                p4.ltrim(str(item['answerDataToken']),0,4)
                p4.execute()



                if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.UPDATE_PERIOD)):        # the latest record time in hbase
                    recordTimestamp = currentTimestamp
                    answerDetailDict={'detail:questionId':str(questionId),
                                    'detail:answerDataId':str(item['answerDataAid']),
                                   'detail:answerDataToken':str(item['answerDataAtoken']),
                                   'detail:answerDataCreated':str(item['answerDataCreated']),
                                   'detail:answerDataDeleted': str(item['answerDataDeleted']),
                                   'detail:answerDataHelpful': str(item['answerDataHelpful']),
                                   'detail:answerVoterCount': str(item['answerVoterCount']),
                                   'detail:answerDataResourceId': str(item['dataResourceId']),
                                   'detail:answerContent': str(item['answerContent'].encode('utf-8')),
                                   'detail:answerCreatedDate': str(item['answerCreatedDate'].encode('utf-8')),
                                   'detail:answerUpdatedDate': str(item['answerUpdatedDate'].encode('utf-8')),
                                   'detail:answerCommentCount': str(item['answerCommentCount']),
                                   'detail:answerAuthorLinkId': str(item['answerAuthorLinkId'].encode('utf-8')),
                                   'detail:answerAuthorImgLink': str(item['answerAuthorImgLink']),
                                   'detail:answerAuthorName': str(item['answerAuthorName'].encode('utf-8')),
                                   'detail:answerAuthorBio': str(item['answerAuthorBio'].encode('utf-8')),

                                   }


                    try:
                        self.answerTable.put(str(item['answerDataToken']),answerDetailDict)

                        # self.redis11.hsetnx(str(questionId),quesDetailDict)
                        self.redis4.lset(str(item['answerDataToken']),0,str(recordTimestamp))

                    except Exception,e:
                        logging.warning('Error with put questionId into redis: '+str(e)+' try again......')
                        try:
                            self.answerTable.put(str(item['answerDataToken']),answerDetailDict)


                            self.redis4.lset(str(item['answerDataToken']),0,str(recordTimestamp))
                            logging.warning('tried again and successfully put data into redis ......')
                        except Exception,e:
                            logging.warning('Error with put questionId into redis: '+str(e)+'tried again and failed')


            return item

        else:
            return item


class AnswerCommentPipeline(object):

    def __init__(self):

        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        # self.redis12 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=12)
        connection = happybase.Connection(settings.HBASE_HOST)

        self.commentTable = connection.table('comment')


    def process_item(self, item, spider):
        if item['spiderName'] == 'answerComment':

            answerDataToken = str(item['answerDataToken'])
            #如果有返回数据，即有评论
            if answerDataToken:
                #这里假定了问题的评论和答案的评论的dataid是不会重复的
                self.redis11.sadd(str(answerDataToken),str(item['commentDataId']))
                if item['userLinkId']:
                    self.redis3.sadd('userLinkIdSet',item['userLinkId'])


                #无论之前有无记录，都会更新redis里的数据


                commentDict={'comment:srcId':str(answerDataToken),
                                'comment:DataId':str(item['commentDataId']),
                               'comment:content':str(item['commentContent'].encode('utf-8')),
                                #日期可能含有中文
                               'comment:date': str(item['commentDate'].encode('utf-8')),
                               'comment:upCount': str(item['commentUpCount']),
                               'comment:userName': item['userName'].encode('utf-8'),
                               'comment:userLinkId': item['userLinkId'].encode('utf-8'),
                               'comment:userImgLink': str(item['userImgLink']),
                                'comment:type':'a'
                               }


                try:

                    self.commentTable.put(str(item['commentDataId']),commentDict)
                    # self.redis11.hsetnx(str(item['commentDataId']),quesCommentDict)

                except Exception,e:
                    logging.warning('Error with put commentDataId into hbase: '+str(e)+' try again......')
                    try:
                        # self.redis11.hsetnx(str(questionId),quesCommentDict)
                        self.commentTable.put(str(item['commentDataId']),commentDict)
                        logging.warning('tried again and successfully put data into hbase ......')
                    except Exception,e:
                        logging.warning('Error with put commentDataId into hbase: '+str(e)+'tried again and failed')
            return item
        else:
            return item



class AnswerVoterPipeline(object):

    def __init__(self):

        # self.redis1 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=1)

        #redis3存放用户索引，linkid，dataid，index
        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        # #redis4存放用户的基础信息
        # self.redis4 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=4)
        # #redis5存放问题的关注者集合
        # self.redis5 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=5)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        # connection = happybase.Connection(settings.HBASE_HOST)
        # self.userTable = connection.table('user')


#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):

        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'answerVoter':
            if item['answerDataToken'] and item['userDataId']:

                #userLinkId可能有中文
                self.redis11.sadd(str(item['answerDataToken']),str(item['userDataId']))
                self.redis3.sadd('userLinkIdSet',str(item['userLinkId'].encode('utf-8')))


            DropItem()
        else:
            DropItem()