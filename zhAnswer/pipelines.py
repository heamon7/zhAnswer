# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
from scrapy.exceptions import DropItem

from zhAnswer import settings
import time
import re
import redis
import happybase
from pymongo import MongoClient

class AnswerInfoPipeline(object):
    def __init__(self):

        # self.redis4 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=4)
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        # connection = happybase.Connection(settings.HBASE_HOST)
        # self.answerTable = connection.table('answer')

        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_answer_info = db['answerInfo']

    def process_item(self, item, spider):
        if item['spiderName'] == 'answerInfo':

            questionId = str(item['questionId'])

            if questionId:
                # self.redis11.sadd(str(questionId),str(item['answerDataToken']))
                currentTimestamp = int(time.time())
                # recordTimestamp = self.redis4.lindex(str(questionId),0)
                recordTimestamp = None
                # if result:
                #     recordTimestamp =result
                # else:
                #     recordTimestamp=''
                # #如果赞同数中含有k，需要转换成数字，并且进位
                try:
                    answerVoterCount = int(item['answerVoterCount'])
                except:
                    resultList = re.split('(\d*)',item['answerVoterCount'])
                    if resultList[2] =='K':
                        answerVoterCount = 1000*(int(resultList[1]))
                    else:
                        answerVoterCount =0
                        logging.error('Error in answerVoterCount with item[answerVoterCount] %s',item['answerVoterCount'])
                if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.INFO_UPDATE_PERIOD)):        # the latest record time in hbase
                    recordTimestamp = currentTimestamp
                    # p4 = self.redis4.pipeline()
                    # p4.lpush(str(item['answerDataToken'])
                    #              # ,int(questionId)
                    #              ,str(item['answerDataId'])
                    #              ,str(answerVoterCount)
                    #              # 其实commentCount也可以去掉
                    #              # ,str(item['answerCommentCount'])
                    #              # ,str(item['questionId'])
                    #              ,str(recordTimestamp))
                    # p4.ltrim(str(item['answerDataToken']),0,4)
                    # p4.execute()

                    answerDetailDict={'ques_id':str(questionId),
                                    'answer_data_id':str(item['answerDataId']),
                                   'answer_data_token':str(item['answerDataToken']),
                                   'answer_data_created':str(item['answerDataCreated']),
                                   'answer_data_deleted': str(item['answerDataDeleted']),
                                   'answer_data_helpful': str(item['answerDataHelpful']),
                                   'answer_voter_count': int(answerVoterCount),
                                   'answer_data_resource_id': str(item['answerDataResourceId']),
                                   'answer_content': str(item['answerContent'].encode('utf-8')),
                                   # 'answer_created_date': str(item['answerCreatedDate'].encode('utf-8')),
                                    'answer_created_date': item['answerCreatedDate'],

                                   # 'answer_updated_date': str(item['answerUpdatedDate'].encode('utf-8')),
                                   'answer_updated_date': item['answerUpdatedDate'],
                                   'answer_comment_count': int(item['answerCommentCount']),
                                   'answer_author_link_id': str(item['answerAuthorLinkId'].encode('utf-8')),
                                   'answer_author_img_link': str(item['answerAuthorImgLink']),
                                   'answer_author_name': str(item['answerAuthorName'].encode('utf-8')),
                                   'answer_author_bio': str(item['answerAuthorBio'].encode('utf-8')),
                                   }
                    try:
                        self.col_answer_info.insert_one(answerDetailDict)
                        # self.redis11.hsetnx(str(questionId),quesDetailDict)
                        # self.redis4.lset(str(item['answerDataToken']),0,str(recordTimestamp))
                    except Exception,e:
                        logging.warning('Error with put answerInfo into mongo: '+str(e)+' try again......')
                        try:
                            self.col_answer_info.insert_one(answerDetailDict)
                            # self.redis4.lset(str(item['answerDataToken']),0,str(recordTimestamp))
                            logging.warning('tried again and successfully put data into mongo ......')
                        except Exception,e:
                            logging.warning('Error with put answerInfo into mongo: '+str(e)+'tried again and failed')
                            logging.error('The item is %s',str(item))
            return item
        else:
            return item


class AnswerCommentPipeline(object):

    def __init__(self):

        # self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        # connection = happybase.Connection(settings.HBASE_HOST)
        # self.commentTable = connection.table('comment')
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_answer_comment = db['answerComment']

    def process_item(self, item, spider):
        if item['spiderName'] == 'answerComment':
            answerDataToken = str(item['answerDataToken'])
            #如果有返回数据，即有评论
            if answerDataToken:
                #这里假定了问题的评论和答案的评论的dataid是不会重复的
                # self.redis11.sadd(str(answerDataToken),str(item['commentDataId']))
                # if item['userLinkId']:
                #     self.redis3.sadd('userLinkIdSet',item['userLinkId'])
                #这里并没有必要在redis里缓存评论的相关数据信息，因此并不做比较，而是每次更新hbase
                commentDict={'answer_data_token':str(answerDataToken),
                                'comment_data_id':str(item['commentDataId']),
                               'comment_content':str(item['commentContent'].encode('utf-8')),
                                #日期可能含有中文
                               # 'comment_date': str(item['commentDate'].encode('utf-8')),
                               'comment_date': item['commentDate'],
                               'comment_voter_count': int(item['commentUpCount']),
                               'comment_author_name': item['userName'].encode('utf-8'),
                               'comment_author_link_id': item['userLinkId'].encode('utf-8'),
                               'comment_author_img_link': str(item['userImgLink']),
                                'type':'a'
                               }
                try:
                   self.col_answer_comment.insert_one(commentDict)
                except Exception,e:
                    logging.warning('Error with put commentDict into mongo: '+str(e)+' try again......')
                    try:
                        self.col_answer_comment.insert_one(commentDict)
                        logging.warning('tried again and successfully put data into mongo ......')
                    except Exception,e:
                        logging.warning('Error with put commentDict into mongo: '+str(e)+'tried again and failed')
            return item
        else:
            return item

class AnswerVoterPipeline(object):

    def __init__(self):
        #redis3存放用户索引，linkid，dataid，index
        # self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_answer_voter = db['answerVoter']
        self.col_user_link_id = db['userLinkId']
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'answerVoter':
            if item['answerDataToken'] and item['userDataId']:
                answer_voter_dict = {
                    'answer_data_token': str(item['answerDataToken']),
                    'user_data_id':str(item['userDataId'])
                }
                user_link_id_dict = {
                    'user_link_id':str(item['userLinkId'].encode('utf-8'))
                }
                #userLinkId可能有中文
                # self.redis11.sadd(str(item['answerDataToken']),str(item['userDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['userLinkId'].encode('utf-8')))
                self.col_answer_voter.insert_one(answer_voter_dict)
                self.col_user_link_id.insert_one(user_link_id_dict)
            DropItem()
        else:
            DropItem()