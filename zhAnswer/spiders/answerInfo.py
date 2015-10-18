# -*- coding: utf-8 -*-
import scrapy
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request,FormRequest
from scrapy.selector import Selector
from scrapy.shell import inspect_response

import datetime
import re
import json
import redis
import happybase
import requests
import logging

from zhAnswer import settings

from zhAnswer.items import AnswerInfoItem
from pymongo import MongoClient



class AnswerinfoSpider(scrapy.Spider):
    name = "answerInfo"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    baseUrl = 'http://www.zhihu.com/node/QuestionAnswerListV2'

    questionIdList = []
    questionAnswerCountList = []

    quesIndex =0
    reqLimit =50  # 后面请求的pagesize
    pipelineLimit = 100000
    threhold = 100
    handle_httpstatus_list = [401,429,500,502,504]
    params= '{"url_token":%s,"pagesize":%s,"offset":%s}'
    QUES_ANSWER_COUNT_INDEX = settings.QUES_ANSWER_COUNT_INDEX

    def __init__(self,stats,spider_type='Master',spider_number=0,partition=1,**kwargs):
        self.stats = stats

        # redis2 以list的形式存储有所有问题的id和问题的info，包括answerCount
        self.redis2 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=settings.QUESTION_INFO_REDIS_DB_NUMBER)
        self.client = MongoClient(settings.MONGO_URL)
        self.db = self.client['zhihu']
        self.col_log = self.db['log']

        crawler_log = {'project':settings.BOT_NAME,
                       'spider':self.name,
                       'spider_type':spider_type,
                       'spider_number':spider_number,
                       'partition':partition,
                       'type':'start',
                       'updated_at':datetime.datetime.now()}

        self.col_log.insert_one(crawler_log)
        try:
            self.spider_type = str(spider_type)
            self.spider_number = int(spider_number)
            self.partition = int(partition)
            # self.email= settings.EMAIL_LIST[self.spider_number]
            # self.password=settings.PASSWORD_LIST[self.spider_number]

        except:
            self.spider_type = 'Master'
            self.spider_number = 0
            self.partition = 1
            # self.email= settings.EMAIL_LIST[self.spider_number]
            # self.password=settings.PASSWORD_LIST[self.spider_number]

    @classmethod
    def from_crawler(cls, crawler,spider_type='Master',spider_number=0,partition=1,**kwargs):
        return cls(crawler.stats,spider_type=spider_type,spider_number=spider_number,partition=partition)

    def start_requests(self):

        self.questionIdList = self.redis2.keys()
        totalLength = len(self.questionIdList)

        p2 = self.redis2.pipeline()
        # cursor = self.db['questionInfo'].find()

        if self.spider_type=='Master':
            redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
            redis11.flushdb()
            logging.warning('Master spider_type is '+self.spider_type)
            if self.partition!=1:
                logging.warning('Master partition is '+str(self.partition))
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.questionIdList)
                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),self.QUES_ANSWER_COUNT_INDEX)
                    if (index+1)%self.pipelineLimit ==0:
                        self.questionAnswerCountList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionAnswerCountList.extend(p2.execute())

                for index in range(1,self.partition):
                    payload ={
                        'project':settings.BOT_NAME
                        ,'spider':self.name
                        ,'spider_type':'Slave'
                        ,'spider_number':index
                        ,'partition':self.partition
                        ,'setting':'JOBDIR=/tmp/scrapy/'+self.name+str(index)
                    }
                    logging.warning('Begin to request'+str(index))
                    response = requests.post('http://'+settings.SCRAPYD_HOST_LIST[index]+':'+settings.SCRAPYD_PORT_LIST[index]+'/schedule.json',data=payload)
                    logging.warning('Response: '+str(index)+' '+str(response))
            else:
                logging.warning('Master  partition is '+str(self.partition))
                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),self.QUES_ANSWER_COUNT_INDEX)
                    if (index+1)%self.pipelineLimit ==0:
                        self.questionAnswerCountList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionAnswerCountList.extend(p2.execute())

        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.questionIdList)
                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),self.QUES_ANSWER_COUNT_INDEX)
                    if (index+1)%self.pipelineLimit ==0:
                        self.questionAnswerCountList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionAnswerCountList.extend(p2.execute())


            else:
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:]
                totalLength = len(self.questionIdList)
                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),self.QUES_ANSWER_COUNT_INDEX)
                    if (index+1)%self.pipelineLimit ==0:
                        self.questionAnswerCountList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionAnswerCountList.extend(p2.execute())

        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))

        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.questionIdList)))
        logging.warning('totalCount questionAnswerCountList to request is :'+str(len(self.questionAnswerCountList)))
        # yield Request("http://www.zhihu.com/",callback = self.post_login)
        yield Request(url ='http://www.zhihu.com',
                      cookies=settings.COOKIES_LIST[self.spider_number],
                      callback =self.after_login)
    # def post_login(self,response):
    #
    #     logging.warning('post_login ing ......')
    #     xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]
    #     yield FormRequest.from_response(response,
    #                                       formdata={
    #                                           '_xsrf':xsrfValue,
    #                                           'email':self.email,
    #                                           'password':self.password,
    #                                           'rememberme': 'y'
    #                                       },
    #                                       dont_filter = True,
    #                                       callback = self.after_login,
    #                                       )

    def after_login(self,response):
        # try:
        #     loginUserLink = response.xpath('//div[@id="zh-top-inner"]/div[@class="top-nav-profile"]/a/@href').extract()[0]
        #     logging.warning('Successfully login with %s  %s  %s',str(loginUserLink),str(self.email),str(self.password))
        # except:
        #     logging.error('Login failed! %s   %s',self.email,self.password)
        try:
            loginUserLink = response.xpath('//div[@id="zh-top-inner"]/div[@class="top-nav-profile"]/a/@href').extract()[0]
            # logging.warning('Successfully login with %s  %s  %s',str(loginUserLink),str(self.email),str(self.password))
            logging.warning('Successfully login with %s  ',str(loginUserLink))

        except:
            logging.error('Login failed! %s',self.email)

        for index ,questionId in enumerate(self.questionIdList):

            xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]
            reqUrl = self.baseUrl
            reqTimes = (int(self.questionAnswerCountList[index])+self.reqLimit-1)/self.reqLimit
            for index in reversed(range(reqTimes)):
                offset =str(self.reqLimit*index)
                reqParams = self.params %(str(questionId),str(self.reqLimit),str(offset))
                yield FormRequest(url =reqUrl
                                  ,meta={'params':reqParams,
                                         'xsrfValue':xsrfValue,
                                         'questionId':questionId,
                                         'offset':offset}
                                  , formdata={
                                        'method':'next'
                                        ,'params':reqParams
                                        ,'_xsrf': xsrfValue

                                    }
                                  ,dont_filter=True
                                  ,callback=self.parsePage
                                  )


    def parsePage(self,response):
        if response.status != 200:
            yield FormRequest(url =response.request.url
                                      ,meta={'params':response.meta['params']
                                             ,'xsrfValue':response.meta['xsrfValue']
                                             ,'questionId':response.meta['questionId']
                                             ,'offset':response.meta['offset']}
                                      , formdata={
                                            'method': 'next'
                                            ,'params':response.meta['params']
                                            ,'_xsrf': response.meta['xsrfValue']
                                        }
                                      ,dont_filter=True
                                      ,callback=self.parsePage
                                      )
        else:
            item =  AnswerInfoItem()
            data = json.loads(response.body)
            answerList = data['msg']

            item['spiderName'] = self.name
            #这里注意要处理含有匿名用户的情况
            if answerList:

                res = Selector(text = ''.join(answerList))
                item['questionId'] = response.meta['questionId']
                item['offset'] = response.meta['offset']


                for sel in res.xpath('//div[contains(@class,"zm-item-answer ")]'):

                    #请求赞同列表时，使用的是data-aid
                    #请求评论列表时，使用的是data-aid
                    item['answerDataId'] = sel.xpath('@data-aid').extract()[0]
                    # 答案的id是Data-atoken
                    item['answerDataToken'] = sel.xpath('@data-atoken').extract()[0]
                    item['answerDataCreated'] = sel.xpath('@data-created').extract()[0]
                    item['answerDataDeleted'] = sel.xpath('@data-deleted').extract()[0]
                    item['answerDataHelpful'] = sel.xpath('@data-helpful').extract()[0]
                    item['answerVoterCount'] = sel.xpath('div[@class="zm-votebar"]//span[@class="count"]/text()').extract()[0]
                    #data-resource-id是答案的内容id，和问题的data-resource-id是一致的

                    item['answerContent'] = '\n\n'.join(sel.xpath('div[@class="zm-item-rich-text"]/div[contains(@class,"zm-editable-content")]/text()').extract())

                    #可能是被建议修改的回答
                    try:
                        item['answerDataResourceId'] = sel.xpath('div[@class="zm-item-rich-text"]/@data-resourceid').extract()[0]
                    except:
                        item['answerDataResourceId'] =''
                        logging.error('Error in answerDataResourceId with QuestionId: %s ,AnswerDataToken: %s ',str(item['questionId']),str(item['answerDataToken']))
                    # 注意userLinkId中可能有中文
                    try:
                        item['answerCreatedDate'] = sel.xpath('div[contains(@class,"zm-item-comment-el")]//a[contains(@class,"answer-date-link")]/@data-tip').re(r'([\-0-9\:]+)')[0]
                    except:

                        item['answerCreatedDate'] = ''

                    try:
                        item['answerUpdatedDate'] = sel.xpath('div[contains(@class,"zm-item-comment-el")]//a[contains(@class,"answer-date-link")]/text()').re(r'([\-0-9\:]+)')[0]
                    except Exception,e:
                        item['answerUpdatedDate'] = ''
                        logging.error('Error in item[answerUpdatedDate] %s',str(e))
                    try:
                        item['answerCommentCount'] = sel.xpath('div[contains(@class,"zm-item-comment-el")]//a[@name="addcomment"]/text')[1].re(r'(\d*)')[0]
                    except:
                        item['answerCommentCount'] =0

                          #这些信息并不需要
                    try:
                        item['answerAuthorLinkId'] = sel.xpath('div[@class="answer-head"]//h3/a[@class="zm-item-link-avatar"]/@href').re(r'/people/(.*)')[0]
                        item['answerAuthorImgLink'] = sel.xpath('div[@class="answer-head"]//h3/a[@class="zm-item-link-avatar"]/img/@src').extract()[0]
                        item['answerAuthorName'] = sel.xpath('div[@class="answer-head"]//h3/a[2]/text()').extract()[0]
                        item['answerAuthorBio'] = sel.xpath('div[@class="answer-head"]//h3/strong/text()').extract()[0]

                    except:
                        item['answerAuthorLinkId'] = ''
                        item['answerAuthorImgLink'] = ''
                        item['answerAuthorName'] = ''
                        item['answerAuthorBio'] = ''


                    yield item

            else:
                #没有用户
                item['questionId']=''
                yield item





    #
    #
    def closed(self,reason):

        self.client = MongoClient(settings.MONGO_URL)
        self.db = self.client['zhihu']
        self.col_log = self.db['log']

        crawler_log = {'project':settings.BOT_NAME,
                       'spider':self.name,
                       'spider_type':self.spider_type,
                       'spider_number':self.spider_number,
                       'partition':self.partition,
                       'type':'close',
                       'stats':self.stats.get_stats(),
                       'updated_at':datetime.datetime.now()}

        self.col_log.insert_one(crawler_log)

        # redis15 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=15)
        # redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        #
        #
        # #这样的顺序是为了防止两个几乎同时结束
        # p15=redis15.pipeline()
        # p15.lpush(str(self.name),self.spider_number)
        # p15.llen(str(self.name))
        # finishedCount= p15.execute()[1]
        # pipelineLimit = 100000
        # batchLimit = 1000
        #
        # if int(self.partition)==int(finishedCount):
        #     #删除其他标记
        #     redis15.ltrim(str(self.name),0,0)
        #
        #     connection = happybase.Connection(settings.HBASE_HOST)
        #     questionTable = connection.table('question')
        #
        #     questionIdList = redis11.keys()
        #     p11 = redis11.pipeline()
        #     tmpQuestionList = []
        #     totalLength = len(questionIdList)
        #
        #     for index, questionId in enumerate(questionIdList):
        #         p11.smembers(str(questionId))
        #         tmpQuestionList.append(str(questionId))
        #
        #         if (index + 1) % pipelineLimit == 0:
        #             questionAnswerDataTokenSetList = p11.execute()
        #             with  questionTable.batch(batch_size=batchLimit):
        #                 for innerIndex, questionAnswerDataTokenSet in enumerate(questionAnswerDataTokenSetList):
        #
        #                     questionTable.put(str(tmpQuestionList[innerIndex]),
        #                                       {'answer:dataTokenList': str(list(questionAnswerDataTokenSet))})
        #                 tmpQuestionList=[]
        #
        #
        #         elif  totalLength - index == 1:
        #             questionAnswerDataTokenSetList = p11.execute()
        #             with  questionTable.batch(batch_size=batchLimit):
        #                 for innerIndex, questionAnswerDataTokenSet in enumerate(questionAnswerDataTokenSetList):
        #
        #                     questionTable.put(str(tmpQuestionList[innerIndex]),
        #                                       {'answer:dataTokenList': str(list(questionAnswerDataTokenSet))})
        #                 tmpQuestionList=[]
        #     #清空队列
        #     redis15.rpop(self.name)
        #     #清空缓存数据的redis11数据库
        #     redis11.flushdb()
        #
        #     payload=settings.NEXT_SCHEDULE_PAYLOAD
        #     logging.warning('Begin to request next schedule')
        #     response = requests.post('http://'+settings.NEXT_SCHEDULE_SCRAPYD_HOST+':'+settings.NEXT_SCHEDULE_SCRAPYD_PORT+'/schedule.json',data=payload)
        #     logging.warning('Response: '+' '+str(response))
        # logging.warning('finished close.....')


