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

from zhAnswer.items import AnswerCommentItem
from pymongo import MongoClient



class AnswercommentSpider(scrapy.Spider):
    name = "answerComment"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    baseUrl = 'http://www.zhihu.com/node/AnswerCommentListV2?params={"answer_id":"%s"}'

    answerDataTokenList = []
    answerDataIdList = []
    questionAnswerCountList = []
    
    quesIndex =0
    reqLimit =50  # 后面请求的pagesize
    pipelineLimit = 100000
    threhold = 100
    handle_httpstatus_list = [401,429,500,502,504]
    # params= '{"url_token":%s,"pagesize":%s,"offset":%s}'
    ANSWER_DATA_ID_INDEX = settings.ANSWER_DATA_ID_INDEX

    def __init__(self,stats,spider_type='Master',spider_number=0,partition=1,**kwargs):
        self.stats = stats

        # redis2 以list的形式存储有所有问题的id和问题的info，包括answerCount
        self.redis_client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=settings.ANSWER_INFO_REDIS_DB_NUMBER)
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

        self.answerDataTokenList = self.redis_client.keys()
        totalLength = len(self.answerDataTokenList)


        p4 =self.redis_client.pipeline()
        if self.spider_type=='Master':
            redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
            redis11.flushdb()

            logging.warning('Master spider_type is '+self.spider_type)
            if self.partition!=1:
                logging.warning('Master partition is '+str(self.partition))
                self.answerDataTokenList = self.answerDataTokenList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.answerDataTokenList)
                for index ,answerDataToken in enumerate(self.answerDataTokenList):
                    p4.lindex(str(answerDataToken),self.ANSWER_DATA_ID_INDEX)
                    if (index+1)%self.pipelineLimit ==0:
                        self.answerDataIdList.extend(p4.execute())
                    elif totalLength-index==1:
                        self.answerDataIdList.extend(p4.execute())

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
                for index ,answerDataToken in enumerate(self.answerDataTokenList):
                    p4.lindex(str(answerDataToken),self.ANSWER_DATA_ID_INDEX)
                    if (index+1)%self.pipelineLimit ==0:
                        self.answerDataIdList.extend(p4.execute())
                    elif totalLength-index==1:
                        self.answerDataIdList.extend(p4.execute())

        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.answerDataTokenList = self.answerDataTokenList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.answerDataTokenList)
                for index ,answerDataToken in enumerate(self.answerDataTokenList):
                    p4.lindex(str(answerDataToken),self.ANSWER_DATA_ID_INDEX)
                    if (index+1)%self.pipelineLimit ==0:
                        self.answerDataIdList.extend(p4.execute())
                    elif totalLength-index==1:
                        self.answerDataIdList.extend(p4.execute())


            else:
                self.answerDataTokenList = self.answerDataTokenList[self.spider_number*totalLength/self.partition:]
                totalLength = len(self.answerDataTokenList)
                for index ,answerDataToken in enumerate(self.answerDataTokenList):
                    p4.lindex(str(answerDataToken),self.ANSWER_DATA_ID_INDEX)
                    if (index+1)%self.pipelineLimit ==0:
                        self.answerDataIdList.extend(p4.execute())
                    elif totalLength-index==1:
                        self.answerDataIdList.extend(p4.execute())

        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))

        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.answerDataIdList)))
        for index ,answerDataId in enumerate(self.answerDataIdList):
                reqUrl = self.baseUrl %str(answerDataId)
                answerDataToken = self.answerDataTokenList[index]
                yield Request(url =reqUrl
                                      ,meta={'answerDataId':answerDataId,
                                             'answerDataToken':answerDataToken
                                             }

                                      ,callback=self.parsePage
                                      )



    def parsePage(self,response):
        if response.status != 200:
             yield Request(url = response.request.url
                           ,meta={'answerDataId':response.meta['answerDataId'],
                                  'answerDataToken':response.meta['answerDataToken']}
                           ,callback=self.parsePage)
        else:
            item =  AnswerCommentItem()
            item['spiderName'] = self.name
            sels=response.xpath('//div[@class="zm-item-comment"]')
            if sels:

                item['answerDataId'] = response.meta['answerDataId']
                item['answerDataToken'] = response.meta['answerDataToken']
                for sel in sels:
                    # 注意，这里因为没有userDataId,因此其他的辅助信息也是有价值的
                    # 另外这里的结构和questionComment的结构完全一样
                    item['commentDataId'] = str(sel.xpath('@data-id').extract()[0])

                    item['commentContent'] =sel.xpath('div[@class="zm-comment-content-wrap"]/div[@class="zm-comment-content"]/text()').extract()[0]
                    item['commentDate'] = sel.xpath('div[@class="zm-comment-content-wrap"]/div[@class="zm-comment-ft"]/span[@class="date"]/text()').extract()[0]
                    item['commentUpCount'] = sel.xpath('div[@class="zm-comment-content-wrap"]/div[@class="zm-comment-ft"]/span[contains(@class,"like-num")]/em/text()').extract()[0]

                    try:
                        item['userLinkId'] = sel.xpath('a[@class="zm-item-link-avatar"]/@href').re(r'/people/(.*)')[0]
                    except:
                        item['userLinkId'] = ''

                    try:
                        item['userName'] = sel.xpath('a[@class="zm-item-link-avatar"]/@title').extract()[0]
                    except:
                        item['userName'] = ''

                    try:
                        item['userImgLink'] = sel.xpath('a[@class="zm-item-link-avatar"]/img/@src').extract()[0]
                    except:
                        item['userImgLink'] = ''






                    yield item
            else:
                item['answerDataToken'] =''
                yield  item



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
        #     answerTable = connection.table('answer')
        #
        #     answerDataTokenList = redis11.keys()
        #     p11 = redis11.pipeline()
        #     tmpAnswerList = []
        #     totalLength = len(answerDataTokenList)
        #
        #     for index, answerDataToken in enumerate(answerDataTokenList):
        #         p11.smembers(str(answerDataToken))
        #         tmpAnswerList.append(str(answerDataToken))
        #
        #         if (index + 1) % pipelineLimit == 0:
        #             answerCommentDataIdSetList = p11.execute()
        #             with  answerTable.batch(batch_size=batchLimit):
        #                 for innerIndex, answerCommentDataIdSet in enumerate(answerCommentDataIdSetList):
        #
        #                     answerTable.put(str(tmpAnswerList[innerIndex]),
        #                                       {'comment:dataTokenList': str(list(answerCommentDataIdSet))})
        #                 tmpAnswerList=[]
        #
        #
        #         elif  totalLength - index == 1:
        #             answerCommentDataIdSetList = p11.execute()
        #             with  answerTable.batch(batch_size=batchLimit):
        #                 for innerIndex, answerCommentDataIdSet in enumerate(answerCommentDataIdSetList):
        #                     answerTable.put(str(tmpAnswerList[innerIndex]),
        #                                       {'comment:dataTokenList': str(list(answerCommentDataIdSet))})
        #                 tmpAnswerList=[]
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



