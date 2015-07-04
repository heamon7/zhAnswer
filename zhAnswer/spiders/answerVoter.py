# -*- coding: utf-8 -*-
import scrapy
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request,FormRequest
from scrapy.selector import Selector
from scrapy.shell import inspect_response

from datetime import datetime
import re
import json
import redis
import happybase
import requests
import logging

from zhAnswer import settings
from zhAnswer.items import AnswerVoterItem



class AnswervoterSpider(scrapy.Spider):
    name = "answerVoter"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    baseUrl = 'http://www.zhihu.com/answer/%s/voters_profile?total=%s&offset=%s'

    answerDataTokenList = []
    answerDataIdList = []
    answerVoterCountList = []

    quesIndex =0
    reqLimit =10  # 后面请求的pagesize
    pipelineLimit = 100000
    threhold = 100
    handle_httpstatus_list = [401,429,500,502,504]


    def __init__(self,spider_type='Master',spider_number=0,partition=1,**kwargs):


        self.redis4 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=4)

        self.spider_type = str(spider_type)
        self.spider_number = int(spider_number)
        self.partition = int(partition)
        self.email= settings.EMAIL_LIST[self.spider_number]
        self.password=settings.PASSWORD_LIST[self.spider_number]

    def start_requests(self):

        # 如何获取到answeanswerDataIdListrId和answerVoterCount需要修改

        self.answerDataTokenList = self.redis4.keys()
        totalLength = len(self.answerDataTokenList)


        p4 = self.redis4.pipeline()

        if self.spider_type=='Master':
            redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
            redis11.flushdb()
            logging.warning('Master spider_type is '+self.spider_type)
            if self.partition!=1:
                logging.warning('Master partition is '+str(self.partition))
                self.answerDataTokenList = self.answerDataTokenList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.answerDataTokenList)
                for index ,answerDataToken in enumerate(self.answerDataTokenList):
                    p4.range(str(answerDataToken),3,4)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p4.execute()
                        self.answerVoterCountList.extend(result[::2])
                        self.answerDataIdList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p4.execute()
                        self.answerVoterCountList.extend(result[::2])
                        self.answerDataIdList.extend(result[1::2])

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
                    p4.range(str(answerDataToken),3,4)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p4.execute()
                        self.answerVoterCountList.extend(result[::2])
                        self.answerDataIdList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p4.execute()
                        self.answerVoterCountList.extend(result[::2])
                        self.answerDataIdList.extend(result[1::2])

        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.answerDataTokenList = self.answerDataTokenList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.answerDataTokenList)
                for index ,answerDataToken in enumerate(self.answerDataTokenList):
                    p4.range(str(answerDataToken),3,4)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p4.execute()
                        self.answerVoterCountList.extend(result[::2])
                        self.answerDataIdList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p4.execute()
                        self.answerVoterCountList.extend(result[::2])
                        self.answerDataIdList.extend(result[1::2])


            else:
                self.answerDataTokenList = self.answerDataTokenList[self.spider_number*totalLength/self.partition:]
                totalLength = len(self.answerDataTokenList)
                for index ,answerDataToken in enumerate(self.answerDataTokenList):
                    p4.range(str(answerDataToken),3,4)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p4.execute()
                        self.answerVoterCountList.extend(result[::2])
                        self.answerDataIdList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p4.execute()
                        self.answerVoterCountList.extend(result[::2])
                        self.answerDataIdList.extend(result[1::2])

        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))

        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.answerDataIdList)))
        logging.warning('totalCount answerVoterCountList to request is :'+str(len(self.answerVoterCountList)))



        for index ,answerDataId in enumerate(self.answerDataIdList):

            answerDataToken=self.answerDataTokenList[index]
            reqTimes = (int(self.answerVoterCountList[index])+self.reqLimit-1)/self.reqLimit
            for index in reversed(range(reqTimes)):
                offset =str(self.reqLimit*index)
                reqUrl = self.baseUrl %(str(answerDataId),str(self.answerVoterCountList[index]),str(offset))
                yield Request(url =reqUrl
                              ,meta= {'answerDataId':answerDataId,
                                      'offset':offset,
                                      'answerDataToken':answerDataToken}
                                  ,callback=self.parsePage
                                  )






    def parsePage(self,response):
        if response.status != 200:
            yield Request(url =response.request.url
                           ,meta= {'answerDataId':response.meta['answerDataId'],
                                   'offset':response.meta['offset'],
                                   'answerDataToken':response.meta['answerDataToken']}
                            ,callback=self.parsePage
                                      )
        else:
            item =  AnswerVoterItem()
            data = json.loads(response.body)
            voterList = data['payload']

            item['spiderName'] = self.name
            #这里注意要处理含有匿名用户的情况
            if voterList:

                res = Selector(text = ''.join(voterList))

                item['answerDataId'] = response.meta['answerDataId']
                item['answerDataId'] = response.meta['answerDataToken']
                item['offset'] = response.meta['offset']


                for sel in res.xpath('//div[contains(@class,"zm-profile-card")]'):

                    try:
                        item['userDataId'] = sel.xpath('div[@class="zg-right"]/button/@data-aid').extract()[0]
                        item['userLinkId'] = sel.xpath('a[contains(@class,"zm-item-link-avatar")]/@href').re(r'/people/(.+)')[0]
                    except:
                        item['userDataId'] = ''
                    yield item
                    #工作量太大，暂时并不需要这些详细信息，这需要Link，然后由userInfo的crawler抓取

                    # item['userImgLink'] = sel.xpath('a[contains(@class,"zm-item-link-avatar")]/img/@href').extract()[0]
                    #
                    # item['userName'] = sel.xpath('div[@class="body"]//a[@class="zg-link"]/text()').extract()[0]
                    # item['userAgreeCount'] = sel.xpath('div[@class="body"]//ul[@class="status"]/li[1]/span/text()').re('(\d+)')[0]
                    # item['userThanksCount'] = sel.xpath('div[@class="body"]//ul[@class="status"]/li[2]/span/text()').re('(\d+)')[0]
                    # item['userAskCount'] = sel.xpath('div[@class="body"]//ul[@class="status"]/li[3]/a/text()').re('(\d+)')[0]
                    # item['userAnswerCount'] = sel.xpath('div[@class="body"]//ul[@class="status"]/li[4]/a/text()').re('(\d+)')[0]

            else:
                #没有用户
                item['answerDataToken']=''
                yield item





    #
    #
    def closed(self,reason):


        redis15 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=15)
        redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)


        #这样的顺序是为了防止两个几乎同时结束
        p15=redis15.pipeline()
        p15.lpush(str(self.name),self.spider_number)
        p15.llen(str(self.name))
        finishedCount= p15.execute()[1]
        pipelineLimit = 100000
        batchLimit = 1000

        if int(self.partition)==int(finishedCount):
            #删除其他标记
            redis15.ltrim(str(self.name),0,0)

            connection = happybase.Connection(settings.HBASE_HOST)
            answerTable = connection.table('answer')

            answerDataTokenList = redis11.keys()
            p11 = redis11.pipeline()
            tmpAnswerList = []
            totalLength = len(answerDataTokenList)

            for index, answerDataToken in enumerate(answerDataTokenList):
                p11.smembers(str(answerDataToken))
                tmpAnswerList.append(str(answerDataToken))

                if (index + 1) % pipelineLimit == 0:
                    answerVoterDataIdSetList = p11.execute()
                    with  answerTable.batch(batch_size=batchLimit):
                        for innerIndex, answerVoterDataIdSet in enumerate(answerVoterDataIdSetList):

                            answerTable.put(str(tmpAnswerList[innerIndex]),
                                              {'voter:dataIdList': str(list(answerVoterDataIdSet))})
                        tmpAnswerList=[]


                elif  totalLength - index == 1:
                    answerVoterDataIdSetList = p11.execute()
                    with  answerTable.batch(batch_size=batchLimit):
                        for innerIndex, answerVoterDataIdSet in enumerate(answerVoterDataIdSetList):

                            answerTable.put(str(tmpAnswerList[innerIndex]),
                                              {'voter:dataIdList': str(list(answerVoterDataIdSet))})
                        tmpAnswerList=[]
            #清空队列
            redis15.rpop(self.name)
            #清空缓存数据的redis11数据库
            redis11.flushdb()

            payload=settings.NEXT_SCHEDULE_PAYLOAD
            logging.warning('Begin to request next schedule')
            response = requests.post('http://'+settings.NEXT_SCHEDULE_SCRAPYD_HOST+':'+settings.NEXT_SCHEDULE_SCRAPYD_PORT+'/schedule.json',data=payload)
            logging.warning('Response: '+' '+str(response))
        logging.warning('finished close.....')

