# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy



class AnswerInfoItem(scrapy.Item):


    spiderName = scrapy.Field()
    questionId = scrapy.Field()
    offset = scrapy.Field()
    answerDataId = scrapy.Field()
    answerDataToken = scrapy.Field()
    answerDataCreated = scrapy.Field()
    answerDataDeleted = scrapy.Field()
    answerDataHelpful = scrapy.Field()
    answerVoterCount = scrapy.Field()
    answerDataResourceId = scrapy.Field()
    answerContent = scrapy.Field()
    answerCreatedDate = scrapy.Field()
    answerUpdatedDate = scrapy.Field()
    answerCommentCount = scrapy.Field()
    answerAuthorLinkId = scrapy.Field()
    answerAuthorImgLink = scrapy.Field()
    answerAuthorName = scrapy.Field()
    answerAuthorBio = scrapy.Field()





class AnswerCommentItem(scrapy.Item):
    spiderName = scrapy.Field()
    answerDataId = scrapy.Field()
    answerDataToken = scrapy.Field()
    commentDataId = scrapy.Field()
    commentContent = scrapy.Field()
    commentDate = scrapy.Field()
    commentUpCount = scrapy.Field()
    userLinkId = scrapy.Field()
    userName = scrapy.Field()
    userImgLink = scrapy.Field()


    pass

class AnswerVoterItem(scrapy.Item):



    spiderName = scrapy.Field()

    offset = scrapy.Field()
    answerDataId = scrapy.Field()
    answerDataToken = scrapy.Field()
    userDataId = scrapy.Field()
    userLinkId = scrapy.Field()
    userImgUrl = scrapy.Field()
    userName = scrapy.Field()









