# -*- coding: utf-8 -*-

# Scrapy settings for zhAnswer project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'zhAnswer'

SPIDER_MODULES = ['zhAnswer.spiders']
NEWSPIDER_MODULE = 'zhAnswer.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'zhAnswer (+http://www.yourdomain.com)'

DOWNLOAD_TIMEOUT = 700

LOG_LEVEL = 'INFO'

DEFAULT_REQUEST_HEADERS = {
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
           'Accept-Encoding': 'gzip, deflate, sdch',
           'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,zh-TW;q=0.2',
           'Connection': 'keep-alive',
           'Host': 'www.zhihu.com',
           'Referer': 'http://www.zhihu.com/',

}

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36'

EXTENSIONS = {
    # 'scrapy.contrib.feedexport.FeedExporter': None,
    'scrapy.extensions.feedexport.FeedExporter': None

}

ITEM_PIPELINES = {
    'zhAnswer.pipelines.AnswerInfoPipeline': 300,
    'zhAnswer.pipelines.AnswerCommentPipeline': 400,
    'zhAnswer.pipelines.AnswerVoterPipeline': 500,


}
SPIDER_MIDDDLEWARES = {
    'scrapy.contrib.spidermiddleware.httperror.HttpErrorMiddleware':300,
}

DUPEFILTER_CLASS = 'zhAnswer.custom_filters.SeenURLFilter'



INFO_UPDATE_PERIOD = '432000' #最快5天更新一次