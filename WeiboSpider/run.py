from scrapy import cmdline


cmd = 'scrapy crawl WeiboSpider -a uid=1560906700'
cmdline.execute(cmd.split())