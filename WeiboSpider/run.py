from scrapy import cmdline


# cmd = 'scrapy crawl WeiboSpider -a uid=6115560351 -s JOBDIR=crawls/WeiboSpider-1'
# cmd = 'scrapy crawl WeiboSpider -a uid=1560906700'
cmd = 'scrapy crawl FansListSpider -a uids=1560906700'
cmdline.execute(cmd.split())