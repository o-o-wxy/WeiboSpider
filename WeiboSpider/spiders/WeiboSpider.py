# -*- coding: utf-8 -*-
# @Author  : CharesFuns
# @Time    : 2020/4/26 16:05
# @Function: A crawler for Sina Weibo

import re
import json
import scrapy
import logging
from lxml import etree
from ..items import *
import random


class WeiboSpider(scrapy.Spider):
    # init parameters
    name = 'WeiboSpider'
    allowed_domains = ['m.weibo.cn', 'weibo.com']  # crawling sites
    handle_httpstatus_list = [403]  # http status code for not ignoring

    deepth = 3

    cookies = [{
        "WEIBOCN_FROM": "1110006030",
        "_T_WM": "38936464962",
        "SCF": "AnMPR4Jx_9PHDwrZ9ZGs75MAqHG51-wxw7z_t5tSv8KyDsm6F07AeeI4DFFAdppe6UnbZ0GTGMJEcm5PI3gu5d8.",
        "SUB": "_2A25NtaFuDeRhGeNG7lIR9C7Pwz2IHXVvWc8mrDV6PUJbktB-LWf5kW1NS1JJegWJ4-W0s0B892myakbIVf_9tYsu",
        "SUBP": "0033WrSXqPxfM725Ws9jqgMF55529P9D9Wh-4Cmbf-fbvDJ3izNIIU-55NHD95Qf1h-7ehB7e0npWs4Dqcjdi--fi-iFiKn4i--Xi-z4iK.7i--Ni-i8i-is",
        "SSOLoginState": "1622266174",
        "MLOGIN": "1",
        "XSRF-TOKEN": "440528",
        "M_WEIBOCN_PARAMS": "featurecode=10000326&luicode=10000011&lfid=231016_-_selffans&fid=1076036692258223&uicode=10000011"
    }
    ]

    def __init__(self, uid, page=5, *args, **kwargs):
        super(WeiboSpider, self).__init__(*args, **kwargs)
        self.start_urls = ['https://m.weibo.cn/']
        self.__uid = uid
        self.__user_info_api = {'api_0': 'api/container/getIndex?type=__uid&value=', 'api_1': '&containerid=100505'}
        self.__weibo_info_api = {'api_0': 'api/container/getIndex?type=__uid&value=',
                                 'api_1': '&containerid=107603', 'api_2': '&page=',
                                 'longtext_api': 'https://m.weibo.cn/statuses/extend?id=',
                                 'precise_time_api': 'https://m.weibo.cn/status/'}
        self.__weibo_page_range = int(page)
        self.__comment_api = {'url': 'https://m.weibo.cn/comments/hotflow?id=',
                              'api_0': '&mid=',
                              'api_1': '&max_id=',
                              'api_2': '&max_id_type=0'}

    def start_requests(self):
        # start of the crawler

        user_info_url = self.crawling_user_info(self.__uid)
        yield scrapy.Request(url=user_info_url, callback=self.parse_user, meta={'deepth': 0}, dont_filter=True)

    def crawling_user_info(self, uuid):
        # to generate user's profile information url
        user_info_url = self.start_urls[0] + self.__user_info_api['api_0'] + \
                        str(uuid) + self.__user_info_api['api_1'] + str(uuid)
        return user_info_url

    def crawling_post_info(self, uuid):
        # to generate user's tweet/post/weibo information url
        weibo_info_urls = []
        self.total_flag = 1
        for i in range(1, self.__weibo_page_range + 1):
            weibo_info_url = self.start_urls[0] + self.__weibo_info_api['api_0'] + str(uuid) + \
                             self.__weibo_info_api['api_1'] + str(uuid) + self.__weibo_info_api['api_2'] + str(i)
            weibo_info_urls.append(weibo_info_url)
        return weibo_info_urls

    def crawling_comment_info(self, mid, max_id=""):
        # generate comment url
        if max_id == "":
            return self.__comment_api["url"] + str(mid) + self.__comment_api["api_0"] + str(mid) + self.__comment_api[
                "api_2"]
        else:
            return self.__comment_api["url"] + str(mid) + self.__comment_api["api_0"] + str(mid) + self.__comment_api[
                "api_1"] + str(max_id) + self.__comment_api["api_2"]

    def crawling_comment_child(self, cid, max_id="0"):
        return "https://m.weibo.cn/comments/hotFlowChild?cid=" + str(cid) + "&max_id=" + str(max_id) + "&max_id_type=0"

    def parse_user(self, response):
        # the parser for user profile
        user_info = json.loads(response.text)['data']['userInfo']
        current_deepth = response.meta['deepth']

        if current_deepth > self.deepth:
            return
        else:
            current_deepth = current_deepth + 1
        del user_info['toolbar_menus']
        user_info_item = UserInfoItem()
        user_info_item['user_info'] = user_info
        uuid = user_info['id']
        yield user_info_item
        print("parse_user" + str(uuid))
        # 判断微博数量与粉丝数量筛选用户
        if user_info['followers_count'] > 100 and user_info['statuses_count'] > 100:
            weibo_info_urls = self.crawling_post_info(uuid)
            for weibo_info_url in weibo_info_urls:
                yield scrapy.Request(url=weibo_info_url,
                                     callback=self.parse_post, meta={'uuid': uuid, 'deepth': current_deepth})

    def parse_post(self, response):
        # the parser for user post
        weibo_info = json.loads(response.text)
        current_deepth = response.meta['deepth']
        # cardListInfo = weibo_info['data']['cardlistInfo']
        # crawl the total number of this user
        # total_item = TotalNumItem()
        # total_item['uid'] = response.meta['uuid']
        # total_item['total_num'] = cardListInfo['total']  # total number of user posts
        # yield total_item
        for card in weibo_info['data']['cards']:
            if card['card_type'] == 9:
                # only card_type equals 9, we need
                mblog = card['mblog']
                user_post_item = UserPostItem()
                user_post_item['user_post'] = mblog
                if user_post_item['user_post']['isLongText']:
                    longtext_url = self.__weibo_info_api['longtext_api'] + mblog['id']
                    yield scrapy.Request(url=longtext_url, callback=self.parse_longtext,
                                         meta={'post_item': user_post_item})
                else:
                    precise_time_url = self.__weibo_info_api['precise_time_api'] + mblog['id']
                    yield scrapy.Request(url=precise_time_url, callback=self.parse_precise_time,
                                         meta={'post_item': user_post_item})
                mid = mblog['id']
                # 评论
                comment_url = self.crawling_comment_info(mid=mid)
                yield scrapy.Request(url=comment_url, callback=self.parse_comment,
                                     cookies=random.choice(self.cookies),
                                     meta={'mid': mid, 'deepth': current_deepth})
                # 点赞
                like_url = "https://m.weibo.cn/api/attitudes/show?id=" + str(mid) + "&page=1"
                yield scrapy.Request(url=like_url, callback=self.parse_like,
                                     cookies=random.choice(self.cookies),
                                     meta={'mid': mid, 'page': 1, 'deepth': current_deepth})
                # 转发
                # https://m.weibo.cn/api/statuses/repostTimeline?id=4630601350516324&page=1
                repost_url = "https://m.weibo.cn/api/statuses/repostTimeline?id=" + str(mid) + "&page=1"
                yield scrapy.Request(url=repost_url, callback=self.parse_repost,
                                     cookies=random.choice(self.cookies),
                                     meta={'mid': mid, 'page': 1, 'deepth': current_deepth})

    def parse_like(self, response):
        if response.status == 403:
            logging.error("this is 403 error. request is " + response.request.url)
            logging.error("remove cookies " + response.request.cookies)
            self.cookies.remove(response.request.cookies)
            if not self.cookies:
                self.crawler.engine.close_spider(self, "cookies is null")
            else:
                yield scrapy.Request(url=response.url, callback=self.parse_like,
                                     cookies=random.choice(self.cookies),
                                     meta=response.meta, dont_filter=True)
        if json.loads(response.text)['ok'] == 0:
            return
        data = json.loads(response.text)['data']
        mid = response.meta['mid']
        page = response.meta['page']
        current_deepth = response.meta['deepth']
        for card in data['data']:
            like_user_item = LikeUserItem()
            like_user_item['like_user'] = card
            like_user_item['like_user']['master_id'] = mid
            yield like_user_item
            if card['user']['followers_count'] > 100:
                new_uid = card['user']['id']
                user_info_url = self.crawling_user_info(uuid=new_uid)
                yield scrapy.Request(url=user_info_url,
                                     callback=self.parse_user,
                                     meta={'deepth': current_deepth})
        # 之后的点赞列表
        max_page = data['max']
        if page < max_page:
            page += 1
            like_url = "https://m.weibo.cn/api/attitudes/show?id=" + str(mid) + "&page=" + str(page)
            yield scrapy.Request(url=like_url, callback=self.parse_like,
                                 cookies=random.choice(self.cookies),
                                 meta={'mid': mid, 'page': page, 'deepth': current_deepth})

    def parse_repost(self, response):
        if response.status == 403:
            logging.error("this is 403 error. request is " + response.request.url)
            logging.error("remove cookies " + response.request.cookies)
            self.cookies.remove(response.request.cookies)
            if not self.cookies:
                self.crawler.engine.close_spider(self, "cookies is null")
            else:
                yield scrapy.Request(url=response.url, callback=self.parse_repost,
                                     cookies=random.choice(self.cookies),
                                     meta=response.meta, dont_filter=True)
        if json.loads(response.text)['ok'] == 0:
            return
        data = json.loads(response.text)['data']
        mid = response.meta['mid']
        page = response.meta['page']
        current_deepth = response.meta['deepth']
        for card in data['data']:
            repost_user_item = RepostUserItem()
            repost_user_item['repost_user'] = card
            repost_user_item['repost_user']['master_id'] = mid
            yield repost_user_item
            if card['user']['followers_count'] > 100:
                new_uid = card['user']['id']
                user_info_url = self.crawling_user_info(uuid=new_uid)
                yield scrapy.Request(url=user_info_url,
                                     callback=self.parse_user, meta={'deepth': current_deepth})
        # 之后的转发列表
        max_page = data['max']
        if page < max_page:
            page += 1
            repost_url = "https://m.weibo.cn/api/statuses/repostTimeline?id=" + str(mid) + "&page=" + str(page)
            yield scrapy.Request(url=repost_url, callback=self.parse_repost,
                                 cookies=random.choice(self.cookies),
                                 meta={'mid': mid, 'page': page, 'deepth': current_deepth})

    def parse_comment_child(self, response):
        if response.status == 403:
            logging.error("this is 403 error. request is " + response.request.url)
            logging.error("remove cookies " + response.request.cookies)
            self.cookies.remove(response.request.cookies)
            if not self.cookies:
                self.crawler.engine.close_spider(self, "cookies is null")
            else:
                yield scrapy.Request(url=response.url, callback=self.parse_comment_child,
                                     cookies=random.choice(self.cookies),
                                     meta=response.meta, dont_filter=True)
        if json.loads(response.text)['ok'] == 0:
            return
        data = json.loads(response.text)
        mid = response.meta['mid']
        current_deepth = response.meta['deepth']
        for card in data['data']:
            user_comment_item = CommentItem()
            user_comment_item['user_comment'] = card
            user_comment_item['user_comment']['master_id'] = mid
            yield user_comment_item
            # 判断微博数量与粉丝数量筛选用户
            if card['user']['followers_count'] > 100 and card['user']['statuses_count'] > 100:
                new_uid = card['user']['id']
                user_info_url = self.crawling_user_info(uuid=new_uid)
                yield scrapy.Request(url=user_info_url,
                                     callback=self.parse_user, meta={'deepth': current_deepth})
        # 爬取之后的评论页面
        while True:
            if data['max_id'] == 0:
                break
            else:
                max_id = data['max_id']
            cid = card['id']
            cid_url = self.crawling_comment_child(cid=cid, max_id=max_id)
            yield scrapy.Request(url=cid_url, callback=self.parse_comment_child,
                                 cookies=random.choice(self.cookies),
                                 meta={'mid': mid, 'cid':cid, 'deepth': current_deepth})

    def parse_comment(self, response):
        if response.status == 403:
            logging.error("this is 403 error. request is " + response.request.url)
            logging.error("remove cookies " + response.request.cookies)
            self.cookies.remove(response.request.cookies)
            if not self.cookies:
                self.crawler.engine.close_spider(self, "cookies is null")
            else:
                yield scrapy.Request(url=response.url, callback=self.parse_comment,
                                     cookies=random.choice(self.cookies),
                                     meta=response.meta, dont_filter=True)
        if json.loads(response.text)['ok'] == 0:
            return
        data = json.loads(response.text)['data']
        mid = response.meta['mid']
        current_deepth = response.meta['deepth']
        for card in data['data']:
            user_comment_item = CommentItem()
            user_comment_item['user_comment'] = card
            user_comment_item['user_comment']['master_id'] = mid
            new_uid = card['user']['id']
            yield user_comment_item
            # 判断微博数量与粉丝数量筛选用户
            if card['user']['followers_count'] > 100 and card['user']['statuses_count'] > 100:
                user_info_url = self.crawling_user_info(uuid=new_uid)
                yield scrapy.Request(url=user_info_url,
                                     callback=self.parse_user, meta={'deepth': current_deepth})
            # 爬取回复
            if card['more_info_type'] != 0:
                cid = card['id']
                cid_url = self.crawling_comment_child(cid=cid, max_id="0")
                yield scrapy.Request(url=cid_url, cookies=random.choice(self.cookies),
                                     callback=self.parse_comment_child, meta={'mid': mid, 'cid':cid, 'deepth': current_deepth})

        # 爬取之后的评论页面
        while True:
            if data['max_id'] == 0:
                break
            else:
                max_id = data['max_id']
            comment_url = self.crawling_comment_info(mid=mid, max_id=max_id)
            yield scrapy.Request(url=comment_url, callback=self.parse_comment,
                                 cookies=random.choice(self.cookies),
                                 meta={'mid': mid, 'deepth': current_deepth})

    def parse_longtext(self, response):
        # parser for longtext post
        user_post_item = response.meta['post_item']
        data = json.loads(response.text)['data']
        user_post_item['user_post']['Long_text'] = data['longTextContent']
        precise_time_url = self.__weibo_info_api['precise_time_api'] + user_post_item['user_post']['id']
        yield scrapy.Request(url=precise_time_url, callback=self.parse_precise_time,
                             meta={'post_item': user_post_item})

    def get_precise_time(self, text):
        page_text = etree.HTML(text)
        result = page_text.xpath('/html/body/script[1]/text()')
        time_str = re.findall(r'"created_at":.+"', "".join(result))
        if time_str:
            precise_time = json.loads('{' + time_str[0] + '}')['created_at']
        else:
            precise_time = None
        return precise_time

    def parse_precise_time(self, response):
        # parse for precise time
        try:
            user_post_item = response.meta['post_item']
            precise_time = self.get_precise_time(response.text)
            user_post_item['user_post']['precise_time'] = precise_time
            yield user_post_item
        except Exception as e:
            self.logger.info(message="[weibo_info_spider] parse_precise_time error!" + repr(e), level=logging.ERROR)
