# -*- coding: utf-8 -*-
# @Author  : CharesFuns
# @Time    : 2020/5/7 16:34
# @Function: WeiboSystem of weibo user's follow and followers list


import json
import logging
import re

import scrapy
from ..items import *
from lxml import etree
from ..database_tool import DBConnector


class FansListSpider(scrapy.Spider):
    name = 'FansListSpider'
    allowed_domains = ['m.weibo.cn', 'weibo.com']
    handle_httpstatus_list = [418]

    def __init__(self, uids, fans_end=10, follows_end=10, *args, **kwargs):
        super(FansListSpider, self).__init__(*args, **kwargs)
        self.uid_list = list(filter(None, uids.split('|')))
        self.root_url = 'https://m.weibo.cn/'
        self.api = {'common_api': 'api/container/getIndex?containerid=231051', 'fans_api_0': '_-_fans_-_',
                    'fans_api_1': '&since_id=', 'follows_api_0': '_-_followers_-_', 'follows_api_1': '&page='}
        self.page_range = {'fans': {'start': 1, 'end': int(fans_end)}, 'follows': {'start': 1, 'end': int(follows_end)}}
        self.__weibo_info_api = {'api_0': 'api/container/getIndex?type=__uid&value=',
                                 'api_1': '&containerid=107603', 'api_2': '&page=',
                                 'longtext_api': 'https://m.weibo.cn/statuses/extend?id=',
                                 'precise_time_api': 'https://m.weibo.cn/status/'}
        self.count = 0
        db_connector = DBConnector()
        self.db, self.client = db_connector.create_mongo_connection()

    def crawl_one(self, uid):
        fans_url_template = self.root_url + self.api['common_api'] + \
                            self.api['fans_api_0'] + uid + self.api['fans_api_1']
        follows_url_template = self.root_url + self.api['common_api'] + \
                               self.api['follows_api_0'] + uid + self.api['follows_api_1']
        fans_urls = [(fans_url_template + str(page_index)) for page_index in range(
            self.page_range['fans']['start'], self.page_range['fans']['end'] + 1)
                     ]
        follows_url = [(follows_url_template + str(page_index)) for page_index in range(
            self.page_range['follows']['start'], self.page_range['follows']['end'] + 1
        )]
        return fans_urls, follows_url

    def crawling_post_info(self, uuid):
        # to generate user's tweet/post/weibo information url
        weibo_info_urls = []
        for i in range(1, 5):
            weibo_info_url = self.root_url + self.__weibo_info_api['api_0'] + str(uuid) + \
                             self.__weibo_info_api['api_1'] + str(uuid) + self.__weibo_info_api['api_2'] + str(i)
            weibo_info_urls.append(weibo_info_url)
        return weibo_info_urls

    def start_requests(self):
        for uid in self.uid_list:
            # fans_url, follows_url = self.crawl_one(uid)
            # for url in fans_url:
            #     yield scrapy.Request(url=url, callback=self.parse_fans, meta={'__uid': uid})
            # for url in follows_url:
            url = self.root_url + self.api['common_api'] + self.api['follows_api_0'] + uid + self.api[
                'follows_api_1'] + str(6)
            yield scrapy.Request(url=url, callback=self.parse_follows, meta={'__uid': uid, 'page': 6})

    # def parse_fans(self, response):
    #     cards = json.loads(response.text)['data']['cards']
    #     fans_item = FansListItem()
    #     fans_item['uid'] = response.meta['__uid']
    #     fans_list = []
    #     for crd in cards:
    #         if crd['card_type'] == 11:
    #             for card in crd['card_group']:
    #                 if card['card_type'] == 10:
    #                     fans_list.append(card['user'])
    #     fans_item['fans_list'] = fans_list
    #     yield fans_item

    def parse_follows(self, response):
        data = json.loads(response.text)
        if data["ok"] == 0:
            return
        cards = data['data']['cards']
        for crd in cards:
            if crd['card_type'] == 11:
                for card in crd['card_group']:
                    if card['card_type'] == 10 and card['user']['followers_count'] >= 10000:
                        user_info_item = UserInfoItem()
                        user_info_item['user_info'] = card['user']
                        uuid = card['user']['id']
                        if self.db['post'].find_one({'user.id':uuid}):
                            print("already in")
                            continue
                        self.count = self.count + 1
                        print(self.count)
                        yield user_info_item
                        url = self.root_url + self.api['common_api'] + self.api['follows_api_0'] + str(uuid) + self.api[
                            'follows_api_1'] + str(1)
                        yield scrapy.Request(url=url, callback=self.parse_follows, meta={'__uid': uuid, 'page': 1})
                        weibo_info_urls = self.crawling_post_info(uuid)
                        for weibo_info_url in weibo_info_urls:
                            yield scrapy.Request(url=weibo_info_url,
                                                 callback=self.parse_post)
        uid = response.meta['__uid']
        page = response.meta['page']
        # print("page: "+str(page))
        url = self.root_url + self.api['common_api'] + self.api['follows_api_0'] + str(uid) + self.api[
            'follows_api_1'] + str(page+1)
        yield scrapy.Request(url=url, callback=self.parse_follows, meta={'__uid': uid, 'page': page + 1})

    def parse_post(self, response):
        # the parser for user post
        weibo_info = json.loads(response.text)
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
