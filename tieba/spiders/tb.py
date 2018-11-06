# -*- coding: utf-8 -*-
import re
import scrapy
from tieba.items import TiebaItem


class TbSpider(scrapy.Spider):
	name = 'tb'
	allowed_domains = ['tieba.baidu.com']
	keywords = ["滴滴","专车","快车","代叫","人脸识别"]
	regex = re.compile(r'href="/p/(.*?)"')

	def start_requests(self):
		for keyword in self.keywords:
			for page in range(0, 10):
				url = "https://tieba.baidu.com/f?kw={0}&ie=utf-8&tp=0&pn={1}".format(keyword, page * 50)
				yield scrapy.Request(url, callback=self.parse)

	def parse(self, response):
		tb_urls = self.regex.findall(response.text)
		urls = map(lambda x: "https://tieba.baidu.com/p/{0}".format(x), tb_urls)
		for url in urls:
			yield scrapy.Request(url, callback=self.parse_tb)

	def parse_tb(self, response):
		pages = response.xpath('//li[@class="l_reply_num"]/span[2]/text()').extract()
		for page in range(1, int(pages[0]) + 1):
			url = "{0}?pn={1}".format(response.url, page)
			yield scrapy.Request(url, callback=self.parse_detail)

	def parse_detail(self, response):
		item = TiebaItem()
		threadId = response.url.split('/')[-1].split('?')[0]
		title = response.xpath('//div[@id="j_core_title_wrap"]/h3/text()').extract()
		ids = response.xpath('//li[@class="d_name"]/@data-field').extract()
		uns = response.xpath('//a[@alog-group="p_author"]/text()').extract()
		contents = response.xpath('//div[@class="d_post_content j_d_post_content "]').xpath("string()").extract()
		createds = response.xpath('//div[@class="post-tail-wrap"]/span[last()]/text()').extract()
		for id, un, content, created in zip(ids, uns, contents, createds):
			item['thread_id'] = threadId
			item['user_id'] = id.split(':')[1].split('}')[0]
			item['title'] = ','.join(title)
			item['user_name'] = un
			item['content'] = content.strip()
			item['created'] = created
			yield item
