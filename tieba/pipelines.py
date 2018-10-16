# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
from scrapy.conf import settings
from scrapy.exceptions import DropItem


class TiebaPipeline(object):
	def process_item(self, item, spider):
		return item


class MongoDBPipeline(object):
	def __init__(self):
		connection = pymongo.MongoClient(
			settings['MONGODB_SERVER'],
			settings['MONGODB_PORT'],
		)
		self.db = connection[settings['MONGODB_DB']]
		# self.db.authenticate(settings['MONGODB_USERNAME'], settings['MONGODB_PASSWORD'])
		self.collection = self.db[settings['MONGODB_COLLECTION']]

	def process_item(self, item, spider):
		valid = True
		for data in item:
			if not data:
				valid = False
				raise DropItem("Missing {0}!".format(data))
		if valid:
			self.collection = self.db[settings['MONGODB_COLLECTION']]
			self.collection.update({'thread_id': item['thread_id'], "user_id": item['user_id'], "title": item['title'],
									"content": item['content'],
									"created": item['created']}, dict(item),
								   upsert=True)
		print("Question added to MongoDB database!")
		return item
