# -*- coding: utf-8 -*-
import scrapy
import re
import os
import uuid

from elasticsearch import Elasticsearch

ELASTIC_API_URL_HOST = os.environ['ELASTIC_API_URL_HOST']
ELASTIC_API_URL_PORT = os.environ['ELASTIC_API_URL_PORT']
ELASTIC_API_USERNAME = os.environ['ELASTIC_API_USERNAME']
ELASTIC_API_PASSWORD = os.environ['ELASTIC_API_PASSWORD']

es=Elasticsearch(host=ELASTIC_API_URL_HOST,
                 scheme='https',
                 port=ELASTIC_API_URL_PORT,
                 http_auth=(ELASTIC_API_USERNAME,ELASTIC_API_PASSWORD))

class ImdbSpider(scrapy.Spider):
    name = 'imdb'
    allowed_domains = ['www.imdb.com']
    start_urls = ['https://www.imdb.com/title/tt0096463/fullcredits/']

    def parse(self, response):
        request = scrapy.Request('https://www.imdb.com/title/tt0096463/fullcredits/',
                             callback=self.parse_actor_from_movie)
        yield request


    def parse_actor_from_movie(self, response):
        actor_name_list = []
        actor_id_list = []

        movie_name = response.css('h3[itemprop="name"] a::text').extract_first()
        movie_id = response.css('h3[itemprop="name"] a::attr(href)').extract_first().split("/")[2]
        movie_year = re.sub('\s+', ' ', (response.css('h3[itemprop="name"] span[class="nobr"]::text').extract_first()).strip(' \t \r \n').replace('\n', ' ') ).strip()
        movie_year = movie_year.replace("(", "").replace(")","").split('\u2013')[0]

        for actor in response.css('table.cast_list td[itemprop="actor"] span[class="itemprop"]::text ').extract():
            actor_name_list.append(actor)

        for link in response.css('table.cast_list td[itemprop="actor"] a::attr(href)').extract():
            actor_id_list.append(link)

        count = 0
        # for character in response.xpath('//td[@class="character"]//div//text()').extract():
        for character in response.css('td[class="character"]::text').extract():

            if character.strip() :
                temp = re.sub( '\s+', ' ', character.strip(' \t \r \n').replace('\n', ' ') ).strip()
                item = dict()

                item['movie_name'] = movie_name
                item['movie_id'] = movie_id
                item['movie_year'] = movie_year
                item['actor_id'] = actor_id_list[count].split("/")[2]
                item['actor_name'] = actor_name_list[count]
                item['role_name'] = temp

                request = scrapy.Request('https://www.imdb.com/name/' + item['actor_id'] + '/bio',
                                         callback=self.parse_actor_bio)
                request.meta['item'] = item
                yield request

                request2 = scrapy.Request('https://www.imdb.com/name/' + item['actor_id'] + '/', callback=self.parse_next_movie)
                yield request2
                count = count + 1

    def parse_next_movie(self, response):
        noisy_movie_titles_actor = response.css('div[id^="actor"]  b a::attr(href)').extract()
        noisy_movie_titles_actress = response.css('div[id^="actress"]  b a::attr(href)').extract()

        next_movies_id = [];
        next_movies_years = []

        if noisy_movie_titles_actor:
            next_movies_id= [i.split("/")[2] for i in noisy_movie_titles_actor]
            next_movies_years = response.css('div[id^="actor"]  span::text').extract()
        elif noisy_movie_titles_actress:
            next_movies_id = [i.split("/")[2] for i in noisy_movie_titles_actress]
            next_movies_years = response.css('div[id^="actress"]  span::text').extract()

        for i,j in zip(next_movies_id, next_movies_years) :
            j = j.split('\u2013')[0].strip()
            if int(j) < 1980 or int(j) > 1989:
                continue
            request = scrapy.Request('https://www.imdb.com/title/'+ i +'/fullcredits/',
                                     callback=self.parse_actor_from_movie)
            yield request

    def parse_actor_bio(self, response):
        birth_date = response.css('td time::attr(datetime)').extract()
        height = response.css('table[id="overviewTable"] td::text' ).extract()
        spouse = 0

        for s in response.css('h4[class="li_group"]::text').extract():
            if s.find("Spouse") != -1:
                spouse = s[s.find("(")+1:s.find(")")]

        if height:
            height = height[-1].strip()
        else:
            height = ""
        if not any(char.isdigit() for char in height):
            height = ""
        item = response.meta['item']
        if birth_date:
            item['birth_date'] = birth_date[0]
        else:
            item['birth_date'] = ""
        item['height'] = height
        item['spouse'] = spouse

        es.index(index='imdb',
                 doc_type='movies',
                 id=uuid.uuid4(),
                 body={
                     "movie_id": item['movie_id'],
                     "movie_name": item['movie_name'],
                     "movie_year": item['movie_year'],
                     "actor_name": item['actor_name'],
                     "actor_id": item['actor_id'],
                     "role_name": item['role_name'],
                     "height": item['height'],
                     "birth_date": item['birth_date'],
                     "spouse" : item['spouse']
                 })

        yield item



