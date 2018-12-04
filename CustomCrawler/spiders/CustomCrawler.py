import scrapy
from urlparse import urlparse
from urlparse import urljoin
import json as jsonParse

class JsonCrawler(scrapy.spiders.CrawlSpider):
	name = "JsonCrawler"		
	json = None
	filters = None
	with open('schema.json') as f:
		json = jsonParse.loads(f.read())	
	#-----------------------------#
	def start_requests(self):		
		self.filters =  self.parameter1 			
		#----load the json and get the object of filters	
		self.allowed_domains =  self.json[self.filters]['allowed_domains']  		
		self.start_urls =  self.json[self.filters]['start_urls'] 		
		self.custom_settings = {
			'DOWNLOAD_DELAY': 4,
			'CONCURRENT_REQUESTS_PER_IP': '8' ,
		}
		self.handle_httpstatus_list = self.json[self.filters]['handle_http_list'] #[403,404,301,302,303,307,308,503]
		print "------------------------------------------------------------------------------------------"
		for url in self.start_urls:
			print url
			url  = urljoin(url, '/')
			yield scrapy.Request(url= url, callback=self.parse)
		#--------end of fetching----#
	#---parse function---#
	def parse(self, response):				
		if response.status == 200:			
			paths =   self.json[self.filters]['format']
			dict = {}
			dict['url'] = response.url
			dict['name'] =  self.filters
			for k, v in zip(paths.keys(), paths.values()):
				dict[ k ]  = response.css( v ).extract_first()
			print dict	 
			yield dict
		headers= {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'}
		try:
			response.meta['present_depth']
		except:
			response.meta['present_depth'] = 0
		try:
			response.meta['depth_limit']
		except:
			response.meta['depth_limit'] = self.json[self.filters]['depth_limit']	

		if response.meta['present_depth'] > response.meta['depth_limit']:
			return
		print '------------------------------------------------------------------------------------------'
		try:
			for newslink in response.css("a::attr(href)").extract():				
				try:
					if 	newslink.split('/')[3]!='en' and newslink.split('/')[2] == "www.urdupoint.com":
						continue
				except:
					continue		
				#avoid to scrap to third party website such like youtube
				if urlparse( response.url )[1] == urlparse( newslink )[1] :
					newslink=urljoin(response.url, newslink)
					yield scrapy.Request(newslink, callback=self.parse, headers=headers, meta={ 'present_depth': response.meta['present_depth']+1, 'depth_limit': response.meta['depth_limit']})
				else:
					continue
		except Exception as e:
			print e			
	#---end of parse function---#