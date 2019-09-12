import json
import time
import scrapy
from datetime import datetime
from elasticsearch import Elasticsearch
from scrapy.selector import Selector

index_name = "products-ezone"
doc_type = "home_appliances"
product_mapping = {'http://<ezn-host>/Categories/Home-Appliances/Air-Conditioners/c/airconditioners/results?q=:topRated&page=1&skuIndex=0&searchResultType=&sort=':'air conditioner','http://<ezn-host>/Categories/Home-Appliances/Refrigerators/c/refrigerators/results?q=:topRated&page=1&skuIndex=0&searchResultType=&sort=':'refrigerator','http://<ezn-host>/Categories/Home-Appliances/Washing-Machines/c/washingmachines/results?q=:topRated&page=1&skuIndex=0&searchResultType=&sort=':'washing machine'}

def get_rel_es_handle():
    es_hosts = ['localhost:9200']
    field_mappings = {
        'timestamp': {'type':'date'},
        'name': {'type':'string', 'index':'not_analyzed'},
        'nameA': {'type':'string', 'index':'analyzed'},        
        'price': {'type':'integer', 'index':'no'},
        'url': {'type':'string', 'index':'not_analyzed'},
        'source': {'type':'string', 'index':'not_analyzed'},
        'section': {'type':'string', 'index':'not_analyzed'},
        'category': {'type':'string', 'index':'not_analyzed'},        
    }
    mappings = {doc_type:{'properties':field_mappings}}
    es = Elasticsearch(hosts=es_hosts)    
    if not es.indices.exists(index_name):
        es.indices.create(index=index_name)
        es.indices.put_mapping(index=index_name, doc_type=doc_type, body=mappings)
    return es
    
es = get_rel_es_handle()

class QuotesSpider(scrapy.Spider):
    name = "ezone"
    
    def start_requests(self):
        """
        """
        urls = ['http://<ezn-host>/Categories/Home-Appliances/Air-Conditioners/c/airconditioners/results?q=:topRated&page=1&skuIndex=0&searchResultType=&sort=',
                'http://<ezn-host>/Categories/Home-Appliances/Refrigerators/c/refrigerators/results?q=:topRated&page=1&skuIndex=0&searchResultType=&sort=',
                'http://<ezn-host>/Categories/Home-Appliances/Washing-Machines/c/washingmachines/results?q=:topRated&page=1&skuIndex=0&searchResultType=&sort='                ]
                
        
        for url in urls:
            category = product_mapping.get(url)
            yield scrapy.Request(url=url, callback=self.parse, meta={'productCategory':category})


    def parse(self, response):
        """
        """
        data = json.loads(response.body)
        pagination = data.get('pagination')
        results = data.get('results', '')
        cur_page = int(pagination.get('currentPage'))
        num_pages = int(pagination.get('numberOfPages'))
        
        resp = Selector(text=results)
        
        # Get the product links from the page
        product_links = resp.css('div.product-title a::attr(href)').extract()
        for product_link in list(product_links):
            product_link = response.urljoin(product_link)
            request = scrapy.Request(product_link, callback=self.parse_product, meta=response.meta)
            yield request
        
        if cur_page + 1 <= num_pages:
            next_page = response.url.replace('page=%s' % cur_page, 'page=%s' % (cur_page+1) )
            if next_page is not None:
                time.sleep(1)
                next_page = response.urljoin(next_page)
                request = scrapy.Request(next_page, callback=self.parse, meta=response.meta)
                yield request
                        
        
    def parse_product(self, response):
        """
        """        
        def extract_with_css(query):
            value = response.css(query).extract_first() 
            return value.strip() if value else ''

        product_info = {
            'name': extract_with_css('div.product-shop-inner h1.product-name::text'),
            'url': response.url,
            'source': 'ezone',
            'section': 'home appliances',
            'category': response.meta.get('productCategory')
        }
        price = extract_with_css('div.product-shop-inner div.product-price span.selling-pricepdp::text')[1:].replace(',', '').split('.')[0].strip()
        try:
            price = long(price)
        except:
            price = -1
        product_info['price'] = price 
        product_info['timestamp'] = datetime.now()
        print product_info
        es.index(index=index_name, doc_type=doc_type, body=product_info)
