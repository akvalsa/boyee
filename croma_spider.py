import time
import scrapy
from datetime import datetime
from elasticsearch import Elasticsearch

index_name = "products-croma"
doc_type = "home_appliances"
product_mapping = {'http://<crm-host>/home-appliances/air-conditioners/c/46':'air conditioner',
                   'http://<crm-host>/kitchen-appliances/refrigerators/c/47':'refrigerator',
                   'http://<crm-host>/hygiene/washing-machines-and-dryers/c/48':'washing machine' }

def get_croma_es_handle():
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
    
es = get_croma_es_handle()

class QuotesSpider(scrapy.Spider):
    name = "croma"
    
    def start_requests(self):
        """
        """
        urls = ['http://<crm-host>/home-appliances/air-conditioners/c/46', 
                'http://<crm-host>/kitchen-appliances/refrigerators/c/47',
                'http://<crm-host>/hygiene/washing-machines-and-dryers/c/48']
        for url in urls:
            category = product_mapping.get(url)
            yield scrapy.Request(url=url, callback=self.parse, meta={'productCategory':category})


    def parse(self, response):
        """
        """
        # Get the product links from the page
        product_links = response.css('div a.productMainLink::attr(href)').extract()
        for product_link in list(product_links):
            product_link = response.urljoin(product_link)
            request = scrapy.Request(product_link, callback=self.parse_product, meta=response.meta)
            yield request
        next_page = response.css('div.paginationBar ul.pagination li.next a::attr(href)').extract_first()   
        if next_page is not None:
            time.sleep(1)
            next_page = response.urljoin(next_page)
            request = scrapy.Request(next_page, callback=self.parse, meta=response.meta)
            yield request
                        
        
    def parse_product(self, response):
        """
        """        
        def extract_with_css(query):
            return response.css(query).extract_first().strip()

        product_info = {
            'name': extract_with_css('div.productDescription h1::text'),
            'url': response.url,
            'source': 'croma',
            'section': 'home appliances',
            'category': response.meta.get('productCategory')
        }
        price = extract_with_css('div.cta h2::text').replace(',', '')
        try:
            price = long(price)
        except:
            price = -1
        product_info['price'] = price 
        product_info['timestamp'] = datetime.now()
        
        # Get product brand and model
        brand_divs = response.css('div.featureClass').xpath("h4[.//text()='General']/parent::div")
        if brand_divs:
            brand_div = brand_divs[0]
            tds = brand_div.css('table tr td::text').extract()
            for index in range(0, len(tds), 2):
                prop_key, prop_val = tds[index:index+2]
                prop_key, prop_val = prop_key.strip().lower(), prop_val.strip().lower()
                if prop_key == 'brand':
                    product_info['brand'] = prop_val
                elif prop_key == 'model no':
                    product_info['model'] = prop_val
        
        #print product_info
        es.index(index=index_name, doc_type=doc_type, body=product_info)
