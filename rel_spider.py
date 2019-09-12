import time
import scrapy
from datetime import datetime
from elasticsearch import Elasticsearch

index_name = "products-reliance"
doc_type = "home_appliances"
product_mapping = {'http://<rel-host>/home-appliances/air-conditioner-coolers.html':'air conditioner',
                   'http://<rel-host>/home-appliances/refrigerators.html':'refrigerator',
                   'http://<rel-host>/home-appliances/washing-machine.html':'washing machine' }

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
    name = "reliance"
    
    def start_requests(self):
        """
        """
        urls = ['http://<rel-host>/home-appliances/air-conditioner-coolers.html',
                'http://<rel-host>/home-appliances/washing-machine.html',
                'http://<rel-host>/home-appliances/refrigerators.html'    ]
        for url in urls:
            category = product_mapping.get(url)
            yield scrapy.Request(url=url, callback=self.parse, meta={'productCategory':category})


    def parse(self, response):
        """
        """
        # Get the product links from the page
        product_links = response.css('div.listpage-products-window ul li.product a::attr(href)').extract()
        for product_link in list(product_links):
            product_link = response.urljoin(product_link)
            request = scrapy.Request(product_link, callback=self.parse_product, meta=response.meta)
            yield request
        next_page = response.css('div.pages a.i-next::attr(href)').extract_first()   
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
            'name': extract_with_css('div.pdp_title::text'),
            'url': response.url,
            'source': 'reliance',
            'section': 'home appliances',
            'category': response.meta.get('productCategory')
        }
        price = extract_with_css('div.pdp_price div.pdp_new_amount::text').replace(',', '').replace('Rs.', '').strip()
        try:
            price = long(price)
        except:
            price = -1
        product_info['price'] = price 
        product_info['timestamp'] = datetime.now()
        
        # Get the brand and model
        rows = response.css('table.pdp_table tr')
        for row in rows:
            props = row.css('td::text').extract()
            if len(props) == 2:
                prop_key , prop_val = props[0].strip().lower() , props[1].strip().lower()
                if prop_key == 'brand':
                    product_info['brand'] = prop_val
                elif prop_key == 'model':
                    product_info['model'] = prop_val
                elif product_info.has_key('brand') and product_info.has_key('model'):
                    break
        print product_info
        #es.index(index=index_name, doc_type=doc_type, body=product_info)
