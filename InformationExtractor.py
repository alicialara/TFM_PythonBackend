# coding=utf-8
import json
import re
import sys
import urllib
import urllib2
from pprint import pprint

import requests
import unidecode as unidecode
from SPARQLWrapper import SPARQLWrapper, JSON
from bs4 import BeautifulSoup

from MysqlND import MysqlND
from Solrindex import Solrindex

reload(sys)
sys.setdefaultencoding('utf8')

class InformationExtractor(object):

    def __init__(self):
        self.limit = 1  # number of elements to look for
        self.save_data = True
        # self.extract_artworks_from_dbpedia()

        # Step 1 - Extract wikidata urls
        # self.extract_artworks_from_wikidata()

        # Step 2 - Foreach wikidata url, extract metadata info -> Solr core 1 ()
        # self.extract_info_from_wikidata_url()

        # Step 3 - Extract wikipedia URL
        # self.extract_wikipedia_url()

        # Step 4 - Extract Museo del Prado URL
        # self.extract_museo_del_prado_url()

        # Step 5 - Extract wikidata labels to Mysql table
        # self.extract_wikidata_labels()

        # Step 6 - Extract museo del prado information
        # self.extract_museo_del_prado_info()

        # Step 7 - Extract info from Wikipedia
        # self.extract_info_from_wikipedia()



        # Utils - remove solr data
        # solr_remove = Solrindex()
        # solr_remove.delete("id:12045")

    # #########################
    # EXTRACT ARTWORKS URL INDEX
    # #########################

    # <editor-fold desc="Extract artworks">
    def extract_artworks_from_dbpedia(self):
        """
                Test query -> extract random artworks from a general query (select * artworks from museo del prado)
            :return:
            """
        sparql = SPARQLWrapper("http://dbpedia.org/sparql")
        sparql.setReturnFormat(JSON)
        query = """ 
                PREFIX dbo: <http://dbpedia.org/ontology/>
                PREFIX dbp: <http://dbpedia.org/resource/>
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
                SELECT distinct(?obra)
                where {
                   ?obra rdf:type dbo:Artwork .
                   ?obra dbo:museum dbr:Museo_del_Prado .
                }
                """
        sparql.setQuery(query)  # the previous query as a literal string
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            print(result)
        self.save_artworks_index(results)
        return True

    def extract_artworks_from_wikidata(self):
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        sparql.setQuery(
            """
            SELECT ?obra WHERE {
              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],es". }
              ?obra wdt:P195 wd:Q160112.
            }
            LIMIT 10000
            """
        )
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            print(result)
        self.save_artworks_index(results)
        return True

    def save_artworks_index(self, data):
        for result in data["results"]["bindings"]:
            dict_solr = {}
            obra = result['obra']
            dict_solr['type_en'] = 0
            str_obra = obra['value']
            query_search_url = "SELECT COUNT(id) AS cuenta FROM dbp_urls WHERE url LIKE %s"
            cuenta = MysqlND.execute_query(query_search_url, (str_obra,)).fetchone()
            total = cuenta[0]
            if total == 0:
                print("Se añade nuevo registro")
                query = "INSERT INTO dbp_urls(type, url) VALUES(0, %s)"
                array = (str_obra,)
                return MysqlND.execute_query(query, array)
            else:
                print("El registro ya existía. No se vuelve a añadir")

        # [{"id": "doc_1", "title": "A test document", }, {"id": "doc_2", "title": "The Banana: Tasty or Dangerous?", }, ]

    # </editor-fold>

    def get_next_wikidata_url_pending(self, type):
        if type == 'visited':
            query_search_url = "SELECT id,url FROM dbp_urls WHERE type=0 and extracted_wikipedia_url=0 LIMIT " + str(
                self.limit)
        elif type == 'extracted_wikipedia_url':
            query_search_url = "SELECT id,url FROM dbp_urls WHERE type=0 and extracted_wikipedia_url=0 LIMIT " + str(
                self.limit)
        elif type == 'extracted_museo_del_prado_url':
            query_search_url = "SELECT id,url FROM dbp_urls WHERE type=0 and extracted_museo_del_prado_url=0 LIMIT " + str(
                self.limit)
        elif type == 'wikidata_labels':
            query_search_url = "SELECT id,url FROM dbp_urls WHERE type=0 and wikidata_labels=0 LIMIT " + str(
                self.limit)
        elif type == 'extracted_museo_del_prado_info':
            query_search_url = "SELECT id,url FROM dbp_urls WHERE type=2 and extracted_museo_del_prado_info=0 LIMIT " + str(
                self.limit)
        elif type == 'extracted_wikipedia_info':
            query_search_url = "SELECT id,url FROM dbp_urls WHERE type=1 and extracted_wikipedia_info=0 LIMIT " + str(
                self.limit)
        print(query_search_url)
        return MysqlND.execute_query(query_search_url, ())

    def set_url_visited(self, id, type):
        if not self.save_data:
            return True
        if type == 'visited':
            query_search_url = "UPDATE dbp_urls SET visited= '1' WHERE id= " + str(id)
        elif type == 'extracted_wikipedia_url':
            query_search_url = "UPDATE dbp_urls SET extracted_wikipedia_url= '1' WHERE id= " + str(id)
        elif type == 'extracted_museo_del_prado_url':
            query_search_url = "UPDATE dbp_urls SET extracted_museo_del_prado_url= '1' WHERE id= " + str(id)
        elif type == 'wikidata_labels':
            query_search_url = "UPDATE dbp_urls SET wikidata_labels= '1' WHERE id= " + str(id)
        elif type == 'extracted_museo_del_prado_info':
            query_search_url = "UPDATE dbp_urls SET extracted_museo_del_prado_info= '1' WHERE id= " + str(id)
        elif type == 'extracted_author_data':
            query_search_url = "UPDATE dbp_urls SET extracted_author_data= '1' WHERE id= " + str(id)
        elif type == 'extracted_wikipedia_info':
            query_search_url = "UPDATE dbp_urls SET extracted_wikipedia_info= '1' WHERE id= " + str(id)
        return MysqlND.execute_query(query_search_url, ())

    def format_date(self, date):
        # date = str(date)

        date_exploded = date.split("/")
        data = date_exploded[0].split("-")
        if len(data) == 3:
            res = data[2]
        else:
            res = data[0]
        # res = res[0:4]
        res = re.sub("[^0-9]", "", res)
        if res == '':
            res = 0
        return int(res)

    def cleanhtml(self, raw_html):
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', raw_html)
        return cleantext

    def is_json(self, myjson):
        try:
            json_object = json.loads(myjson)
        except ValueError, e:
            return False
        return True

    def load_dirty_json(self, dirty_json):
        regex_replace = [(r"([ \{,:\[])(u)?'([^']+)'", r'\1"\3"'), (r" False([, \}\]])", r' false\1'),
                         (r" True([, \}\]])", r' true\1')]
        for r, s in regex_replace:
            dirty_json = re.sub(r, s, dirty_json)
        return dirty_json


    # #########################
    # EXTRACT INFO FROM ARTWORKS PENDING FROM WIKIDATA
    # #########################

    # <editor-fold desc="EXTRACT INFO FROM ARTWORKS PENDING FROM WIKIDATA">
    def extract_info_from_wikidata_url(self):
        for (id, url) in self.get_next_wikidata_url_pending('visited'):
            sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
            # Extract ID artwork from URL
            url_splitted = url.split("/")
            wikidata_id_entity = str(url_splitted[4])

            wikidata_query = """
                SELECT * WHERE {
                  wd:""" + wikidata_id_entity + """  rdfs:label ?label.
                  OPTIONAL { wd:""" + wikidata_id_entity + """ wdt:P31 ?instancia_de. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P361 ?forma_parte_de. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P18 ?imagen. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P571 ?fecha_de_fundacion_o_creacion. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P135 ?movimiento. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P17 ?pais. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P276 ?ubicacion. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P136 ?genero_artistico. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P170 ?creador. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P495 ?pais_de_origen. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P2048 ?altura. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P2049 ?ancho. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P186 ?material_empleado. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P180 ?representa_a. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P195 ?coleccion. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P217 ?numero_de_inventario. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P973 ?descrito_en_la_URL. }
                  OPTIONAL { wd:""" + wikidata_id_entity + """  wdt:P1257 ?elemento_Iconclass_representado. }
                  FILTER(LANGMATCHES(LANG(?label), "ES"))
                }
                LIMIT 1000
                """

            sparql.setQuery(wikidata_query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            data_results = results["results"]["bindings"]

            array_solr = {'id': int(id)}
            array_solr_aux = {}
            array_solr_keys = []
            for item in data_results:  # for each artwork -> ex: {u'pais_de_origen': {u'type': u'uri', u'value': u'htt
                claves = item.keys()
                for key in claves:
                    aux = item[key]  # ej. {u'type': u'uri', u'value': u'http://www.wikidata.org/entity/Q38'}
                    valor = str(aux['value'].encode('utf-8'))
                    tipo = '_s'  # default: data type is strings
                    if aux['type'] == 'literal':
                        if 'datatype' in aux:
                            if aux['datatype'] == 'http://www.w3.org/2001/XMLSchema#dateTime':
                                tipo = '_dt'  # date type
                            if aux['datatype'] == 'http://www.w3.org/2001/XMLSchema#decimal':
                                tipo = '_f'  # int type
                                valor = float(valor)

                    key_formatted = key + tipo

                    if key in array_solr_keys:  # si esta clave ya existe, puede ser multivalue
                        if array_solr_aux[key] and array_solr_aux[key] != valor:  # si en realidad es el mismo registro.
                            # print(type(array_solr_aux[key]))
                            if (type(array_solr_aux[key]) is str) or (type(array_solr_aux[key]) is float) or (
                                    type(array_solr_aux[key]) is unicode):  # si ya existía
                                # convert to array
                                last_value = array_solr_aux[key]

                                del (array_solr[key_formatted])
                                del (array_solr_aux[key])
                                # del(array_solr_keys[key])

                                key_formatted = str(key_formatted) + 's'

                                array_solr_aux[key] = [last_value, valor]
                                array_solr[key_formatted] = [last_value, valor]
                                array_solr_keys.append(key)

                            elif type(array_solr_aux[key]) is list:
                                key_formatted = str(key_formatted) + 's'
                                if valor not in array_solr[key_formatted]:
                                    array_solr_aux[key].append(valor)
                                    array_solr[key_formatted].append(valor)
                                    array_solr_keys.append(key)
                    else:
                        key_formatted = str(key_formatted)
                        array_solr_aux[key] = valor
                        array_solr[key_formatted] = valor
                        array_solr_keys.append(key)

            salida = self.save_solr_registry(array_solr)  # [{'id': 93L}, {u'numero_de_inventario_s': u'P02824'}, {u'a
            self.set_url_visited(id, 'visited')
        return salida

    def save_solr_registry(self, data):
        if not self.save_data:
            return True
        query_solr = Solrindex()
        return query_solr.add_data(data)
        # resultado = query_solr.search('*', '*')
        # print("Saw {0} result(s).".format(len(resultado)))

    # #########################
    # EXTRACT WIKIPEDIA URLs FOR EACH ARTWORK FROM WIKIDATA
    # #########################

    def extract_wikipedia_url(self):
        for (id_wikidata, url) in self.get_next_wikidata_url_pending('extracted_wikipedia_url'):
            url_splitted = url.split("/")
            wikidata_id_entity = str(url_splitted[4])
            url_query_wikidata = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=xml&props=sitelinks&format=json&languages=es&sitefilter=eswiki&ids=' + wikidata_id_entity  # ej. Q75846

            response = urllib.urlopen(url_query_wikidata)
            data = json.loads(response.read())
            if 'eswiki' in data['entities'][wikidata_id_entity]['sitelinks']:
                wikipedia_title = data['entities'][wikidata_id_entity]['sitelinks']['eswiki']['title']  # ex. El Lavatorio (Tintoretto)
                # get the URL
                wikidata_title_formatted_url = wikipedia_title.replace(" ", "_")
                wikipedia_url = 'https://es.wikipedia.org/wiki/' + wikidata_title_formatted_url
                # check http status
                try:
                    r = requests.head(wikipedia_url)
                    print(r.status_code)
                    if r.status_code == 200:
                        query = "INSERT INTO dbp_urls(type, url) VALUES(1, %s)"
                        array = (wikipedia_url,)
                        result_q = MysqlND.execute_query(query, array)
                        id_wikipedia = result_q.lastrowid
                    else:
                        id_wikipedia = -1

                    q = "SELECT count(id_wikidata) as cuenta FROM urls_relations WHERE id_wikidata=" + str(
                        id_wikidata) + " AND id_wikipedia=" + str(id_wikipedia)
                    cuenta = MysqlND.execute_query(q, ()).fetchone()
                    total = cuenta[0]
                    if total == 0:
                        q = "INSERT INTO urls_relations (id_wikidata,id_wikipedia,id_museodelprado) VALUES (" + str(
                            id_wikidata) + "," + str(id_wikipedia) + ",-1)"
                        MysqlND.execute_query(q, ())
                    else:
                        q = "UPDATE urls_relations SET id_wikipedia=" + str(
                            id_wikipedia) + " WHERE id_wikidata = " + str(id_wikidata)
                        MysqlND.execute_query(q, ())
                    self.set_url_visited(id_wikidata, 'extracted_wikipedia_url')


                except requests.ConnectionError:
                    print("Failed to connect: " + wikipedia_url)

                # print wikipedia_title
            else:
                id_wikipedia = -1
                q = "SELECT count(id_wikidata) as cuenta FROM urls_relations WHERE id_wikidata=" + str(
                    id_wikidata) + " AND id_wikipedia=" + str(id_wikipedia)
                cuenta = MysqlND.execute_query(q, ()).fetchone()
                total = cuenta[0]
                if total == 0:
                    q = "INSERT INTO urls_relations (id_wikidata,id_wikipedia,id_museodelprado) VALUES (" + str(
                        id_wikidata) + "," + str(id_wikipedia) + ",-1)"
                    MysqlND.execute_query(q, ())
                else:
                    q = "UPDATE urls_relations SET id_wikipedia=" + str(id_wikipedia) + " WHERE id_wikidata = " + str(
                        id_wikidata)
                    MysqlND.execute_query(q, ())
                self.set_url_visited(id_wikidata, 'extracted_wikipedia_url')

    # #########################
    # EXTRACT MUSEO DEL PRADO URLs FOR EACH ARTWORK FROM WIKIDATA
    # #########################

    def extract_museo_del_prado_url(self):
        for (id_wikidata, url) in self.get_next_wikidata_url_pending('extracted_museo_del_prado_url'):
            query_solr = Solrindex()

            # results = query_solr.search('descrito_en_la_URL_s:*')
            # results = query_solr.search({'id:'+str(id_wikidata), 'descrito_en_la_URL_s:*'})
            results = query_solr.search('descrito_en_la_URL_s:* AND id:' + str(id_wikidata))

            # The ``Results`` object stores total results found, by default the top
            # ten most relevant results and any additional data like
            # facets/highlighting/spelling/etc.
            print("Saw {0} result(s).".format(len(results)))

            # Just loop over it to access the results.
            for result in results:
                url_museo_prado = format(result['descrito_en_la_URL_s'])
                print("The descrito_en_la_URL_s is '{0}'.".format(result['descrito_en_la_URL_s']))
                # first, check http status
                r = requests.head(url_museo_prado)
                print(r.status_code)
                if r.status_code == 200:
                    # check language
                    is_museo_del_prado = url_museo_prado.find('www.museodelprado.es')
                    if is_museo_del_prado != -1:

                        is_english = url_museo_prado.find('www.museodelprado.es/en/the-collection/')
                        if is_english != -1:
                            # if it's english, get the spanish url
                            hdr = {
                                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                                'Accept-Encoding': 'none',
                                'Accept-Language': 'en-US,en;q=0.8',
                                'Connection': 'keep-alive'}

                            req = urllib2.Request(url_museo_prado, headers=hdr)

                            try:
                                page = urllib2.urlopen(req)
                            except urllib2.HTTPError, e:
                                print e.fp.read()
                            content = page.read()
                            soup = BeautifulSoup(content, 'html.parser')
                            for a in soup.find_all('link', attrs={"hreflang": "es"}):
                                url_museo_prado = a['href']

                        is_spanish = url_museo_prado.find('www.museodelprado.es/coleccion/obra-de-arte/')
                        if is_spanish != -1:
                            # save the url and the relationship
                            q = "SELECT count(id) as cuenta FROM dbp_urls WHERE type=2 AND url LIKE %s"
                            array = (url_museo_prado,)
                            cuenta = MysqlND.execute_query(q, array).fetchone()
                            total = cuenta[0]
                            if total == 0:
                                query = "INSERT INTO dbp_urls(type, url) VALUES(2, %s)"
                                result_q = MysqlND.execute_query(query, array)
                                id_museodelprado = result_q.lastrowid

                                q = "UPDATE urls_relations SET id_museodelprado=" + str(id_museodelprado) + " WHERE id_wikidata = " + str(id_wikidata)
                                MysqlND.execute_query(q, ())

            self.set_url_visited(id_wikidata, 'extracted_museo_del_prado_url')

    # #########################
    # EXTRACT WIKIDATA LABELS TO MySQL
    # #########################

    def extract_wikidata_labels(self):
        for (id_wikidata, url) in self.get_next_wikidata_url_pending('wikidata_labels'):
            query_solr = Solrindex()

            # results = query_solr.search('descrito_en_la_URL_s:*')
            # results = query_solr.search({'id:'+str(id_wikidata), 'descrito_en_la_URL_s:*'})
            results = query_solr.search('descrito_en_la_URL_s:* AND id:' + str(id_wikidata))

            # The ``Results`` object stores total results found, by default the top
            # ten most relevant results and any additional data like
            # facets/highlighting/spelling/etc.
            print("Saw {0} result(s).".format(len(results)))

            # Just loop over it to access the results.
            for result in results:
                for (key, value) in result.iteritems():
                    if isinstance(value, list):  # if it's an array, get one by one
                        for i in value:
                            is_wikidata_url = i.find('www.wikidata.org/entity/')
                            if is_wikidata_url != -1:
                                url_splitted = i.split("/")
                                wikidata_id_entity = str(url_splitted[4])
                                url_query_wikidata = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&props=labels&languages=es&sitefilter=eswiki&ids=' + wikidata_id_entity
                                response = urllib.urlopen(url_query_wikidata)
                                data = json.loads(response.read())
                                if 'labels' in data['entities'][wikidata_id_entity]:
                                    if 'es' in data['entities'][wikidata_id_entity]['labels']:
                                        wikidata_label = data['entities'][wikidata_id_entity]['labels']['es']['value']  # ex. España
                                        query = "INSERT IGNORE INTO wikidata_entities_to_labels(id, key_solr,label) VALUES(%s,%s,%s)"
                                        array = (wikidata_id_entity, key, wikidata_label)
                                        MysqlND.execute_query(query, array)
                    else:
                        if isinstance(value, (int, long, float)):
                            value = str(value)
                        else:
                            value = value.encode("utf-8")
                        is_wikidata_url = value.find('www.wikidata.org/entity/')
                        if is_wikidata_url != -1:
                            url_splitted = value.split("/")
                            wikidata_id_entity = str(url_splitted[4])
                            url_query_wikidata = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&props=labels&languages=es&sitefilter=eswiki&ids=' + wikidata_id_entity
                            response = urllib.urlopen(url_query_wikidata)
                            data = json.loads(response.read())
                            if 'labels' in data['entities'][wikidata_id_entity]:
                                if 'es' in data['entities'][wikidata_id_entity]['labels']:
                                    wikidata_label = data['entities'][wikidata_id_entity]['labels']['es'][
                                        'value']  # ex. España
                                    query = "INSERT IGNORE INTO wikidata_entities_to_labels(id, key_solr,label) VALUES(%s,%s,%s)"
                                    array = (wikidata_id_entity, key, wikidata_label)
                                    MysqlND.execute_query(query, array)
            self.set_url_visited(id_wikidata, 'wikidata_labels')

    # #########################
    # EXTRACT MUSEO DEL PRADO INFO
    # #########################

    def extract_museo_del_prado_info(self):
        for (id_museodelprado, url_museodelprado) in self.get_next_wikidata_url_pending('extracted_museo_del_prado_info'):

            print str(id_museodelprado) + " - " + str(url_museodelprado)

            hdr = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                'Accept-Encoding': 'none',
                'Accept-Language': 'en-US,en;q=0.8',
                'Connection': 'keep-alive'}

            req = urllib2.Request(url_museodelprado, headers=hdr)

            try:
                page = urllib2.urlopen(req)
            except urllib2.HTTPError, e:
                print e.fp.read()
            content = page.read()
            soup = BeautifulSoup(content, 'html.parser')
            json_data = soup.find('script', attrs={"type": "application/ld+json"})
            data = ''
            if json_data:
                json_text = json_data.getText()
                if self.is_json(json_text):
                    data = json.loads(json_text)
                else:
                    json_text = self.load_dirty_json(json_text)
                    if self.is_json(json_text):
                        data = json.loads(json_text)

            author_url = ''

            array_solr = {'id': int(id_museodelprado), 'type': '2'}
            authors = []

            if len(data) > 0:
                for (key, value) in data.iteritems():
                    solr_field = '_s'
                    if key == 'description':
                        solr_field = '_txt_es'

                    elif key == 'dateCreated' or key == 'dateModified' or key == 'datePublished':
                        solr_field = '_d'
                        value = self.format_date(value)
                    elif key == 'height':
                        solr_field = '_f'
                        value = value['value'].replace(",", ".")
                    elif key == 'width':
                        solr_field = '_f'
                        value = value['value'].replace(",", ".")
                    elif key == 'author':
                        solr_field = '_s'

                    elif isinstance(value, (int, long, float)):
                        solr_field = '_i'
                    elif isinstance(value, (list)):
                        value = json.dumps(value)

                    new_key_solr = key + solr_field
                    if new_key_solr != 'associatedMedia_s' and new_key_solr != 'author_s':
                        array_solr[new_key_solr] = value
            else:
                print "Extract info !!"
                if soup.find('span', attrs={"property": "pm:publicationDate"}):
                    array_solr['datePublished_d'] = self.format_date(soup.find('span', attrs={"property": "pm:publicationDate"}).getText())

                if soup.find('meta', attrs={"name": "twitter:image"}):
                    image_s = soup.find('meta', attrs={"name": "twitter:image"})
                    array_solr['image_s'] = image_s['content']

                if soup.find('section', attrs={"class": "ficha"}).find('span', attrs={"property": "cidoc:p102_has_title"}):
                    array_solr['artEdition_s'] = soup.find('section', attrs={"class": "ficha"}).find('span', attrs={"property": "cidoc:p102_has_title"}).getText()

                if soup.find('div', attrs={"class": "obra"}).find("div", attrs={"property": "cidoc:p3_has_note"}):
                    array_solr['description_txt_es'] = soup.find('div', attrs={"class": "obra"}).find("div", attrs={"property": "cidoc:p3_has_note"}).getText()

                if len(soup.find_all('span', attrs={"property": "ecidoc:p90_E60_has_value"})) > 0:
                    c = soup.find_all('span', attrs={"property": "ecidoc:p90_E60_has_value"})
                    array_solr['height_f'] = float(c[0].getText().replace(",", "."))
                    array_solr['width_f'] = float(c[1].getText().replace(",", "."))

                if soup.find('input', attrs={"id": "recurso_modification_date"}):
                    dateModified_d = soup.find('input', attrs={"id": "recurso_modification_date"})
                    array_solr['dateModified_d'] = self.format_date(dateModified_d['value'])

                if soup.find('div', attrs={"class": "ficha-tecnica"}).find('dd', attrs={"property": "date:textDate"}):
                    array_solr['dateCreated_d'] = self.format_date(soup.find('div', attrs={"class": "ficha-tecnica"}).find('dd', attrs={"property": "date:textDate"}).getText())

                # if soup.find('section', attrs={"class": "multimedia"}).find('div', attrs={"class": "carousel inactive"}):
                #     array_solr['associatedMedia_s'] = soup.find('section', attrs={"class": "multimedia"}).find('div', attrs={"class": "carousel inactive"}).getText()

                if soup.find('div', attrs={"class": "ficha-tecnica"}).find('span', attrs={"rel": "ecidoc:p108i_E12_p32_used_general_technique"}):
                    array_solr['artform_s'] = soup.find('div', attrs={"class": "ficha-tecnica"}).find('span', attrs={"rel": "ecidoc:p108i_E12_p32_used_general_technique"}).getText()

                if soup.find('div', attrs={"class": "obra"}).find("h1", attrs={"property": "ecidoc:p102_E35_p3_has_title"}):
                    array_solr['name_s'] = soup.find('div', attrs={"class": "obra"}).find("h1", attrs={"property": "ecidoc:p102_E35_p3_has_title"}).getText()

                if soup.find('div', attrs={"class": "ficha-tecnica"}).find('span', attrs={"rel": "ecidoc:p108i_E12_p126_employed_support"}):
                    array_solr['artworkSurface_s'] = soup.find('div', attrs={"class": "ficha-tecnica"}).find('span', attrs={"rel": "ecidoc:p108i_E12_p126_employed_support"}).getText()

                array_solr['_context_s'] = "http://schema.org"
                array_solr['_type_s'] = "VisualArtwork"

            # array_solr['artform_s'] = self.cleanhtml(array_solr['artform_s'])
            # array_solr['description_txt_es'] = self.cleanhtml(array_solr['description_txt_es'])
            # array_solr['artworkSurface_s'] = self.cleanhtml(array_solr['artworkSurface_s'])

            if soup.find('dd', attrs={"property": "cidoc:p27_moved_from"}):
                array_solr['procedencia_s'] = soup.find('dd', attrs={"property": "cidoc:p27_moved_from"}).getText()

            # data missing: id artwork, url author (to extract more info), ...

            # if soup.find('input', attrs={"class": "inpt_FacetasProyAutoCompBuscadorCom"}):
            #     represents_topic_list = soup.find('input', attrs={"class": "inpt_FacetasProyAutoCompBuscadorCom"})
            #     represents_topic_list = represents_topic_list['value']

            tags_list = soup.find('div', attrs={"class": "tags"}).find_all('a')
            tags = []
            for tag in tags_list:
                tag_cleaned = tag.getText()
                if tag_cleaned != '+':
                    tags.append(tag_cleaned)

            array_solr['keywords_ss'] = tags

            # find all new links from artworks
            new_artworks_links = soup.findAll('a', href=re.compile('^https://www.museodelprado.es/coleccion/obra-de-arte/*'))
            self.extract_new_artwoks_from_an_artwork_in_museodelprado(new_artworks_links, array_solr)

            if soup.find('section', attrs={"id": "ficha-obra"}).find('div', attrs={"class": "autor"}):
                if len(soup.find('section', attrs={"id": "ficha-obra"}).find('div', attrs={"class": "autor"}).find_all('a')) > 0:
                    for data_authors in soup.find('section', attrs={"id": "ficha-obra"}).find('div', attrs={"class": "autor"}).find_all('a'):
                        author_url = data_authors['href']
                        authors.append(author_url)
                else:
                    author_url = ''
            else:
                if len(soup.find('section', attrs={"id": "ficha-obra"}).find('dl', attrs={"class": "autor"}).find_all('a')) > 0:
                    for data_authors in soup.find('section', attrs={"id": "ficha-obra"}).find('dl', attrs={"class": "autor"}).find_all('a'):
                        author_url = data_authors['href']
                        authors.append(author_url)
                else:
                    author_url = ''

            array_solr['id_author_is'] = []
            if author_url == '':
                array_solr['id_author_is'].append(0)
            else:
                for aut in authors:
                    array_solr['id_author_is'].append(self.extract_info_from_author(aut))

            self.save_solr_registry(array_solr)  # [{'id': 93L}, {u'numero_de_inventario_s': u'P02824'}, {u'a
            self.set_url_visited(id_museodelprado, 'extracted_museo_del_prado_info')

    def extract_new_artwoks_from_an_artwork_in_museodelprado(self, new_artworks_links, array_solr):
        for new_links in new_artworks_links:
            new_link = new_links['href']
            # is in our database?
            q = "SELECT count(*) as cuenta FROM dbp_urls WHERE TYPE=2 AND url LIKE %s"
            array_q = (new_link,)
            cuenta = MysqlND.execute_query(q, array_q).fetchone()
            if cuenta[0] == 0:
                query = "INSERT INTO dbp_urls(type, url) VALUES(2, %s)"
                result_q = MysqlND.execute_query(query, array_q)
                id_museodelprado = result_q.lastrowid
                # look for that artwork in wikidata -> use art edition in sparql
                numero_de_inventario_s = array_solr['artEdition_s']
                numero_de_inventario_s_v2 = numero_de_inventario_s.replace('0', '',
                                                                           1)  # remove one zero to look for the artwork in two ways
                wikidata_query = """
                                SELECT * WHERE {
                                  ?obra wdt:P217 '""" + numero_de_inventario_s + """ '
                                }
                                LIMIT 1
                                """
                sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
                sparql.setQuery(wikidata_query)
                sparql.setReturnFormat(JSON)
                results = sparql.query().convert()
                data_results = results["results"]["bindings"]
                if len(data_results) == 0:
                    wikidata_query = """
                                                        SELECT * WHERE {
                                                          ?obra wdt:P217 '""" + numero_de_inventario_s_v2 + """ '
                                                        }
                                                        LIMIT 1
                                                        """
                    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
                    sparql.setQuery(wikidata_query)
                    sparql.setReturnFormat(JSON)
                    results = sparql.query().convert()
                    data_results = results["results"]["bindings"]
                    if len(data_results) == 0:
                        id_wikidata = -1
                    else:
                        id_wikidata = self.save_artworks_index(results).lastrowid
                else:
                    id_wikidata = self.save_artworks_index(results).lastrowid

                # insert as a relation
                q = "SELECT count(id_wikidata) as cuenta FROM urls_relations WHERE id_wikidata=" + str(
                    id_wikidata) + " AND id_museodelprado=" + str(id_museodelprado)
                cuenta = MysqlND.execute_query(q, ()).fetchone()
                total = cuenta[0]
                if total == 0:
                    q = "INSERT INTO urls_relations (id_wikidata,id_wikipedia,id_museodelprado) VALUES (" + str(
                        id_wikidata) + ",-1," + str(id_museodelprado) + ")"
                    MysqlND.execute_query(q, ())

    def extract_info_from_author(self, url_author):
        array_solr_author = {}
        array_solr_author['url_s'] = url_author
        q = "SELECT count(*) as cuenta,id FROM dbp_urls WHERE TYPE=3 AND url LIKE %s"
        array_q = (array_solr_author['url_s'],)
        cuenta = MysqlND.execute_query(q, array_q).fetchone()
        results = {}
        if cuenta[0] == 0:
            query = "INSERT INTO dbp_urls(type, url) VALUES(3, %s)"
            result_q = MysqlND.execute_query(query, array_q)
            id_museodelprado_author = result_q.lastrowid
            results['id'] = id_museodelprado_author
        else:
            results['id'] = id_museodelprado_author = cuenta[1]

        # exists in solr??
        query_solr = Solrindex()
        results_solr = query_solr.search('id:' + str(id_museodelprado_author))
        print("Saw {0} result(s).".format(len(results_solr)))

        if len(results_solr) == 0:

            hdr = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                'Accept-Encoding': 'none',
                'Accept-Language': 'en-US,en;q=0.8',
                'Connection': 'keep-alive'}

            req = urllib2.Request(array_solr_author['url_s'], headers=hdr)

            try:
                page = urllib2.urlopen(req)
            except urllib2.HTTPError, e:
                print e.fp.read()
            content = page.read()
            soup = BeautifulSoup(content, 'html.parser')

            array_solr_author['id'] = results['id']
            array_solr_author['name_s'] = soup.find('h1', attrs={"property": "ecidoc:p131_E82_p102_has_title"}).getText()

            if soup.find('div', attrs={"property": "cidoc:p3_has_note"}):
                array_solr_author['description_txt_es'] = soup.find('div', attrs={"property": "cidoc:p3_has_note"}).getText()

            array_solr_author['type'] = 3

            if soup.find('span', attrs={"property": "ecidoc:p96_E67_p4_gave_birth_date"}):
                array_solr_author['p96_E67_p4_gave_birth_date_d'] = self.format_date(soup.find('span', attrs={"property": "ecidoc:p96_E67_p4_gave_birth_date"}).getText())

            if soup.find('span', attrs={"property": "ecidoc:p100i_E69_p4_death_date"}):
                array_solr_author['p100i_E69_p4_death_date_d'] = self.format_date(soup.find('span', attrs={"property": "ecidoc:p100i_E69_p4_death_date"}).getText())
            if soup.find('span', attrs={"property": "ecidoc:p96_E67_p7_gave_birth_place"}):
                array_solr_author['p96_E67_p7_gave_birth_place_s'] = soup.find('span', attrs={"property": "ecidoc:p96_E67_p7_gave_birth_place"}).getText()

            if soup.find('span', attrs={"property": "ecidoc:p100i_E69_p7_death_place"}):
                array_solr_author['p100i_E69_p7_death_place_s'] = soup.find('span', attrs={"property": "ecidoc:p100i_E69_p7_death_place"}).getText()

            if soup.find('div', attrs={"property": "cidoc:p3_has_note"}):
                array_solr_author['p3_has_note_s'] = soup.find('div', attrs={"property": "cidoc:p3_has_note"}).getText()

            tags_list = soup.find('div', attrs={"class": "tags"}).find_all('a')
            tags = []
            for tag in tags_list:
                tag_cleaned = tag.getText()
                if tag_cleaned != '+':
                    tags.append(tag_cleaned)

            array_solr_author['keywords_ss'] = tags
            # @todo SAVE TO SOLR
            self.save_solr_registry(array_solr_author)  # [{'id': 93L}, {u'numero_de_inventario_s': u'P02824'}, {u'a

        self.set_url_visited(id_museodelprado_author, 'extracted_author_data')

        return id_museodelprado_author
    # </editor-fold>

    # #########################
    # EXTRACT WIKIPEDIA INFO
    # #########################

    # <editor-fold desc="EXTRACT WIKIPEDIA INFO">
    def find_str(self, s, char):
        index = 0

        if char in s:
            c = char[0]
            for ch in s:
                if ch == c:
                    if s[index:index + len(char)] == char:
                        return index

                index += 1

        return -1

    def add_url_to_database(self,type,url):
        # add url to database
        q = "SELECT count(*) as cuenta,id FROM dbp_urls WHERE TYPE="+str(type)+" AND url LIKE %s"
        if type == 4 or type == 5 or type == 6 or type == 7 or type == 8:
            if 'https://es.wikipedia.org' not in url:
                url = 'https://es.wikipedia.org' + url
        url = url.split("#", 1)[0]
        array_q = (url,)
        cuenta = MysqlND.execute_query(q, array_q).fetchone()
        if cuenta[0] == 0:
            query = "INSERT INTO dbp_urls(type, url) VALUES("+str(type)+", %s)"
            result_q = MysqlND.execute_query(query, array_q)
            return result_q.lastrowid
        else:
            return cuenta[1]

    def extract_general_info_from_wikipedia_to_solr(self, type, id_url_database):
        query_search_url = "SELECT id,url,extracted_wikipedia_info FROM dbp_urls WHERE id=" + str(id_url_database) + " LIMIT " + str(self.limit)
        results = MysqlND.execute_query(query_search_url, ())
        array_solr = {}
        for id_wikipedia,url_wikipedia,extracted_wikipedia_info in results:
            if extracted_wikipedia_info == 0:
                # print(str(id_wikipedia) + " - " + url_wikipedia)
                hdr = {
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                    'Accept-Encoding': 'none',
                    'Accept-Language': 'en-US,en;q=0.8',
                    'Connection': 'keep-alive'}

                req = urllib2.Request(url_wikipedia, headers=hdr)

                try:
                    page = urllib2.urlopen(req)
                    page = urllib2.urlopen(req)
                except urllib2.HTTPError, e:
                    print e.fp.read()
                content = page.read()
                soup = BeautifulSoup(content, 'html.parser')
                array_solr['id'] = int(id_wikipedia)
                array_solr['type'] = type

                array_solr['name_s'] = soup.find('h1', attrs={"id": "firstHeading"}).getText()

                if type == 8:
                    indice_wikipedia = soup.find('div', attrs={"id": "toc"})
                    if indice_wikipedia:
                        indice_wikipedia.decompose()
                    index = soup.find('div', attrs={"class": "mw-parser-output"})
                    if index:
                        for i in index.find_all('h2'):
                            event_fixed_html = ''
                            event_fixed_plain_text = ''
                            section_title = i.contents[0].getText()
                            section_id_lowercase = section_title.lower()
                            section_id_unaccented_string = unidecode.unidecode(section_id_lowercase)
                            for paragraph in i.next_siblings:
                                if paragraph != '\n':
                                    nextNode = paragraph
                                    tag_name = nextNode.name
                                    if tag_name == 'ul':
                                        for l in paragraph.find_all('li'):
                                            event_fixed_html += str(l)
                                            event_fixed_plain_text += '<li>' + l.getText() + '</li>'
                                    if tag_name == 'h2':
                                        break
                            # section_links = i.find('a')
                            if len(event_fixed_plain_text) > 5:
                                array_solr[section_id_unaccented_string + '_txt_es'] = event_fixed_html.encode('utf-8').strip()
                                array_solr[section_id_unaccented_string + '_string_txt_es'] = event_fixed_plain_text.encode('utf-8').strip()
                else:
                    if soup.find('div', attrs={"id": "toc"}):
                        sections_list = soup.find('div', attrs={"id": "toc"}).find_all('a')

                        for tag in sections_list:
                            wikipedia_section_id = tag['href']
                            wikipedia_section_title = tag.find('span', attrs={"class": "toctext"}).getText()
                            # print (wikipedia_section_id + " - " + wikipedia_section_title)
                            wikipedia_section_id = wikipedia_section_id.replace("#", "")
                            node = soup.find('span', attrs={"id": wikipedia_section_id}).parent

                            section_id_lowercase = wikipedia_section_id.lower()
                            section_id_unaccented_string = unidecode.unidecode(section_id_lowercase)
                            p_fixed = ""
                            for paragraph in node.next_siblings:
                                if paragraph != '\n':
                                    nextNode = paragraph
                                    tag_name = nextNode.name
                                    if tag_name == 'p':
                                        parg = str(paragraph)
                                        p_fixed += parg
                                    if tag_name == 'h2':
                                        break
                            if len(p_fixed) > 5:
                                array_solr[section_id_unaccented_string + '_txt_es'] = p_fixed.encode('utf-8').strip()
                                array_solr[section_id_unaccented_string + '_string_txt_es'] = BeautifulSoup(p_fixed, "lxml").text.encode('utf-8').strip()

                    p_fixed = ''
                    if soup.find('div', attrs={"class": "mw-parser-output"}):
                        for paragraph in soup.find('div', attrs={"class": "mw-parser-output"}).find_all('p'):
                            if len(paragraph) > 4:
                                parg = str(paragraph)
                                p_fixed += parg
                        if len(p_fixed) > 5:
                            array_solr['description_txt_es'] = p_fixed.encode('utf-8').strip()
                            array_solr['description_string_txt_es'] = BeautifulSoup(p_fixed, "lxml").text.encode('utf-8').strip()
                    # pprint(array_solr)
                self.save_solr_registry(array_solr)  # [{'id': 93L}, {u'numero_de_inventario_s': u'P02824'}, {u'a
                self.set_url_visited(id_wikipedia, 'extracted_wikipedia_info')



    def extract_wikidata_info_from_wikidata_link(self, url, idioma, wikidata_link):
        type = 7
        entity_instancia_de = ''
        wikidata_url_id = ''
        id_url_wikipedia_author = 0
        wikidata_label_instancia_de= ''
        wikidata_label = ''
        wikidata_instancia_de_id_entity = -1

        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        # Extract ID artwork from URL
        url_splitted = wikidata_link.split("/")
        wikidata_id_entity = str(url_splitted[-1])

        url_query_wikidata = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&props=labels&languages=' + idioma + '&sitefilter=' + idioma + 'wiki&ids=' + wikidata_id_entity
        response = urllib.urlopen(url_query_wikidata)
        data = json.loads(response.read())
        if 'labels' in data['entities'][wikidata_id_entity]:
            if idioma in data['entities'][wikidata_id_entity]['labels']:
                wikidata_label = data['entities'][wikidata_id_entity]['labels'][idioma]['value']  # ex. España
                query = "INSERT IGNORE INTO wikidata_entities_to_labels(id, key_solr,label) VALUES(%s,%s,%s)"
                array = (wikidata_id_entity, 'instancia_de', wikidata_label)
                MysqlND.execute_query(query, array)
                wikidata_url_id = wikidata_id_entity

        wikidata_query = """
                                    SELECT * WHERE {
                                      wd:""" + wikidata_id_entity + """  rdfs:label ?label.
                                      OPTIONAL { wd:""" + wikidata_id_entity + """ wdt:P31 ?instancia_de. }
                                      FILTER(LANGMATCHES(LANG(?label), '""" + idioma.upper() + """'))
                                    }
                                    LIMIT 1
                                    """

        sparql.setQuery(wikidata_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        data_results = results["results"]["bindings"]

        if len(data_results) > 0 and 'instancia_de' in data_results[0]:
            entity_instancia_de = data_results[0]['instancia_de']['value']
            url_splitted = entity_instancia_de.split("/")
            wikidata_instancia_de_id_entity = str(url_splitted[4])

            url_query_wikidata = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&props=labels&languages=' + idioma + '&sitefilter=' + idioma + 'wiki&ids=' + wikidata_instancia_de_id_entity
            response = urllib.urlopen(url_query_wikidata)
            data = json.loads(response.read())
            if 'labels' in data['entities'][wikidata_instancia_de_id_entity]:
                if idioma in data['entities'][wikidata_instancia_de_id_entity]['labels']:
                    wikidata_label_instancia_de = data['entities'][wikidata_instancia_de_id_entity]['labels'][idioma]['value']  # ex. España
                    query = "INSERT IGNORE INTO wikidata_entities_to_labels(id, key_solr,label) VALUES(%s,%s,%s)"
                    array = (wikidata_instancia_de_id_entity, 'instancia_de', wikidata_label_instancia_de)
                    MysqlND.execute_query(query, array)

            if wikidata_instancia_de_id_entity == 'Q5':  # is_human
                type = 6
                q = "SELECT count(*) as cuenta,id FROM dbp_urls WHERE TYPE=" + str(type) + " AND url LIKE %s"
                array_q = (url,)
                cuenta = MysqlND.execute_query(q, array_q).fetchone()
                if cuenta[0] == 0:
                    query = "INSERT INTO dbp_urls(type, url) VALUES(" + str(type) + ", %s)"
                    result_q = MysqlND.execute_query(query, array_q)
                    id_url_wikipedia_author = result_q.lastrowid
                else:
                    id_url_wikipedia_author = cuenta[1]

        return wikidata_url_id, type, wikidata_instancia_de_id_entity, id_url_wikipedia_author, wikidata_label_instancia_de, wikidata_label

    def get_wikidata_info_from_wikipedia_url(self, url, label, idioma='es'):
        # print("get_wikidata_info_from_wikidata_url(url)  -> " + url)
        type = 7
        entity_instancia_de = ''
        wikidata_url_id = ''
        id_url_wikipedia_author = 0
        wikidata_id_entity = -1
        wikidata_label_instancia_de = -1
        wikidata_label = label

        hdr = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'}

        req = urllib2.Request(url, headers=hdr)

        request = requests.get(url)
        if request.status_code == 200:
            try:
                page = urllib2.urlopen(req)
            except urllib2.HTTPError, e:
                print e.fp.read()

            content = page.read()
            soup = BeautifulSoup(content, 'html.parser')
            # 1. find wikidata id

            node = soup.find('a', title=re.compile("^wikidata:"))
            if node:
                # tengo el enlace a wikidata
                wikidata_link = node['href']
                url_splitted = wikidata_link.split("/")
                wikidata_id_entity = str(url_splitted[-1])
                wikidata_url_id, type, entity_instancia_de, id_url_wikipedia_author, wikidata_label_instancia_de, wikidata_label = self.extract_wikidata_info_from_wikidata_link(url, idioma, wikidata_link)
            else:
                if self.representsInt(label):
                    type = 8

                node = soup.find('a', href=re.compile("^https://www.wikidata.org/wiki/Special:EntityPage/"))
                if node:
                    # tengo el enlace a wikidata
                    wikidata_link = node['href']
                    url_splitted = wikidata_link.split("/")
                    wikidata_id_entity = str(url_splitted[-1])
                    wikidata_url_id, type, entity_instancia_de, id_url_wikipedia_author, wikidata_label_instancia_de, wikidata_label = self.extract_wikidata_info_from_wikidata_link(url, idioma, wikidata_link)

        return wikidata_url_id,wikidata_id_entity, type, entity_instancia_de, id_url_wikipedia_author, wikidata_label_instancia_de, wikidata_label

    def representsInt(self, s):
        try:
            int(s)
            return True
        except ValueError:
            return False

    def extract_linking_info(self, html_data):
        html_data = BeautifulSoup(html_data, "html.parser")
        node_links = html_data.find_all('a')
        if node_links:
            for link in node_links:
                href = link['href']
                if '/wiki/' in href:
                    label = str(link.getText())
                    # extract info from this links
                    if 'https://es.wikipedia.org' not in href:
                        href = 'https://es.wikipedia.org' + href

                    if 'commons.wikimedia' not in href:
                        wikidata_url_id, wikidata_id_entity, type, entity_instancia_de, id_url_wikipedia_author, wikidata_label_instancia_de, wikidata_label = self.get_wikidata_info_from_wikipedia_url(href, label)

                        id_database = self.add_url_to_database(type, href)
                        self.extract_general_info_from_wikipedia_to_solr(type, id_database)
                        link['href'] = id_database
                        link['instancia_de'] = entity_instancia_de
                        link['wikidata_id'] = wikidata_id_entity
                        link['id_url_wikipedia_author'] = id_url_wikipedia_author
        return str(html_data)

    def extract_info_from_wikipedia(self,id_wikipedia=-1, url_wikipedia=-1):
        if id_wikipedia == -1:
            values = self.get_next_wikidata_url_pending('extracted_wikipedia_info')
        else:
            values = []
            values.append([str(id_wikipedia),url_wikipedia])

        for (id_wikipedia, url_wikipedia) in values:
            array_solr = {}
            # print(str(id_wikipedia) + " - " + url_wikipedia)
            hdr = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                'Accept-Encoding': 'none',
                'Accept-Language': 'en-US,en;q=0.8',
                'Connection': 'keep-alive'}

            req = urllib2.Request(url_wikipedia, headers=hdr)

            try:
                page = urllib2.urlopen(req)
            except urllib2.HTTPError, e:
                print e.fp.read()
            content = page.read()
            soup = BeautifulSoup(content, 'html.parser')
            array_solr['id'] = int(id_wikipedia)
            array_solr['type'] = 1

            if soup.find('div', attrs={"id": "toc"}):
                sections_list = soup.find('div', attrs={"id": "toc"}).find_all('a')

                for tag in sections_list:
                    wikipedia_section_id = tag['href']
                    wikipedia_section_id = wikipedia_section_id.replace("#", "")
                    node = soup.find('span', attrs={"id": wikipedia_section_id}).parent

                    section_id_lowercase = wikipedia_section_id.lower()
                    section_id_unaccented_string = unidecode.unidecode(section_id_lowercase)
                    p_fixed = ""
                    for paragraph in node.next_siblings:
                        if paragraph != '\n':
                            nextNode = paragraph
                            tag_name = nextNode.name
                            if tag_name == 'p':
                                parg = str(paragraph)
                                p_fixed += parg
                            if tag_name == 'h2':
                                break
                    if len(p_fixed) > 5:
                        array_solr[section_id_unaccented_string + '_txt_es'] = p_fixed.encode('utf-8').strip()
                        array_solr[section_id_unaccented_string + '_formatted_txt_es'] = self.extract_linking_info(p_fixed.encode('utf-8').strip())
                        array_solr[section_id_unaccented_string + '_string_txt_es'] = BeautifulSoup(p_fixed, "lxml").text.encode('utf-8').strip()

            p_fixed = ''
            if soup.find('div', attrs={"class": "mw-parser-output"}):
                for paragraph in soup.find('div', attrs={"class": "mw-parser-output"}).find_all('p'):
                    if len(paragraph) > 4:
                        parg = str(paragraph)
                        p_fixed += parg
                if len(p_fixed) > 5:

                    array_solr['description_txt_es'] = p_fixed
                    array_solr['description_formatted_txt_es'] = self.extract_linking_info(p_fixed)
                    array_solr['description_string_txt_es'] = BeautifulSoup(p_fixed, "lxml").text.encode('utf-8').strip()

            infobox = soup.find('table', attrs={"class": "infobox"})
            if infobox:
                for th in infobox.find_all('tr'):
                    th_ = th.find('th')
                    if th_:
                        child = th_.contents[0].encode('utf-8').strip()
                        if child == 'Técnica':
                            link = th.find('a')
                            if link:
                                href = link['href']
                                if '/wiki/' in href:
                                    link_name = link.getText()
                                    # print (href + " - " + link_name)
                                    array_solr['tecnica_s'] = link_name
                                    id_tecnica_database = self.add_url_to_database(4, href)
                                    array_solr['tecnica_id_i'] = int(id_tecnica_database)
                                    self.extract_general_info_from_wikipedia_to_solr(4, id_tecnica_database)
                                else:
                                    t = th.find('td')
                                    if t:
                                        array_solr['tecnica_s'] = t.getText()
                            else:
                                t = th.find('td')
                                if t:
                                    array_solr['tecnica_s'] = t.getText()
                        if child == 'Estilo':
                            link = th.find('a')
                            if link:
                                href = link['href']
                                if '/wiki/' in href:
                                    link_name = link.getText()
                                    # print (href + " - " + link_name)
                                    array_solr['estilo_s'] = link_name
                                    id_tecnica_database = self.add_url_to_database(5, href)
                                    array_solr['estilo_id_i'] = int(id_tecnica_database)
                                    self.extract_general_info_from_wikipedia_to_solr(5, id_tecnica_database)
                                else:
                                    t = th.find('td')
                                    if t:
                                        array_solr['estilo_s'] = t.getText()
                            else:
                                t = th.find('td')
                                if t:
                                    array_solr['estilo_s'] = t.getText()

            pprint(array_solr)
            self.save_solr_registry(array_solr)  # [{'id': 93L}, {u'numero_de_inventario_s': u'P02824'}, {u'a
            self.set_url_visited(id_wikipedia, 'extracted_wikipedia_info')
    # </editor-fold>


