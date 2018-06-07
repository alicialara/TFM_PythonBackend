# coding=utf-8

import json
# from bs4 import BeautifulSoup
import re
import urllib
import urllib2

import requests
from SPARQLWrapper import SPARQLWrapper, JSON
from bs4 import BeautifulSoup, NavigableString

from InformationExtractor import InformationExtractor
from MysqlND import MysqlND
from Solrindex import Solrindex
from functions import represents_int, get_dbp_url_info, get_id_entity_wikidata_from_id_entidad, \
    get_id_entity_wikidata_from_id_artwork, get_id_entity_wikidata_from_id_character, \
    get_id_event_associated_with_element, get_timestamp, get_year


class PreprocessInformation(object):

    def __init__(self):
        self.limit = 10000  # number of elements to look for
        self.invalid_tags = ['b', 'i', 'u']
        self.id_artwork_processed = -1
        self.information_extraction = InformationExtractor()
        # Step 8 - Mix artworks data
        # self.mix_artworks_info_from_core_preprocess()
        # self.lookfor_next_concepts()

    def representsInt(self, s):
        try:
            int(s)
            return True
        except ValueError:
            return False

    def strip_tags(self, html, invalid_tags):
        soup = BeautifulSoup(html, 'html.parser')

        for tag in soup.find_all(True):
            if tag.name in invalid_tags:
                s = ""

                for c in tag.contents:
                    if not isinstance(c, NavigableString):
                        c = self.strip_tags(unicode(c), invalid_tags)
                    s += unicode(c)

                tag.replaceWith(s)

        return soup

    def get_type_from_url_wikidata(self, url_wikidata):
        query = "SELECT type FROM dbp_urls WHERE url LIKE '" + url_wikidata + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            return 7
        else:
            result = results.fetchone()
            type = result[0]
            return int(type)

    def extract_wikipedia_url(self, wikidata_id_entity, url_wikidata):
        type = self.get_type_from_url_wikidata(url_wikidata)
        if type == 0:
            type = 2
        entity_instancia_de = ''
        wikidata_url_id = ''
        id_url_wikipedia_author = 0
        wikidata_label = ''
        id_wikipedia = -1
        wikidata_instancia_de_id_entity = -1
        wikidata_instancia_de_label = ''

        # 1 get wikidata label
        wikidata_id_entity = str(wikidata_id_entity)
        url_query_wikidata = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&props=labels&languages=es&sitefilter=eswiki&ids=' + wikidata_id_entity
        response = urllib.urlopen(url_query_wikidata)
        data = json.loads(response.read())
        if 'labels' in data['entities'][wikidata_id_entity]:
            if 'es' in data['entities'][wikidata_id_entity]['labels']:
                wikidata_label = data['entities'][wikidata_id_entity]['labels']['es']['value']  # ex. España
                if self.representsInt(wikidata_label):
                    type = 8
                query = "INSERT IGNORE INTO wikidata_entities_to_labels(id, key_solr,label) VALUES(%s,%s,%s)"
                array = (wikidata_id_entity, 'instancia_de', wikidata_label)
                MysqlND.execute_query(query, array)

        # 1 get wikidata instancia_de label (if exists)
        wikidata_query = """SELECT * WHERE {
                                wd:""" + wikidata_id_entity + """  rdfs:label ?label.
                                OPTIONAL { wd:""" + wikidata_id_entity + """ wdt:P31 ?instancia_de. }
                                FILTER(LANGMATCHES(LANG(?label), "ES"))
                            }
                            LIMIT 1
                         """
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        sparql.setQuery(wikidata_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        data_results = results["results"]["bindings"]
        if (len(data_results) > 0) and ('instancia_de' in data_results[0]):
            entity_instancia_de = data_results[0]['instancia_de']['value']
            url_splitted = entity_instancia_de.split("/")
            wikidata_instancia_de_id_entity = str(url_splitted[4])
            url_query_wikidata = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&props=labels&languages=es&sitefilter=eswiki&ids=' + wikidata_instancia_de_id_entity
            response = urllib.urlopen(url_query_wikidata)
            data = json.loads(response.read())
            if 'labels' in data['entities'][wikidata_instancia_de_id_entity]:
                if 'es' in data['entities'][wikidata_instancia_de_id_entity]['labels']:
                    wikidata_instancia_de_label = data['entities'][wikidata_instancia_de_id_entity]['labels']['es'][
                        'value']  # ex. España
                    query = "INSERT IGNORE INTO wikidata_entities_to_labels(id, key_solr,label) VALUES(%s,%s,%s)"
                    array = (wikidata_instancia_de_id_entity, 'instancia_de', wikidata_instancia_de_label)
                    MysqlND.execute_query(query, array)

            if wikidata_instancia_de_id_entity == 'Q5':  # is_human
                type = 6
                q = "SELECT count(*) as cuenta,id FROM dbp_urls WHERE TYPE=" + str(type) + " AND url LIKE %s"
                array_q = (url_wikidata,)
                cuenta = MysqlND.execute_query(q, array_q).fetchone()
                if cuenta[0] == 0:
                    query = "INSERT INTO dbp_urls(type, url) VALUES(" + str(type) + ", %s)"
                    result_q = MysqlND.execute_query(query, array_q)
                    id_url_wikipedia_author = result_q.lastrowid
                else:
                    id_url_wikipedia_author = cuenta[1]

            # 3 get wikipedia url, id

            url_query_wikidata = 'https://www.wikidata.org/w/api.php?action=wbgetentities&props=sitelinks&format=json&languages=es&sitefilter=eswiki&ids=' + wikidata_id_entity  # ej. Q75846
            response = urllib.urlopen(url_query_wikidata)
            data = json.loads(response.read())
            if 'eswiki' in data['entities'][wikidata_id_entity]['sitelinks']:
                wikipedia_title = data['entities'][wikidata_id_entity]['sitelinks']['eswiki'][
                    'title']  # ex. El Lavatorio (Tintoretto)

                # get the URL
                wikidata_title_formatted_url = wikipedia_title.replace(" ", "_")
                wikipedia_url = 'https://es.wikipedia.org/wiki/' + wikidata_title_formatted_url
                # check http status
                try:
                    r = requests.head(wikipedia_url)
                    # print(r.status_code)
                    if r.status_code == 200:
                        id_wikipedia = self.information_extraction.add_url_to_database(type, wikipedia_url)

                        self.information_extraction.extract_general_info_from_wikipedia_to_solr(type, id_wikipedia)
                    else:
                        id_wikipedia = -1
                except requests.ConnectionError:
                    print("Failed to connect: " + wikipedia_url)
                # print wikipedia_title

        return id_wikipedia, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label

    # #########################
    # MIX ARTWORKS INFO
    # #########################

    def find_value_in_wikidata_entities_to_labels(self, wd_entity_id):
        query = "SELECT id,key_solr,label FROM wikidata_entities_to_labels WHERE id ='" + str(
            wd_entity_id) + "' LIMIT 1"
        # print query
        results = MysqlND.execute_query(query, ())
        # url_wikidata = "http://www.wikidata.org/entity/" + str(wd_entity_id)

        if results.rowcount > 0:
            result = results.fetchone()
            id, key_solr, label = result[0], result[1], result[2]
        else:
            id, key_solr, label = -1, '', ''

        return id, key_solr, label

    def mix_artworks_info_from_core_preprocess(self):
        query_get_next_data = "SELECT id_wikidata,id_wikipedia,id_museodelprado " \
                              "FROM urls_relations " \
                              "WHERE visited = 0 " \
                              "AND id_museodelprado != -1 " \
                              "AND id_wikipedia != -1 " \
                              "AND id_wikidata != -1 " \
                              "LIMIT " + str(self.limit)
        results_get_next_data = MysqlND.execute_query(query_get_next_data, ())

        for (id_wikidata, id_wikipedia, id_museodelprado) in results_get_next_data:
            query_solr = Solrindex('http://localhost:8983/solr/ND_preprocessing')

            data = {'id': -1, 'name': '', 'image': '', 'description_mp': '', 'description_wp': '', 'date_created': '',
                    'tecnica': '', 'superficie': '', 'estilo': '', 'alto': '', 'ancho': '',
                    'identificador_catalogo': '', 'coleccion': '', 'forma_parte_de': '', 'instancia_de': '',
                    'material_empleado': '', 'pais_de_origen': '', 'pais': '', 'ubicacion': '', 'genero_artistico': [],
                    'movimiento': [], 'representa_a': [], 'keywords_wp': [], 'keywords_mp': [], 'procedencia': [],
                    'id_wikidata': int(id_wikidata), 'id_wikipedia': int(id_wikipedia),
                    'id_museodelprado': int(id_museodelprado)}

            print "ID Museo del Prado: " + str(id_museodelprado)
            if id_museodelprado != -1:
                results = query_solr.search('id:' + str(id_museodelprado))
                for result in results:
                    if 'description_txt_es' in result:
                        data['description_mp'] = str(result['description_txt_es'])

                    data['name'] = str(result['name_s'])

                    data['image'] = str(result['image_s'])
                    if 'keywords_ss' in result:
                        for kw in result['keywords_ss']:
                            data['keywords_mp'].append(str(kw))

                    if 'dateCreated_d' in result:
                        data['date_created'] = int(result['dateCreated_d'])

                    if 'artform_s' in result:
                        data['tecnica'] = str(result['artform_s'])

                    if 'artworkSurface_s' in result:
                        data['superficie'] = str(result['artworkSurface_s'])

                    if 'height_f' in result:
                        data['alto'] = float(result['height_f'])

                    if 'width_f' in result:
                        data['ancho'] = float(result['width_f'])

                    if 'procedencia_s' in result:
                        procedencia_s = result['procedencia_s'].split(";")
                        for p in procedencia_s:
                            p_procedencia = p.split(".")
                            for p_p in p_procedencia:
                                data['procedencia'].append(str(p_p))

                    if 'artEdition_s' in result:
                        data['identificador_catalogo'] = str(result['artEdition_s'])

            print "ID Wikipedia: " + str(id_wikipedia)
            if id_wikipedia != -1:
                results = query_solr.search('id:' + str(id_wikipedia))
                result = results.docs
                if result[0]:
                    result = result[0]
                    if result['description_formatted_txt_es']:
                        description_formatted_txt_es = result['description_formatted_txt_es']
                        soup = BeautifulSoup(description_formatted_txt_es, 'html.parser')

                        all_bolds = soup.find_all('b')
                        data['keywords_wp'] = []
                        for t in all_bolds:
                            data['keywords_wp'].append(str(t.getText()))

                        all_italics = soup.find_all('i')
                        data['keywords_wp'] = []
                        for t in all_italics:
                            data['keywords_wp'].append(str(t.getText()))

                        all_links = soup.find_all('a')
                        for links in all_links:
                            del links['class']
                            del links['id']
                            href = links['href']
                            # print href
                            if self.representsInt(href):
                                data_link = query_solr.search('id:' + str(href))
                                data_link = data_link.docs
                                # pprint(data_link)
                                if data_link[0]:
                                    data_link = data_link[0]
                                    name_link = data_link['name_s']
                                    type_link = data_link['type'][0]
                                    # print(name_link + " -- " + str(type_link))
                                    links['type'] = type_link

                                instancia_de = links['instancia_de']
                                if 'www.wikidata.org' in instancia_de:
                                    instancia_de = instancia_de.replace("http://www.wikidata.org/entity/", "")
                                    links['instancia_de'] = instancia_de
                                    id, key_solr, label = self.find_value_in_wikidata_entities_to_labels(instancia_de)
                                    label = unicode(label, errors='replace')
                                    links['instancia_de_title'] = label

                            else:
                                links.name = 'b'
                                del links['href']
                                del links['title']

                        html = self.strip_tags(str(soup), self.invalid_tags)
                        data['description_wp'] = str(html)

                    if ('tecnica_id_i' in result) and ('tecnica_s' in result):
                        data['tecnica'] = '<a href="' + str(result['tecnica_id_i']) + '">' + str(
                            result['tecnica_s']) + '</a>'

                    if 'estilo_s' in result:
                        if 'estilo_id_i' in result:
                            data['estilo'] = '<a href="' + str(result['estilo_id_i']) + '">' + str(
                                result['estilo_s']) + '</a>'
                        else:
                            data['estilo'] = str(result['estilo_s'])

            print "ID Wikidata: " + str(id_wikidata)
            if id_wikidata != -1:
                results = query_solr.search('id:' + str(id_wikidata))
                for result in results:
                    if data['name'] == '':
                        data['name'] = result['label_s']

                    if 'coleccion_s' in result:
                        wikidata_url = result['coleccion_s']
                        # busco el tag y creo el enlace
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                            wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['coleccion'] = '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(
                                id_url_wikipedia_author) + '" ' \
                                                           'type="' + str(type) + '" wikidata_id="' + str(
                                wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                                                                   'wikidata_instancia_de_id_entity="' + str(
                                wikidata_instancia_de_id_entity) + '" ' \
                                                                   'wikidata_instancia_de_label="' + str(
                                wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'

                    if 'forma_parte_de_s' in result:
                        wikidata_url = result['forma_parte_de_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                            wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['forma_parte_de'] = '<a href="' + str(
                                id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                                                                                'type="' + str(
                                type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(
                                wikidata_label) + '" ' \
                                                  'wikidata_instancia_de_id_entity="' + str(
                                wikidata_instancia_de_id_entity) + '" ' \
                                                                   'wikidata_instancia_de_label="' + str(
                                wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'

                    if 'genero_artistico_s' in result:
                        wikidata_url = result['genero_artistico_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                            wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['genero_artistico'] = '<a href="' + str(
                                id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                                                                                'type="' + str(
                                type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(
                                wikidata_label) + '" ' \
                                                  'wikidata_instancia_de_id_entity="' + str(
                                wikidata_instancia_de_id_entity) + '" ' \
                                                                   'wikidata_instancia_de_label="' + str(
                                wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'

                    if 'instancia_de_s' in result:
                        wikidata_url = result['instancia_de_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                            wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['instancia_de'] = '<a href="' + str(
                                id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                                                                                'type="' + str(
                                type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(
                                wikidata_label) + '" ' \
                                                  'wikidata_instancia_de_id_entity="' + str(
                                wikidata_instancia_de_id_entity) + '" ' \
                                                                   'wikidata_instancia_de_label="' + str(
                                wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'
                    if 'material_empleado_s' in result:
                        wikidata_url = result['material_empleado_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                            wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['material_empleado'] = '<a href="' + str(
                                id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                                                                                'type="' + str(
                                type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(
                                wikidata_label) + '" ' \
                                                  'wikidata_instancia_de_id_entity="' + str(
                                wikidata_instancia_de_id_entity) + '" ' \
                                                                   'wikidata_instancia_de_label="' + str(
                                wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'
                    if 'pais_de_origen_s' in result:
                        wikidata_url = result['pais_de_origen_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                            wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['pais_de_origen'] = '<a href="' + str(
                                id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                                                                                'type="' + str(
                                type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(
                                wikidata_label) + '" ' \
                                                  'wikidata_instancia_de_id_entity="' + str(
                                wikidata_instancia_de_id_entity) + '" ' \
                                                                   'wikidata_instancia_de_label="' + str(
                                wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'
                    if 'pais_s' in result:
                        wikidata_url = result['pais_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                            wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['pais'] = '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(
                                id_url_wikipedia_author) + '" ' \
                                                           'type="' + str(type) + '" wikidata_id="' + str(
                                wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                                                                   'wikidata_instancia_de_id_entity="' + str(
                                wikidata_instancia_de_id_entity) + '" ' \
                                                                   'wikidata_instancia_de_label="' + str(
                                wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'
                    if 'ubicacion_s' in result:
                        wikidata_url = result['ubicacion_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                            wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['ubicacion'] = '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(
                                id_url_wikipedia_author) + '" ' \
                                                           'type="' + str(type) + '" wikidata_id="' + str(
                                wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                                                                   'wikidata_instancia_de_id_entity="' + str(
                                wikidata_instancia_de_id_entity) + '" ' \
                                                                   'wikidata_instancia_de_label="' + str(
                                wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'

                    if 'movimiento_ss' in result:
                        wikidata_urls = result['movimiento_ss']
                        for wikidata_url in wikidata_urls:
                            wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                            id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                                wikidata_id_entity, wikidata_url)
                            if wikidata_label != '':
                                data['movimiento'].append(
                                    '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(
                                        id_url_wikipedia_author) + '" ' \
                                                                   'type="' + str(type) + '" wikidata_id="' + str(
                                        wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                                                                           'wikidata_instancia_de_id_entity="' + str(
                                        wikidata_instancia_de_id_entity) + '" ' \
                                                                           'wikidata_instancia_de_label="' + str(
                                        wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>')
                    if 'representa_a_ss' in result:
                        wikidata_urls = result['representa_a_ss']
                        for wikidata_url in wikidata_urls:
                            wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                            id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                                wikidata_id_entity, wikidata_url)
                            if wikidata_label != '':
                                data['representa_a'].append(
                                    '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(
                                        id_url_wikipedia_author) + '" ' \
                                                                   'type="' + str(type) + '" wikidata_id="' + str(
                                        wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                                                                           'wikidata_instancia_de_id_entity="' + str(
                                        wikidata_instancia_de_id_entity) + '" ' \
                                                                           'wikidata_instancia_de_label="' + str(
                                        wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>')

            q = "SELECT count(*) as cuenta,id FROM processed_artworks WHERE id_wikidata=" + str(
                id_wikidata) + " AND id_wikipedia=" + str(id_wikipedia) + " AND id_museodelprado=" + str(
                id_museodelprado)
            array_q = ()
            print(q)
            cuenta = MysqlND.execute_query(q, array_q).fetchone()
            if cuenta[0] == 0:
                query = "INSERT INTO processed_artworks(id_wikidata,id_wikipedia,id_museodelprado) VALUES(" + str(
                    id_wikidata) + "," + str(id_wikipedia) + ", " + str(id_museodelprado) + ")"
                result_q = MysqlND.execute_query(query, array_q)
                id_processed_artwork = result_q.lastrowid
            else:
                id_processed_artwork = cuenta[1]

            data['id'] = int(id_processed_artwork)

            # pprint(data)

            query_solr = Solrindex("http://localhost:8983/solr/ND_processed")
            query_solr.add_data(data)

            update = "UPDATE urls_relations SET visited=1 WHERE id_wikidata=" + str(
                id_wikidata) + " AND id_wikipedia=" + str(id_wikipedia) + " AND id_museodelprado=" + str(
                id_museodelprado)
            print(update)
            MysqlND.execute_query(update, ())

            print "ID PROCESSED ARTWORK = " + str(id_processed_artwork)

    # #########################
    # Preprocess concepts - references
    # #########################

    def add_wikidata_instancia_de(self, wikidata_instancia_de_id_entity, wikidata_instancia_de_label):
        query = "SELECT id FROM wikidata_instancias_de WHERE id_entity = '" + str(wikidata_instancia_de_id_entity) + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            if wikidata_instancia_de_label == 'página de desambiguación de Wikimedia':
                wikidata_instancia_de_label = ''
            insert = "INSERT INTO wikidata_instancias_de(id_entity, label) VALUES (%s,%s)"
            array = (wikidata_instancia_de_id_entity, wikidata_instancia_de_label,)
            result_insert = MysqlND.execute_query(insert, array)
            return result_insert.lastrowid
        else:
            res = results.fetchone()
            return res[0]

    def add_wikidata_id(self, wikidata_id_entity, wikidata_label, id_instancia_de):
        query = "SELECT id FROM wikidata_entidades WHERE id_entity = '" + wikidata_id_entity + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            insert = "INSERT INTO wikidata_entidades(id_entity, label, id_instancia_de) VALUES (%s,%s,%s)"
            array = (wikidata_id_entity, wikidata_label, str(id_instancia_de))
            result_insert = MysqlND.execute_query(insert, array)
            return result_insert.lastrowid
        else:
            res = results.fetchone()
            return res[0]

    def add_reference(self, type, label, url='', id_entity=-1, id_entity_instancia_de=-1):
        label = label.replace("'","")
        query = "SELECT id, id_entity FROM processed_references WHERE label = '" + label + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            insert = "INSERT INTO processed_references(type,label, url, id_entity, id_entity_instancia_de) VALUES (%s,%s,%s,%s,%s)"
            array = (type, label, url, id_entity, id_entity_instancia_de)
            result_insert = MysqlND.execute_query(insert, array)
            return result_insert.lastrowid
        else:
            res = results.fetchone()
            id = res[0]
            id_entity_old = res[1]
            if id_entity != -1 and (id_entity_old==-1 or id_entity==id_entity_old):
                query = "UPDATE processed_references SET id_entity=%s, id_entity_instancia_de=%s, url=%s WHERE id=%s"
                MysqlND.execute_query(query, (id_entity, id_entity_instancia_de, url, id,))
            return id

    def add_character(self, type, label, id_dbp_urls=-1, url='', id_entity=-1, id_entity_instancia_de=-1):
        label = label.replace("'", "")
        if type == 3:
            id_dbp_urls_wikipedia = -1
            url_dbp_urls_wikipedia = ''
            id_dbp_urls_museodelprado = id_dbp_urls
            url_dbp_urls_museodelprado = url
        else:
            id_dbp_urls_wikipedia = id_dbp_urls
            url_dbp_urls_wikipedia = url
            id_dbp_urls_museodelprado = -1
            url_dbp_urls_museodelprado = ''

        if id_entity != -1:
            query = "SELECT id,id_dbp_urls_wikipedia,id_dbp_urls_museodelprado FROM processed_characters WHERE label = '" + label + "' AND id_entity = '" + str(
                id_entity) + "'"
        else:
            query = "SELECT id,id_dbp_urls_wikipedia,id_dbp_urls_museodelprado FROM processed_characters WHERE label = '" + label + "'"

        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            insert = "INSERT INTO processed_characters(label,id_dbp_urls_wikipedia, url_dbp_urls_wikipedia, url_dbp_urls_museodelprado,id_dbp_urls_museodelprado, id_entity, id_entity_instancia_de) VALUES (%s,%s, %s,%s,%s,%s,%s)"
            array = (
                label, id_dbp_urls_wikipedia, url_dbp_urls_wikipedia, url_dbp_urls_museodelprado, id_dbp_urls_museodelprado,
                id_entity, id_entity_instancia_de)
            result_insert = MysqlND.execute_query(insert, array)
            return result_insert.lastrowid
        else:
            res = results.fetchone()
            id_dbp_urls_wikipedia_ = res[1]
            id_dbp_urls_museodelprado_ = res[2]
            if id_dbp_urls_wikipedia != -1 and id_dbp_urls_wikipedia_ != id_dbp_urls_wikipedia:
                update = "UPDATE processed_characters SET id_dbp_urls_wikipedia = " + str(
                    id_dbp_urls) + ",url_dbp_urls_wikipedia=%s WHERE id=" + str(res[0])
                array = (url,)
                MysqlND.execute_query(update, array)
            elif id_dbp_urls_museodelprado != -1 and id_dbp_urls_museodelprado_ != id_dbp_urls_museodelprado:
                update = "UPDATE processed_characters SET id_dbp_urls_museodelprado = " + str(
                    id_dbp_urls) + ",url_dbp_urls_museodelprado=%s WHERE id=" + str(res[0])
                array = (url,)
                MysqlND.execute_query(update, array)
            return res[0]

    def add_event(self, type, label, year, event_type, id_artwork_related=-1, id_character_related=-1,id_reference_related=-1):
        label = label.encode('utf-8').strip()
        id_reference = -1
        id_event = -1
        type_ = -1
        label = label.replace("'", "")
        if id_character_related != -1:
            id_reference = id_character_related
            type_ = 3
        elif id_artwork_related != -1:
            id_reference = id_artwork_related
            type_ = 2
        elif id_reference_related != -1:
            id_reference = id_reference_related
            type_ = 7
        if id_reference != -1:
            id_event = get_id_event_associated_with_element(year, id_reference, type_)
        query = "SELECT id FROM processed_events WHERE label = %s AND type = '" + str(type) + "'"
        results = MysqlND.execute_query(query, (label,))
        if results.rowcount == 0:
            if id_event == -1:
                insert = "INSERT INTO processed_events(type, label, year, event_type,id_artwork_related,id_character_related,id_reference_related) VALUES (%s,%s, %s,%s,%s, %s,%s)"
                array = (type, label, year, event_type, id_artwork_related, id_character_related, id_reference_related)
                result_insert = MysqlND.execute_query(insert, array)
                return result_insert.lastrowid
            else:
                return id_event
        else:
            res = results.fetchone()
            return res[0]

    def search_label_in_wikidata_and_wikipedia(self, label, idioma_encontrado='es', label_orig=''):
        label_ = label.replace(" ", "_")
        label_ = urllib.quote(label_.encode('utf8'), ':/')

        url_query_wikidata_es = 'https://es.wikipedia.org/w/api.php?action=query&list=search&format=json&srwhat=nearmatch&srsearch=' + label_  # ej. San Marcuola
        url_query_wikidata_en = 'https://en.wikipedia.org/w/api.php?action=query&list=search&format=json&srwhat=nearmatch&srsearch=' + label_  # ej. San Marcuola

        if idioma_encontrado == 'es':
            url_query = url_query_wikidata_es
        else:
            url_query = url_query_wikidata_en

        response = urllib.urlopen(url_query)
        data = json.loads(response.read())
        id_pag_wikipedia = 0

        if 'query' in data and len(data['query']['search']) > 0:
            # bingo
            info = data['query']['search'][0]
            id_pag_wikipedia = info['pageid']
            if 'snippet' in info:
                snippet = info['snippet']
                if '#REDIRECT' in snippet or "#Redirect" in snippet or "#redirección" in snippet:
                    snippet = snippet.replace("#REDIRECT", "")
                    snippet = snippet.replace("#Redirect", "")
                    snippet = snippet.replace("#redirección", "")
                    snippet = snippet.replace("[[", "")
                    snippet = snippet.replace("]]", "")
                    label_ = snippet.strip()
                    if "<" in label_:
                        label_modif = label_.split("<")
                        label_ = label_modif[0]
                    return self.search_label_in_wikidata_and_wikipedia(label_, idioma_encontrado, label_orig=label)

        elif idioma_encontrado == 'es':
            idioma_encontrado = 'en'
            return self.search_label_in_wikidata_and_wikipedia(label, idioma_encontrado, label_orig=label)

        if id_pag_wikipedia != 0:
            url_wikipedia = 'https://' + idioma_encontrado + '.wikipedia.org/wiki/' + label_
            ie = InformationExtractor()
            wikidata_url_id, wikidata_id_entity, type, wikidata_instancia_de_id_entity, id_url_wikipedia_author, wikidata_instancia_de_label, wikidata_label = ie.get_wikidata_info_from_wikipedia_url(
                url_wikipedia, label, idioma_encontrado)
            # print wikidata_id_entity
            if wikidata_id_entity != -1:
                id_instancia_de = self.add_wikidata_instancia_de(wikidata_instancia_de_id_entity,
                                                                 wikidata_instancia_de_label)
                id_entity = self.add_wikidata_id(wikidata_id_entity, wikidata_label, id_instancia_de)

                wikidata_url = 'https://www.wikidata.org/wiki/' + wikidata_id_entity
                if idioma_encontrado == 'es':
                    id_wikipedia_, id_url_wikipedia, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                        wikidata_id_entity, wikidata_url)
                    if label_orig != '':
                        label = label_orig
                    self.add_reference(type, label, url_wikipedia, id_entity, id_instancia_de)
                    return id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label, wikidata_id_entity, wikidata_label
                else:
                    if label_orig != '':
                        label = label_orig
                    self.add_reference(type, label, url_wikipedia, id_entity, id_instancia_de)
                    return -1, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label, wikidata_id_entity, wikidata_label
        if "(" in label:
            label_mod = re.sub(r'\([^)]*\)', '', label)
            label_mod = label_mod.split('(', 1)[0]
            label_mod = label_mod.strip()
            return self.search_label_in_wikidata_and_wikipedia(label_mod, 'es', label_orig=label)
        return False

    def search_author(self, id_author_museodelprado, url_author_museodelprado, label_author_museodelprado):  # mezclado de autores de museoprado con personajes de wikidata/wikipedia
        label_author_museodelprado = re.sub(r'\([^)]*\)', '', label_author_museodelprado)
        name_ = label_author_museodelprado.split(",")

        if len(name_) == 2:
            label_author_museodelprado_mod = str(name_[1]) + " " + str(name_[0])
        else:
            label_author_museodelprado_mod = label_author_museodelprado

        id_entity = -1
        wikidata_instancia_de_id_entity = 'Q5'
        wikidata_instancia_de_label = 'human'
        wikidata_label = ''
        query_solr = Solrindex()
        existe = False
        ensolr = False
        results_solr = query_solr.search('name_s:' + str(label_author_museodelprado) + '~10')
        if len(results_solr) > 0:
            result_solr = results_solr.docs
            res = result_solr[0]
            existe = True
            ensolr = True
        else:
            results_solr = query_solr.search('name_s:' + str(label_author_museodelprado_mod) + '~10')
            if len(results_solr) > 0:
                result_solr = results_solr.docs
                res = result_solr[0]
                existe = True
                ensolr = True
        if not existe:
            values = self.search_label_in_wikidata_and_wikipedia(label_author_museodelprado_mod)
            if not values:
                values = self.search_label_in_wikidata_and_wikipedia(label_author_museodelprado)
                if not values:
                    existe = False
                else:
                    id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label, id_entity, wikidata_label = \
                        values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]
                    existe = True
            else:
                id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label, id_entity, wikidata_label = \
                    values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]
                existe = True

        if existe:
            if ensolr:
                data = res  # @todo a ver si encuentro un caso de estos y lo implemento

        return id_entity, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label

    def get_authors_from_artwork(self, id_artwork):
        query = "SELECT id_museodelprado FROM processed_artworks WHERE id =" + str(id_artwork)
        results = MysqlND.execute_query(query, ())
        if results.rowcount > 0:
            result = results.fetchone()
            id_museodelprado = result[0]
            if id_museodelprado != -1:
                query_solr = Solrindex()
                results_solr = query_solr.search('id:' + str(id_museodelprado))
                if len(results_solr) > 0:
                    result_solr = results_solr.docs
                    res = result_solr[0]
                    ids_authors = res['id_author_is']
                    if ids_authors[0] == 0:  # es anónimo
                        return -1
                    return self.add_authors_data_to_database(ids_authors)
        else:
            return -1

    def add_authors_data_to_database(self, authors):
        characters = []
        for id_author_dbp_urls in authors:
            if id_author_dbp_urls == 0:
                return False
            data = get_dbp_url_info(id_author_dbp_urls, True)
            type_author = data['type']
            url_author = data['url']
            label_author = data['name_s']

            wikidata_id_entity, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.search_author(
                id_author_dbp_urls, url_author, label_author)
            if wikidata_id_entity != -1:
                id_instancia_de = self.add_wikidata_instancia_de(wikidata_instancia_de_id_entity,
                                                                 wikidata_instancia_de_label)
                id_entity = self.add_wikidata_id(wikidata_id_entity, wikidata_label, id_instancia_de)

            else:
                id_entity = -1
                id_instancia_de = -1

            id_character = self.add_character(type_author, label_author, id_author_dbp_urls, url_author, id_entity,
                                              id_instancia_de)
            characters.append(id_character)
        return characters

    def search_id_processed_artwork(self, id_dbp_url_museodelprado):
        query = "SELECT id FROM processed_artworks WHERE id_museodelprado = " + str(id_dbp_url_museodelprado)
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            raise SystemExit(
                'No encuentro id_wikidata en search_artwork_entity_and_instance_of() con id_dbp_url_museodelprado= ' + str(
                    id_dbp_url_museodelprado))
        else:
            res = results.fetchone()
            id_processed_artwork = res[0]
            return id_processed_artwork

    def create_event_created_from_artwork_or_author_birth(self, year):
        id_artwork_related = -1
        id_character_related = -1
        id_reference_related = -1
        query_solr = Solrindex('http://localhost:8983/solr/ND_processed')
        # results_solr = query_solr.search('name_s:"' + str(label) + '"')date_created:1600 AND id:320
        results_solr = query_solr.search(
            'date_created:"' + str(year) + '" AND id:"' + str(self.id_artwork_processed) + '"')
        if len(results_solr) > 0:
            result_solr = results_solr.docs
            res = result_solr[0]
            label_event = res['name']
            type_event = 'Creación obra'
            id_artwork_related = self.id_artwork_processed
        else:
            query_solr = Solrindex()
            if represents_int(year):
                results_solr = query_solr.search('p96_E67_p4_gave_birth_date_d:"' + str(year) + '"')
            else:
                results_solr = []
            encontrado = False
            if len(results_solr) > 0:
                authors = results_solr.docs
                for author in authors:
                    id_author = author['id']
                    name_author = author['name_s']
                    results_solr_author = query_solr.search(
                        'id_author_is:"' + str(id_author) + '" AND id:"' + str(self.id_artwork_processed) + '"')
                    if len(results_solr_author) > 0:
                        data = results_solr_author.docs[0]
                        type_event = 'Nacimiento'
                        label_event = name_author
                        encontrado = True
                        ids_authors = self.add_authors_data_to_database([id_author])
                        id_character_related = ids_authors[0]
                        break
            if not encontrado:
                if represents_int(year):
                    results_solr = query_solr.search('p100i_E69_p4_death_date_d:"' + str(year) + '"')
                else:
                    results_solr = []
                if len(results_solr) > 0:
                    authors = results_solr.docs
                    for author in authors:
                        id_author = author['id']
                        name_author = author['name_s']
                        results_solr_author = query_solr.search(
                            'id_author_is:"' + str(id_author) + '" AND id:"' + str(self.id_artwork_processed) + '"')
                        if len(results_solr_author) > 0:
                            data = results_solr_author.docs[0]
                            type_event = 'Nacimiento'
                            label_event = name_author
                            encontrado = True
                            ids_authors = self.add_authors_data_to_database([id_author])
                            id_character_related = ids_authors[0]
                            break
            if not encontrado:
                return -1

        type_event = unicode(type_event, "utf-8")
        return self.add_event(8, type_event + ' ' + label_event, year, type_event, id_artwork_related,
                              id_character_related, id_reference_related)

    def add_events_data_to_database(self, year, res_solr):
        events_types = ['nacimientos', 'fallecimientos', 'acontecimientos']
        for i in events_types:
            event_type_with_wikipedia_links = i + "_txt_es"
            event_type_without_wikipedia_links = i + "_string_txt_es"
            if event_type_without_wikipedia_links in res_solr:
                html = res_solr[event_type_without_wikipedia_links]
                soup = BeautifulSoup(html, 'html.parser')
                if soup.find('li'):
                    for li in soup.find_all('li'):
                        event_label = li.getText()
                        self.add_event(8, event_label, year, i)
        # además creo los eventos de creación de obra asociado a esta fecha
        return self.create_event_created_from_artwork_or_author_birth(year)

    def get_id_dbp_url_mp_from_id_artwork(self, id_artwork):
        query = "SELECT id_museodelprado FROM processed_artworks WHERE id = " + str(id_artwork)
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            raise SystemExit(
                'No encuentro id_wikidata en search_artwork_entity_and_instance_of() con id_artwork= ' + str(
                    id_artwork))
        else:
            res = results.fetchone()
            id_museodelprado = res[0]
            return id_museodelprado

    def exists_label_as_reference(self, label):
        label = label.replace("'", "")
        query = "SELECT id,type,label,url,id_entity,id_entity_instancia_de FROM processed_references WHERE label = '" + label + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            return False
        else:
            res = results.fetchone()
            return label, res[1], res[3], res[4], res[5], res[0]

    def exists_label_as_character(self, label):
        label = label.replace("'", "")
        query = "SELECT id,label,id_dbp_urls_wikipedia,url_dbp_urls_wikipedia, id_dbp_urls_museodelprado,url_dbp_urls_museodelprado, id_entity, id_entity_instancia_de FROM processed_characters WHERE label = '" + label + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            return False
        else:
            res = results.fetchone()
            id_processed_reference, label, id_dbp_urls_wikipedia, url_dbp_urls_wikipedia, id_dbp_urls_museodelprado, url_dbp_urls_museodelprado, id_entity, id_entity_instancia_de = \
                res[0], res[1], res[2], res[3], res[4], res[5], res[6], res[7]
            if id_dbp_urls_museodelprado != -1:
                type = 3
                url = url_dbp_urls_museodelprado
            else:
                type = 6
                url = url_dbp_urls_wikipedia
            return label, type, url, id_entity, id_entity_instancia_de, id_processed_reference

    def exists_label_as_event(self, label):

        label = label.replace("'", "")
        query = "SELECT id,type,label FROM processed_events WHERE label = '" + label + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            return False
        else:
            res = results.fetchone()
            return label, res[1], '', -1, -1, res[0]

    def search_id_entity_in_database(self, id_entity):
        query = "SELECT id,type,label,url,id_entity,id_entity_instancia_de FROM processed_references WHERE id_entity = '" + str(
            id_entity) + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            query = "SELECT id,label,id_dbp_urls_wikipedia,url_dbp_urls_wikipedia, id_dbp_urls_museodelprado,url_dbp_urls_museodelprado, id_entity, id_entity_instancia_de FROM processed_characters WHERE id_entity = '" + str(
                id_entity) + "'"
            results = MysqlND.execute_query(query, ())
            if results.rowcount == 0:
                return False
            else:
                res = results.fetchone()
                id_processed_reference, label, id_dbp_urls_wikipedia, url_dbp_urls_wikipedia, id_dbp_urls_museodelprado, url_dbp_urls_museodelprado, id_entity, id_entity_instancia_de = \
                    res[0], res[1], res[2], res[3], res[4], res[5], res[6], res[7]
                if id_dbp_urls_museodelprado != -1:
                    type = 3
                    url = url_dbp_urls_museodelprado
                else:
                    type = 6
                    url = url_dbp_urls_wikipedia
                return label, type, url, id_entity, id_entity_instancia_de, id_processed_reference
        else:
            res = results.fetchone()
            return res[2], res[1], res[3], res[4], res[5], res[0]

    def search_id_instancia_de_in_database(self, id_entity_instancia_de):
        query = "SELECT id,type,label,url,id_entity,id_entity_instancia_de FROM processed_references WHERE id_entity_instancia_de = '" + id_entity_instancia_de + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            query = "SELECT id,label,id_dbp_urls_wikipedia,url_dbp_urls_wikipedia, id_dbp_urls_museodelprado,url_dbp_urls_museodelprado, id_entity, id_entity_instancia_de FROM processed_characters WHERE id_entity_instancia_de = '" + id_entity_instancia_de + "'"
            results = MysqlND.execute_query(query, ())
            if results.rowcount == 0:
                return False
            else:
                res = results.fetchone()
                id_processed_reference, label, id_dbp_urls_wikipedia, url_dbp_urls_wikipedia, id_dbp_urls_museodelprado, url_dbp_urls_museodelprado, id_entity, id_entity_instancia_de = \
                    res[0], res[1], res[2], res[3], res[4], res[5], res[6], res[7]
                if id_dbp_urls_museodelprado != -1:
                    type = 3
                    url = url_dbp_urls_museodelprado
                else:
                    type = 6
                    url = url_dbp_urls_wikipedia
                return label, type, url, id_entity, id_entity_instancia_de, id_processed_reference
        else:
            res = results.fetchone()
            return res[2], res[1], res[3], res[4], res[5], res[0]

    def exists_as_entity(self, label):
        label = label.replace("'", "")
        query = "SELECT id,label,id_entity, id_instancia_de FROM wikidata_entidades WHERE label = '" + label + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            return False
        else:
            res = results.fetchone()
            id, label, id_entity, id_instancia_de = res[0], res[1], res[2], res[3]

            return self.search_id_entity_in_database(id)

    def search_for_label(self, label):
        # query = "SELECT id,key_solr,label FROM wikidata_entities_to_labels WHERE label LIKE '" + label + "'"
        # results = MysqlND.execute_query(query, ())
        # if results.rowcount == 0:
        query_solr = Solrindex()
        query_solr_nd_processed = Solrindex('http://localhost:8983/solr/ND_processed')
        label = label.replace('"',"")
        results_solr = query_solr.search('name_s:"' + str(label) + '"')
        id_processed_artwork = -1
        if len(results_solr) > 0:
            result_solr = results_solr.docs
            res = result_solr[0]
            # print(results_solr)
            res_tipe = res['type']
            res_id_dbp_urls = res['id']
            type = res_tipe[0]
            if type == 2:  # if is artwork
                authors = res['id_author_is']
                id_dbp_urls_museodelprado = res['id']
                if len(results_solr) > 1:
                    id_dbp_urls_museodelprado_current_artwork = self.get_id_dbp_url_mp_from_id_artwork(
                        self.id_artwork_processed)
                    id_processed_artwork = self.search_id_processed_artwork(id_dbp_urls_museodelprado)
                    if id_dbp_urls_museodelprado_current_artwork != id_dbp_urls_museodelprado:
                        str_query = 'name_s:"' + str(label) + '" AND id:' + str(
                            id_dbp_urls_museodelprado_current_artwork)
                        results_solr = query_solr.search(str_query)
                        if len(results_solr) > 0:
                            result_solr = results_solr.docs
                            res = result_solr[0]
                            authors = res['id_author_is']
                            id_dbp_urls_museodelprado = res['id']
                            self.add_authors_data_to_database(authors)
                            id_processed_artwork = self.search_id_processed_artwork(id_dbp_urls_museodelprado)
                            return label, type, '', -1, -1, id_processed_artwork  # si es una obra, no necesito almacenar más info
                        else:
                            str_query = 'name:"' + str(label) + '" AND id_museodelprado:' + str(
                                id_dbp_urls_museodelprado_current_artwork)
                            results_solr = query_solr_nd_processed.search(str_query)
                            if len(results_solr) > 0:
                                result_solr = results_solr.docs
                                res = result_solr[0]
                                id_processed_artwork = res['id']
                            return label, type, '', -1, -1, id_processed_artwork  # si es una obra, no necesito almacenar más info

                    else:
                        characters = self.add_authors_data_to_database(authors)
                        id_processed_artwork = self.search_id_processed_artwork(id_dbp_urls_museodelprado)
                        return label, type, '', -1, -1, id_processed_artwork  # si es una obra, no necesito almacenar más info
                else:
                    authors = res['id_author_is']
                    id_dbp_urls_museodelprado = res['id']
                    self.add_authors_data_to_database(authors)
                    id_processed_artwork = self.search_id_processed_artwork(id_dbp_urls_museodelprado)
                    return label, type, '', -1, -1, id_processed_artwork  # si es una obra, no necesito almacenar más info
            if type == 3 or type == 6:
                characters = self.add_authors_data_to_database([res_id_dbp_urls])
                id_character = characters[0]
                return label, type, '', -1, -1, id_character
            if type == 8:
                id_event = self.add_events_data_to_database(label, res)
                return label, type, '', -1, -1, id_event
        else:
            results_solr = query_solr_nd_processed.search('name:"' + str(label) + '"')
            if len(results_solr) > 0:
                type = 2
                result_solr = results_solr.docs
                res = result_solr[0]

                if len(results_solr) > 1:
                    id_processed_artwork = res['id']
                    id_dbp_urls_museodelprado = res['id_museodelprado']
                    id_dbp_urls_museodelprado_current_artwork = self.get_id_dbp_url_mp_from_id_artwork(
                        self.id_artwork_processed)
                    if id_dbp_urls_museodelprado_current_artwork != id_dbp_urls_museodelprado:
                        str_query = 'name_s:"' + str(label) + '" AND id:' + str(
                            id_dbp_urls_museodelprado_current_artwork)
                        results_solr = query_solr.search(str_query)
                        if len(results_solr) > 0:
                            result_solr = results_solr.docs
                            res = result_solr[0]
                            authors = res['id_author_is']
                            id_dbp_urls_museodelprado = res['id']
                            self.add_authors_data_to_database(authors)
                            id_processed_artwork = self.search_id_processed_artwork(id_dbp_urls_museodelprado)
                            return label, type, '', -1, -1, id_processed_artwork  # si es una obra, no necesito almacenar más info
                        else:
                            str_query = 'name:"' + str(label) + '" AND id_museodelprado:' + str(
                                id_dbp_urls_museodelprado_current_artwork)
                            results_solr = query_solr_nd_processed.search(str_query)
                            if len(results_solr) > 0:
                                result_solr = results_solr.docs
                                res = result_solr[0]
                                id_processed_artwork = res['id']
                                return label, type, '', -1, -1, id_processed_artwork  # si es una obra, no necesito almacenar más info
                else:
                    id_processed_artwork = res['id']
                    return label, type, '', -1, -1, id_processed_artwork  # si es una obra, no necesito almacenar más info
            if id_processed_artwork != -1:
                return label, type, '', -1, -1, id_processed_artwork  # si es una obra, no necesito almacenar más info

            # ultimo intento: buscar en wikipedia por si encuentro algo
            # primero parto el string en trozos separados por comas, porque suelen venir así
            # y miro si la primera parte del string existe en wikidata
            label_parts = label.split(",")
            if not represents_int(label_parts[0]) and len(label_parts[0]) > 1:
                values = self.search_label_in_wikidata_and_wikipedia(label_parts[0])
                if values:
                    id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label, wikidata_id_entity, wikidata_label = \
                        values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]
                    # id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label, wikidata_id_entity, wikidata_label = self.search_label_in_wikidata_and_wikipedia(label_parts[0])
                    id_instancia_de = self.add_wikidata_instancia_de(wikidata_instancia_de_id_entity,
                                                                     wikidata_instancia_de_label)
                    id_entity = self.add_wikidata_id(wikidata_id_entity, wikidata_label, id_instancia_de)
                    return label, type, '', id_entity, id_instancia_de, -1
        return False

    def search_label_in_wikipedia(self, label):
        values = self.search_label_in_wikidata_and_wikipedia(label)
        if values:
            id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label, wikidata_id_entity, wikidata_label = values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]
            id_instancia_de = self.add_wikidata_instancia_de(wikidata_instancia_de_id_entity,
                                                             wikidata_instancia_de_label)
            id_entity = self.add_wikidata_id(wikidata_id_entity, wikidata_label, id_instancia_de)
            return label, type, '', id_entity, id_instancia_de, -1

    def exists_already_label(self, label):
        label = label.replace("%", "por ciento")
        exists_as_reference = self.exists_label_as_reference(label)
        if exists_as_reference:
            # print("Encontrado como referencia: " + label)
            label, type, url, id_entity, id_instancia_de, id_processed_reference = exists_as_reference[0], \
                                                                                   exists_as_reference[1], \
                                                                                   exists_as_reference[2], \
                                                                                   exists_as_reference[3], \
                                                                                   exists_as_reference[4], \
                                                                                   exists_as_reference[5]
            return id_processed_reference, type

        exists_as_event = self.exists_label_as_event(label)
        if exists_as_event:
            # print("Encontrado como evento: " + label)
            label, type, url, id_entity, id_instancia_de, id_processed_reference = exists_as_event[0], exists_as_event[
                1], exists_as_event[2], exists_as_event[3], exists_as_event[4], exists_as_event[5]
            return id_processed_reference, type

        exists_as_character = self.exists_label_as_character(label)
        if exists_as_character:
            # print("Encontrado como personaje: " + label)
            label, type, url, id_entity, id_instancia_de, id_processed_reference = exists_as_character[0], \
                                                                                   exists_as_character[1], \
                                                                                   exists_as_character[2], \
                                                                                   exists_as_character[3], \
                                                                                   exists_as_character[4], \
                                                                                   exists_as_character[5]
            return id_processed_reference, type

        exists_as_entity = self.exists_as_entity(label)
        if exists_as_entity:
            # print("Encontrado como id entidad o instancia de (Wikidata): " + label)
            label, type, url, id_entity, id_instancia_de, id_processed_reference = exists_as_entity[0], \
                                                                                   exists_as_entity[1], \
                                                                                   exists_as_entity[2], \
                                                                                   exists_as_entity[3], \
                                                                                   exists_as_entity[4], \
                                                                                   exists_as_entity[5]
            return id_processed_reference, type
        return False

    def create_reference(self, l, is_string=False, createIfNotExistsWikidataEntity=True):
        type = 7
        url = ''
        id_entity = -1
        wikidata_id = -1
        id_instancia_de = -1
        exists = False
        id_reference = -1
        if is_string:
            label = l.strip()
            # print("Searching for label..." + label)
            label = label.replace("'", "").replace("%", "por ciento")

            exists_as_reference = self.exists_label_as_reference(label)
            if exists_as_reference:
                print("Encontrado como referencia")
                label, type, url, id_entity, id_instancia_de, id_processed_reference = exists_as_reference[0], \
                                                                                       exists_as_reference[1], \
                                                                                       exists_as_reference[2], \
                                                                                       exists_as_reference[3], \
                                                                                       exists_as_reference[4], \
                                                                                       exists_as_reference[5]
                return id_processed_reference, type

            exists_as_event = self.exists_label_as_event(label)
            if exists_as_event:
                print("Encontrado como evento")
                label, type, url, id_entity, id_instancia_de, id_processed_reference = exists_as_event[0], \
                                                                                       exists_as_event[1], \
                                                                                       exists_as_event[2], \
                                                                                       exists_as_event[3], \
                                                                                       exists_as_event[4], \
                                                                                       exists_as_event[5]
                return id_processed_reference, type

            exists_as_character = self.exists_label_as_character(label)
            if exists_as_character:
                print("Encontrado como personaje")
                label, type, url, id_entity, id_instancia_de, id_processed_reference = exists_as_character[0], \
                                                                                       exists_as_character[1], \
                                                                                       exists_as_character[2], \
                                                                                       exists_as_character[3], \
                                                                                       exists_as_character[4], \
                                                                                       exists_as_character[5]
                return id_processed_reference, type

            exists_as_entity = self.exists_as_entity(label)
            if exists_as_entity:
                print("Encontrado como id entidad o instancia de")
                label, type, url, id_entity, id_instancia_de, id_processed_reference = exists_as_entity[0], \
                                                                                       exists_as_entity[1], \
                                                                                       exists_as_entity[2], \
                                                                                       exists_as_entity[3], \
                                                                                       exists_as_entity[4], \
                                                                                       exists_as_entity[5]
                return id_processed_reference, type

            values = self.search_for_label(label)
            if not values:
                print("No he encontrado en wikidata ni en ningún lao")
            else:
                label, type, url, id_entity, id_instancia_de, id_reference = values[0], values[1], values[2], values[3], \
                                                                             values[4], values[5]
                if type == 2:
                    id_artwork = id_reference
                    wikidata_id, label_, instancia_de = get_id_entity_wikidata_from_id_artwork(id_artwork)
                elif type == 8:
                    id_event = id_reference
                    wikidata_id, instancia_de, id_processed_reference = -1, -1, id_event
                elif type == 3 or type == 6:
                    id_artwork = id_reference
                    wikidata_id, label_, instancia_de = get_id_entity_wikidata_from_id_character(id_artwork)
                else:
                    wikidata_id, label_, instancia_de = get_id_entity_wikidata_from_id_entidad(id_entity)
                exists = True
        else:
            contents = l.contents[0].encode('utf-8').strip()
            if contents:
                if represents_int(l['href']):
                    id_dbp_url = l['href']
                    data = get_dbp_url_info(id_dbp_url, False)
                    if data:
                        type = data['type']
                        url = data['url']

            attribs = l.attrs

            if 'type' in attribs:
                type = l['type']
            label = l.getText()
            if 'wikidata_label' in attribs:
                wikidata_label = l['wikidata_label']
            else:
                wikidata_label = l.getText()

            if 'wikidata_id' in attribs:
                wikidata_id = l['wikidata_id']
                wikidata_instancia_de_id_entity = l['wikidata_instancia_de_id_entity']
                wikidata_instancia_de_label = l['wikidata_instancia_de_label']
                id_instancia_de = self.add_wikidata_instancia_de(wikidata_instancia_de_id_entity,
                                                                 wikidata_instancia_de_label)
                id_entity = self.add_wikidata_id(wikidata_id, wikidata_label, id_instancia_de)
            type = int(type)

        if type == 6:
            id_processed_reference = self.add_character(type, label, -1, url, id_entity, id_instancia_de)
        elif type == 3:
            if id_reference != -1:
                id_processed_reference = id_reference  # en realidad id_entity en este caso es el id_character
            else:
                id_processed_reference = self.add_character(type, label, -1, url, id_entity, id_instancia_de)
        elif type == 8:
            if id_reference != -1:
                id_processed_reference = id_reference  # en realidad id_entity en este caso es el id_event
            else:
                id_processed_reference = self.create_event_created_from_artwork_or_author_birth(label)
        elif type == 2:
            id_processed_reference = id_reference  # en realidad id_entity en este caso es el id_artwork
        else:
            if url == '' and wikidata_id != -1:
                wikidata_url = "http://www.wikidata.org/entity/" + str(wikidata_id)
                id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(
                    wikidata_id, wikidata_url)
                if wikidata_label == '':
                    wikidata_label = label

                id_instancia_de = self.add_wikidata_instancia_de(wikidata_instancia_de_id_entity,
                                                                 wikidata_instancia_de_label)
                id_entity = self.add_wikidata_id(wikidata_id, wikidata_label, id_instancia_de)

                id_processed_reference = self.add_reference(type, label, url, id_entity, id_instancia_de)
            else:
                if createIfNotExistsWikidataEntity:
                    id_processed_reference = self.add_reference(type, label, url, id_entity, id_instancia_de)
                elif id_entity != -1:
                    id_processed_reference = self.add_reference(type, label, url, id_entity, id_instancia_de)
                else:
                    id_processed_reference = -1

        return id_processed_reference, type

    def update_reference_and_get_final_tag(self, tag):
        if isinstance(tag, list):
            new_list = []
            for t in tag:
                soup = BeautifulSoup(t, 'html.parser')
                if soup.find('a'):
                    for l in soup.find_all('a'):
                        id_reference, type = self.create_reference(l)
                        l.attrs = {}
                        if id_reference != -1:
                            if type == 2:
                                l["id_artwork"] = str(id_reference)
                            elif type == 3 or type == 6:
                                l["id_character"] = str(id_reference)
                            elif type == 8:
                                l["id_event"] = str(id_reference)
                            else:
                                l["id_reference"] = str(id_reference)
                            new_list.append(str(l))
                        else:
                            new_list.append(l.getText())
                else:
                    if (t != '(' and t != ')' and t != '[' and t != ']') and len(t) > 2:
                        if "[" in t:  # elimino info entre corchetes
                            label_mod = re.sub(r'\[[^)]*\]', '', t)
                            label_mod = label_mod.split('[', 1)[0]
                            t = label_mod.strip()
                        id_reference, type = self.create_reference(t, True)
                        if id_reference != -1:
                            if type == 2:
                                new_list.append('<a id_artwork="' + str(id_reference) + '">' + t + '</a>')
                            elif type == 3 or type == 6:
                                new_list.append('<a id_character="' + str(id_reference) + '">' + t + '</a>')
                            elif type == 8:
                                new_list.append('<a id_event="' + str(id_reference) + '">' + t + '</a>')
                            else:
                                new_list.append('<a id_reference="' + str(id_reference) + '">' + t + '</a>')
                        else:
                            new_list.append(t)
            return new_list
        else:
            soup = BeautifulSoup(tag, 'html.parser')
            t = ''
            if soup.find('a'):
                for l in soup.find_all('a'):
                    id_reference, type = self.create_reference(l)
                    l.attrs = {}
                    if id_reference != -1:
                        if type == 2:
                            l["id_artwork"] = str(id_reference)
                        elif type == 3 or type == 6:
                            l["id_character"] = str(id_reference)
                        elif type == 8:
                            l["id_event"] = str(id_reference)
                        else:
                            l["id_reference"] = str(id_reference)
                    else:
                        l = l.getText()

                return str(soup)
            else:
                if (tag != '(' and tag != ')' and tag != '[' and tag != ']') and len(tag) > 2:
                    if "[" in tag:  # elimino info entre corchetes
                        label_mod = re.sub(r'\[[^)]*\]', '', t)
                        label_mod = label_mod.split('[', 1)[0]
                        tag = label_mod.strip()

                    id_reference, type = self.create_reference(tag, True)
                    if id_reference != -1:
                        if type == 2:
                            return '<a id_artwork="' + str(id_reference) + '">' + tag + '</a>'
                        elif type == 3 or type == 6:
                            return '<a id_character="' + str(id_reference) + '">' + tag + '</a>'
                        elif type == 8:
                            return '<a id_event="' + str(id_reference) + '">' + tag + '</a>'
                        else:
                            return '<a id_reference="' + str(id_reference) + '">' + tag + '</a>'
                    else:
                        return tag
                else:
                    return ''

    def lookfor_next_concepts(self):

        query = "SELECT id,id_wikidata,id_wikipedia,id_museodelprado FROM processed_artworks WHERE processed_concepts=0 LIMIT " + str(
            self.limit)
        # print query
        results = MysqlND.execute_query(query, ())
        query_solr = Solrindex('http://localhost:8983/solr/ND_processed')
        for id_artwork_ in results:

            id_artwork_processed = int(id_artwork_[0])
            id_wikidata = int(id_artwork_[1])
            id_wikipedia = int(id_artwork_[2])
            id_museodelprado = int(id_artwork_[3])

            self.id_artwork_processed = id_artwork_processed
            print("ID preprocessed: " + str(id_artwork_processed))

            results_solr = query_solr.search('id:' + str(id_artwork_processed))
            if len(results_solr) > 0:
                result_solr = results_solr.docs[0]

                name = result_solr['name']
                print("Artwork: " + str(name))

                if '_version_' in result_solr: del result_solr['_version_']

                result_solr['authors'] = self.get_authors_from_artwork(id_artwork_processed)
                result_solr['type'] = 2
                result_solr['id_wikidata'] = id_wikidata
                result_solr['id_wikipedia'] = id_wikipedia
                result_solr['id_museodelprado'] = id_museodelprado

                if 'keywords_mp' in result_solr:
                    result_solr['keywords_mp'] = self.update_reference_and_get_final_tag(result_solr['keywords_mp'])

                if 'keywords_wp' in result_solr:
                    result_solr['keywords_wp'] = self.update_reference_and_get_final_tag(result_solr['keywords_wp'])

                if 'superficie' in result_solr:
                    result_solr['superficie'] = self.update_reference_and_get_final_tag(result_solr['superficie'])

                if 'movimiento' in result_solr:
                    result_solr['movimiento'] = self.update_reference_and_get_final_tag(result_solr['movimiento'])

                if 'representa_a' in result_solr:
                    result_solr['representa_a'] = self.update_reference_and_get_final_tag(result_solr['representa_a'])

                if 'ubicacion' in result_solr:
                    result_solr['ubicacion'] = self.update_reference_and_get_final_tag(result_solr['ubicacion'])

                if 'material_empleado' in result_solr:
                    result_solr['material_empleado'] = self.update_reference_and_get_final_tag(
                        result_solr['material_empleado'])

                if 'pais' in result_solr:
                    result_solr['pais'] = self.update_reference_and_get_final_tag(result_solr['pais'])

                if 'tecnica' in result_solr:
                    result_solr['tecnica'] = self.update_reference_and_get_final_tag(result_solr['tecnica'])

                if 'coleccion' in result_solr:
                    result_solr['coleccion'] = self.update_reference_and_get_final_tag(result_solr['coleccion'])

                if 'pais_de_origen' in result_solr:
                    result_solr['pais_de_origen'] = self.update_reference_and_get_final_tag(
                        result_solr['pais_de_origen'])

                if 'genero_artistico' in result_solr:
                    result_solr['genero_artistico'] = self.update_reference_and_get_final_tag(
                        result_solr['genero_artistico'])

                if 'instancia_de' in result_solr:
                    result_solr['instancia_de'] = self.update_reference_and_get_final_tag(result_solr['instancia_de'])

                if 'estilo' in result_solr:
                    result_solr['estilo'] = self.update_reference_and_get_final_tag(result_solr['estilo'])

                if 'procedencia' in result_solr:
                    result_solr['procedencia'] = self.update_reference_and_get_final_tag(result_solr['procedencia'])

                query_solr_final = Solrindex('http://localhost:8983/solr/ND_final')
                query_solr_final.add_data(result_solr)
                update = "UPDATE processed_artworks SET processed_concepts=1 WHERE id= " + str(id_artwork_processed)
                MysqlND.execute_query(update, ())

    def extract_all_wars(self):
        sparql_query = """
                    SELECT ?guerra ?guerraLabel ?instancia_de ?instancia_deLabel ?fecha_de_inicio ?fecha_de_fin WHERE {
                      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],ES". }
                      ?guerra wdt:P31 wd:Q198.
                      OPTIONAL { ?guerra wdt:P31 ?instancia_de. }
                      ?guerra wdt:P580 ?fecha_de_inicio.
                      OPTIONAL { ?guerra wdt:P582 ?fecha_de_fin. }
                    }
                    """
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        data_results = results["results"]["bindings"]
        if len(data_results) > 0:
            for result in data_results:
                guerra_url_wikidata = result['guerra']['value']
                guerra_id_entity = guerra_url_wikidata.replace("http://www.wikidata.org/entity/", "")
                label = self.get_label_from_wikidata(result['guerraLabel']['value'])

                if 'instancia_de' in result:
                    instancia_de = result['instancia_de']['value']
                    instancia_de_id_entity = instancia_de.replace("http://www.wikidata.org/entity/", "")
                    instancia_deLabel = self.get_label_from_wikidata(result['instancia_deLabel']['value'])
                    id_instancia_de = self.add_wikidata_instancia_de(instancia_de_id_entity, instancia_deLabel)
                else:
                    id_instancia_de = -1

                id_entity = self.add_wikidata_id(guerra_id_entity, label, id_instancia_de)

                fecha_de_inicio = result['fecha_de_inicio']['value']
                timestamp_end_date = ''
                if 'fecha_de_fin' in result:
                    fecha_de_fin = result['fecha_de_fin']['value']
                    timestamp_end_date = get_timestamp(fecha_de_fin)
                timestamp_init_date = get_timestamp(fecha_de_inicio)

                query = "SELECT id FROM processed_events WHERE label = %s AND type = '8'"
                results = MysqlND.execute_query(query, (label,))
                if results.rowcount == 0:
                    year = get_year(timestamp_init_date)
                    id_reference = self.add_reference('7', label, url='', id_entity=id_entity, id_entity_instancia_de=id_instancia_de)
                    insert = "INSERT INTO processed_events(type, label, year, init, end, event_type,id_artwork_related,id_character_related,id_reference_related) VALUES (%s,%s, %s,%s, %s,%s,%s, %s,%s)"
                    array = ('8', label, year, timestamp_init_date, timestamp_end_date, 'guerra', '-1', '-1', id_reference)
                    MysqlND.execute_query(insert, array)

        return True

    def get_label_from_wikidata(self, wikidata_id_entity):
        wikidata_id_entity = wikidata_id_entity.replace("http://www.wikidata.org/entity/", "")
        url_query_wikidata = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&props=labels&sitefilter=eswiki|enwiki&ids=' + wikidata_id_entity
        response = urllib.urlopen(url_query_wikidata)
        data = json.loads(response.read())
        if 'labels' in data['entities'][wikidata_id_entity]:
            if 'es' in data['entities'][wikidata_id_entity]['labels']:
                return data['entities'][wikidata_id_entity]['labels']['es']['value']  # ex. España
            elif 'en' in data['entities'][wikidata_id_entity]['labels']:
                return data['entities'][wikidata_id_entity]['labels']['en']['value']
            elif 'fr' in data['entities'][wikidata_id_entity]['labels']:
                return data['entities'][wikidata_id_entity]['labels']['fr']['value']
            else:
                return ''
        return ''

    def create_events_from_all_characters(self):
        query = "SELECT id,label,id_dbp_urls_wikipedia,url_dbp_urls_wikipedia,id_dbp_urls_museodelprado,url_dbp_urls_museodelprado,id_entity,id_entity FROM processed_characters where id>1189"
        results = MysqlND.execute_query(query, ())
        ie = InformationExtractor()
        # query_solr = Solrindex('http://localhost:8983/solr/ND_preprocessing_pruebas')
        for character in results:
            id, label, id_dbp_urls_wikipedia, url_dbp_urls_wikipedia, id_dbp_urls_museodelprado, url_dbp_urls_museodelprado, id_entity, id_entity_instancia_de = character[0], character[1], character[2], character[3], character[4], character[5], character[6], character[7]
            # if id_entity == -1:
            #     if id_dbp_urls_wikipedia != -1:
            #         wikidata_url_id, wikidata_id_entity, type, wikidata_instancia_de_id_entity, id_url_wikipedia_author, wikidata_instancia_de_label, wikidata_label = ie.get_wikidata_info_from_wikipedia_url(url_dbp_urls_wikipedia, label)
            #         id_instancia_de = self.add_wikidata_instancia_de(wikidata_instancia_de_id_entity, wikidata_instancia_de_label)
            #         id_entity = self.add_wikidata_id(wikidata_id_entity, wikidata_label, id_instancia_de)
            #         query = "UPDATE processed_characters SET id_entity=" + str(id_entity) + ",id_entity_instancia_de=" + str(id_instancia_de) + " WHERE id=" + str(id)
            #         MysqlND.execute_query(query, ())
            # else:
            #     if id_dbp_urls_wikipedia == -1:
            #         wikidata_id_entity = self.get_character_wikidata_entity_from_id_character(id_entity)
            #         if wikidata_id_entity:
            #             more_info_from_wikidata = self.get_more_info_from_wikidata_id_entity(wikidata_id_entity)
            #             # pprint(more_info_from_wikidata)
            #             if 'sitelinks' in more_info_from_wikidata['entities'][wikidata_id_entity]:
            #                 if 'eswiki' in more_info_from_wikidata['entities'][wikidata_id_entity]['sitelinks']:
            #                     find_url = more_info_from_wikidata['entities'][wikidata_id_entity]['sitelinks']['eswiki']['title']
            #                     url_wikipedia = 'https://es.wikipedia.org/wiki/' + find_url
            #                     q = "SELECT count(*) as cuenta,id FROM dbp_urls WHERE type=6 AND url LIKE %s"
            #                     array_q = (url_wikipedia,)
            #                     cuenta = MysqlND.execute_query(q, array_q).fetchone()
            #                     if cuenta[0] == 0:
            #                         query = "INSERT INTO dbp_urls(type, url) VALUES(6, %s)"
            #                         result_q = MysqlND.execute_query(query, array_q)
            #                         id_dbp_urls_wikipedia = result_q.lastrowid
            #                     else:
            #                         id_dbp_urls_wikipedia = cuenta[1]
            #                     query2 = "UPDATE processed_characters SET id_dbp_urls_wikipedia=" + str(id_dbp_urls_wikipedia) + " , url_dbp_urls_wikipedia=%s WHERE id=" + str(id)
            #                     MysqlND.execute_query(query2, (url_wikipedia,))
            if id_entity != -1:
                wikidata_id_entity = self.get_character_wikidata_entity_from_id_character(id_entity)
                fecha_nacimiento = self.extract_property_in_wikidata_url(wikidata_id_entity,'P569')
                lugar_nacimiento, id_entity_lugar_nacimiento = self.extract_property_in_wikidata_url(wikidata_id_entity,'P19', get_id_entity=True)
                fecha_fallecimiento = self.extract_property_in_wikidata_url(wikidata_id_entity,'P570')
                lugar_fallecimiento, id_entity_lugar_fallecimiento = self.extract_property_in_wikidata_url(wikidata_id_entity,'P20', get_id_entity=True)

                if id_entity_lugar_nacimiento != '':
                    lugar_nacimiento = self.get_label_from_wikidata(id_entity_lugar_nacimiento)
                if id_entity_lugar_fallecimiento != '':
                    lugar_fallecimiento = self.get_label_from_wikidata(id_entity_lugar_fallecimiento)
                self.insert_events_of_character(fecha_nacimiento,lugar_nacimiento, fecha_fallecimiento, lugar_fallecimiento, id, label)

            #     self.information_extraction.extract_info_from_wikipedia(id_dbp_urls_wikipedia, url_dbp_urls_wikipedia)

    def insert_events_of_character(self, fecha_nacimiento_, lugar_nacimiento, fecha_fallecimiento_, lugar_fallecimiento, id_character, label_character):
        fecha_nacimiento_ = fecha_nacimiento_.replace("Gregorian", "")
        fecha_fallecimiento_ = fecha_fallecimiento_.replace("Gregorian", "")
        fecha_nacimiento = get_timestamp(fecha_nacimiento_)
        fecha_fallecimiento = get_timestamp(fecha_fallecimiento_)
        year_birth = get_year(fecha_nacimiento)
        year_death = get_year(fecha_fallecimiento)
        label_birth = "Nacimiento de " + label_character + " en " + lugar_nacimiento + " en " + fecha_nacimiento_
        label_death = "Fallecimiento de " + label_character + " en " + lugar_fallecimiento + " en " + fecha_fallecimiento_
        type = 8
        event_type_birth = 'nacimientos'
        event_type_death = 'fallecimientos'

        query = "SELECT id FROM processed_events WHERE label = %s AND event_type = '" + str(event_type_birth) + "'"
        results = MysqlND.execute_query(query, (label_birth,))
        if results.rowcount == 0:
            insert = "INSERT INTO processed_events(type, label, year, event_type, place, init, end,id_artwork_related,id_character_related,id_reference_related) VALUES (%s,%s, %s,%s,%s, %s,%s,%s,%s,%s)"
            array = (type, label_birth, year_birth, event_type_birth, lugar_nacimiento, fecha_nacimiento, fecha_fallecimiento, -1, id_character, -1)
            result_insert = MysqlND.execute_query(insert, array)


        query = "SELECT id FROM processed_events WHERE label = %s AND event_type = '" + str(event_type_death) + "'"
        results = MysqlND.execute_query(query, (label_death,))
        if results.rowcount == 0:
            insert = "INSERT INTO processed_events(type, label, year, event_type, place, init, end,id_artwork_related,id_character_related,id_reference_related) VALUES (%s,%s, %s,%s,%s, %s,%s,%s,%s,%s)"
            array = (type, label_death, year_death, event_type_death, lugar_fallecimiento, fecha_nacimiento, fecha_fallecimiento, -1, id_character, -1)
            result_insert = MysqlND.execute_query(insert, array)
        return True


    def get_character_wikidata_entity_from_id_character(self, id_character):
        query = "SELECT id_entity FROM wikidata_entidades WHERE id =%s LIMIT 1"
        results = MysqlND.execute_query(query, (id_character,))
        if results.rowcount > 0:
            return results.fetchone()[0]
        else:
            return False

    def extract_property_in_wikidata_url(self, id_entity, property, get_id_entity=False):
        url = 'https://www.wikidata.org/wiki/' + id_entity
        hdr = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'}

        req = urllib2.Request(url, headers=hdr)
        try:
            page = urllib2.urlopen(req)
            page = urllib2.urlopen(req)
        except urllib2.HTTPError as e:
            print(e.fp.read())
        content = page.read()
        soup = BeautifulSoup(content, 'html.parser')
        node = soup.find('div', attrs={"id": property})
        if node == None:
            if get_id_entity:
                return '', ''
            return ''
        node = node.find('div', attrs={'class': 'wikibase-snakview-value'})
        if get_id_entity:
            node = node.find('a')
            if node == None:
                return '',''
            return node.getText(), node['title']
        if property == 'P18':
            node = node.find('img')
            return node['src']
        return node.getText()

#     def extract_all_characters_from_wikidata(self):
#         sparql_query = """
#                             SELECT ?label ?labelLabel ?fecha_de_nacimiento ?fecha_de_fallecimiento WHERE {
#   SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],ES". }
#   ?label wdt:P31 wd:Q5.
#   OPTIONAL { ?label wdt:P569 ?fecha_de_nacimiento. }
#   OPTIONAL { ?label wdt:P570 ?fecha_de_fallecimiento. }
# }
#                             """
#         sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
#         sparql.setQuery(sparql_query)
#         sparql.setReturnFormat(JSON)
#         results = sparql.query().convert()
#         data_results = results["results"]["bindings"]
#         if len(data_results) > 0:
#             for result in data_results:
#                 character_url_wikidata = result['label']['value']
#                 character_id_entity = character_url_wikidata.replace("http://www.wikidata.org/entity/", "")
#                 character_label = self.get_label_from_wikidata(result['labelLabel']['value'])
#                 fecha_de_nacimiento = result['fecha_de_nacimiento']['value']
#                 fecha_de_fallecimiento = result['fecha_de_fallecimiento']['value']
#                 id_instancia_de = 812
#                 id_entity = self.add_wikidata_id(character_id_entity, character_label, id_instancia_de)
#
#                 pprint(more_info_from_wikidata)

    def get_more_info_from_wikidata_id_entity(self, wikidata_id_entity):
        url = "https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids=" + wikidata_id_entity + "&languages=es"
        response = urllib.urlopen(url)
        return json.loads(response.read())
        # if 'labels' in data['entities'][wikidata_instancia_de_id_entity]:
    # #########################
    # Preprocess authors
    # #########################
