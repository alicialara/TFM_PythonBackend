# coding=utf-8

import json
import urllib

import requests
from SPARQLWrapper import SPARQLWrapper, JSON
# from bs4 import BeautifulSoup
from bs4 import BeautifulSoup, NavigableString

from InformationExtractor import InformationExtractor
from MysqlND import MysqlND
from Solrindex import Solrindex
from functions import represents_int, get_dbp_url_info


class PreprocessInformation(object):

    def __init__(self):
        self.limit = 1  # number of elements to look for
        self.invalid_tags = ['b', 'i', 'u']

        self.information_extraction = InformationExtractor()
        # Step 8 - Mix artworks data
        # self.mix_artworks_info_from_core_preprocess()
        self.lookfor_next_concepts()



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

    def extract_wikipedia_url(self, wikidata_id_entity, url_wikidata):
        type = 7
        entity_instancia_de = ''
        wikidata_url_id = ''
        id_url_wikipedia_author = 0
        wikidata_label = ''
        id_wikipedia = -1
        wikidata_instancia_de_id_entity = -1
        wikidata_instancia_de_label = ''

        # 1 get wikidata label
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
                    wikidata_instancia_de_label = data['entities'][wikidata_instancia_de_id_entity]['labels']['es']['value']  # ex. España
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
                wikipedia_title = data['entities'][wikidata_id_entity]['sitelinks']['eswiki']['title']  # ex. El Lavatorio (Tintoretto)

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
        query = "SELECT id,key_solr,label FROM wikidata_entities_to_labels WHERE id ='" + str(wd_entity_id) + "' LIMIT 1"
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

            data = {'id': -1, 'name': '', 'image': '', 'description_mp': '', 'description_wp': '', 'date_created': '', 'tecnica': '', 'superficie': '', 'estilo': '', 'alto': '', 'ancho': '', 'identificador_catalogo': '', 'coleccion': '', 'forma_parte_de': '', 'instancia_de': '',
                    'material_empleado': '', 'pais_de_origen': '', 'pais': '', 'ubicacion': '', 'genero_artistico': [], 'movimiento': [], 'representa_a': [], 'keywords_wp': [], 'keywords_mp': [], 'procedencia': [], 'id_wikidata': int(id_wikidata), 'id_wikipedia': int(id_wikipedia),
                    'id_museodelprado': int(id_museodelprado)}

            print "ID Museo del Prado: " + str(id_museodelprado)
            if id_museodelprado != -1:
                results = query_solr.search('id:'+str(id_museodelprado))
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
                        data['tecnica'] = '<a href="'+str(result['tecnica_id_i'])+'">'+str(result['tecnica_s'])+'</a>'

                    if 'estilo_s' in result:
                        if 'estilo_id_i' in result:
                            data['estilo'] = '<a href="'+str(result['estilo_id_i'])+'">'+str(result['estilo_s'])+'</a>'
                        else:
                            data['estilo'] = str(result['estilo_s'])



            print "ID Wikidata: " + str(id_wikidata)
            if id_wikidata != -1:
                results = query_solr.search('id:'+str(id_wikidata))
                for result in results:
                    if data['name'] == '':
                        data['name'] = result['label_s']

                    if 'coleccion_s' in result:
                        wikidata_url = result['coleccion_s']
                        # busco el tag y creo el enlace
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['coleccion'] = '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                'type="' + str(type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                'wikidata_instancia_de_id_entity="' + str(wikidata_instancia_de_id_entity) + '" ' \
                                                'wikidata_instancia_de_label="' + str(wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'

                    if 'forma_parte_de_s' in result:
                        wikidata_url = result['forma_parte_de_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['forma_parte_de'] = '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                'type="' + str(type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                'wikidata_instancia_de_id_entity="' + str(wikidata_instancia_de_id_entity) + '" ' \
                                                'wikidata_instancia_de_label="' + str(wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'

                    if 'genero_artistico_s' in result:
                        wikidata_url = result['genero_artistico_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['genero_artistico'] = '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                'type="' + str(type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                'wikidata_instancia_de_id_entity="' + str(wikidata_instancia_de_id_entity) + '" ' \
                                                'wikidata_instancia_de_label="' + str(wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'

                    if 'instancia_de_s' in result:
                        wikidata_url = result['instancia_de_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['instancia_de'] = '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                'type="' + str(type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                'wikidata_instancia_de_id_entity="' + str(wikidata_instancia_de_id_entity) + '" ' \
                                                'wikidata_instancia_de_label="' + str(wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'
                    if 'material_empleado_s' in result:
                        wikidata_url = result['material_empleado_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['material_empleado'] = '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                'type="' + str(type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                'wikidata_instancia_de_id_entity="' + str(wikidata_instancia_de_id_entity) + '" ' \
                                                'wikidata_instancia_de_label="' + str(wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'
                    if 'pais_de_origen_s' in result:
                        wikidata_url = result['pais_de_origen_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['pais_de_origen'] = '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                'type="' + str(type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                'wikidata_instancia_de_id_entity="' + str(wikidata_instancia_de_id_entity) + '" ' \
                                                'wikidata_instancia_de_label="' + str(wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'
                    if 'pais_s' in result:
                        wikidata_url = result['pais_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['pais'] = '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                'type="' + str(type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                'wikidata_instancia_de_id_entity="' + str(wikidata_instancia_de_id_entity) + '" ' \
                                                'wikidata_instancia_de_label="' + str(wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'
                    if 'ubicacion_s' in result:
                        wikidata_url = result['ubicacion_s']
                        wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                        id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(wikidata_id_entity, wikidata_url)
                        if wikidata_label != '':
                            data['ubicacion'] = '<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                'type="' + str(type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                'wikidata_instancia_de_id_entity="' + str(wikidata_instancia_de_id_entity) + '" ' \
                                                'wikidata_instancia_de_label="' + str(wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>'


                    if 'movimiento_ss' in result:
                        wikidata_urls = result['movimiento_ss']
                        for wikidata_url in wikidata_urls:
                            wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                            id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(wikidata_id_entity, wikidata_url)
                            if wikidata_label != '':
                                data['movimiento'].append('<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                    'type="' + str(type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                    'wikidata_instancia_de_id_entity="' + str(wikidata_instancia_de_id_entity) + '" ' \
                                                    'wikidata_instancia_de_label="' + str(wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>')
                    if 'representa_a_ss' in result:
                        wikidata_urls = result['representa_a_ss']
                        for wikidata_url in wikidata_urls:
                            wikidata_id_entity = wikidata_url.replace("http://www.wikidata.org/entity/", "")
                            id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(wikidata_id_entity, wikidata_url)
                            if wikidata_label != '':
                                data['representa_a'].append('<a href="' + str(id_wikipedia_) + '" id_url_wikipedia_author="' + str(id_url_wikipedia_author) + '" ' \
                                                    'type="' + str(type) + '" wikidata_id="' + str(wikidata_id_entity) + '" wikidata_label="' + str(wikidata_label) + '" ' \
                                                    'wikidata_instancia_de_id_entity="' + str(wikidata_instancia_de_id_entity) + '" ' \
                                                    'wikidata_instancia_de_label="' + str(wikidata_instancia_de_label) + '">' + str(wikidata_label) + '</a>')

            q = "SELECT count(*) as cuenta,id FROM processed_artworks WHERE id_wikidata=" + str(id_wikidata) + " AND id_wikipedia=" + str(id_wikipedia) + " AND id_museodelprado=" + str(id_museodelprado)
            array_q = ()
            print(q)
            cuenta = MysqlND.execute_query(q, array_q).fetchone()
            if cuenta[0] == 0:
                query = "INSERT INTO processed_artworks(id_wikidata,id_wikipedia,id_museodelprado) VALUES(" + str(id_wikidata) + "," + str(id_wikipedia) + ", " + str(id_museodelprado) + ")"
                result_q = MysqlND.execute_query(query, array_q)
                id_processed_artwork = result_q.lastrowid
            else:
                id_processed_artwork = cuenta[1]

            data['id'] = int(id_processed_artwork)

            # pprint(data)

            query_solr = Solrindex("http://localhost:8983/solr/ND_processed")
            query_solr.add_data(data)

            update = "UPDATE urls_relations SET visited=1 WHERE id_wikidata=" + str(id_wikidata) + " AND id_wikipedia=" + str(id_wikipedia) + " AND id_museodelprado=" + str(id_museodelprado)
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
            insert = "INSERT INTO wikidata_instancias_de(id_entity, label) VALUES (%s,%s)"
            array = (wikidata_instancia_de_id_entity,wikidata_instancia_de_label,)
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
        query = "SELECT id FROM processed_references WHERE label = '" + label + "' AND type = '" + str(type) + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            insert = "INSERT INTO processed_references(type,label, url, id_entity, id_entity_instancia_de) VALUES (%s,%s,%s,%s,%s)"
            array = (type, label, url,id_entity,id_entity_instancia_de)
            result_insert = MysqlND.execute_query(insert, array)
            return result_insert.lastrowid
        else:
            res = results.fetchone()
            return res[0]

    def add_character(self, type, label, url='', id_entity=-1, id_entity_instancia_de=-1):
        query = "SELECT id FROM processed_characters WHERE label = '" + label + "' AND type = '" + str(type) + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            insert = "INSERT INTO processed_characters(type,label, url, id_entity, id_entity_instancia_de) VALUES (%s,%s,%s,%s,%s)"
            array = (type, label, url,id_entity,id_entity_instancia_de)
            result_insert = MysqlND.execute_query(insert, array)
            return result_insert.lastrowid
        else:
            res = results.fetchone()
            return res[0]

    def search_for_label(self, label):
        query = "SELECT id,key_solr,label FROM wikidata_entities_to_labels WHERE label LIKE '" + label + "'"
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            query_solr = Solrindex()
            results_solr = query_solr.search('name_s:"' + str(label) + '"')
            if len(results_solr) > 0:
                result_solr = results_solr.docs
                res = result_solr[0]
                print(results_solr)
                res_tipe = res['type']
                type = res_tipe[0]
                if type == 2: #if is artwork
                    authors = res['id_author_is']
                    for id_author in authors:
                        data = get_dbp_url_info(id_author, True)
                        type_author = data['type']
                        url_author = data['url']
                        label_author = data['name_s']
                        # self.add_character(type_author, label_author, url_author, id_entity, id_entity_instancia_de)




        else:
            res = results.fetchone()
            id_entity = res[0]
            key_solr = res[1]
            label = res[2]
            wikidata_url = "http://www.wikidata.org/entity/" + id_entity
            wikidata_id_entity = id_entity
            id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = self.extract_wikipedia_url(wikidata_id_entity, wikidata_url)
            id_instancia_de = self.add_wikidata_instancia_de(wikidata_instancia_de_id_entity, wikidata_instancia_de_label)
            id_entity = self.add_wikidata_id(id_entity, label, id_instancia_de)
            return label, type, '', id_entity, id_instancia_de

    def create_reference(self, l, is_string=False):
        type = 7
        url = ''
        id_entity = -1
        id_instancia_de = -1

        if is_string:
            label = l.strip()
            if not self.search_for_label(label):
                print "No he encontrado en wikidata ni en ningún lao"
            else:
                label, type, url, id_entity, id_instancia_de = self.search_for_label(label)
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

            if 'wikidata_label' in attribs:
                label = l['wikidata_label']
            else:
                label = l.getText()

            if 'wikidata_id' in attribs:
                wikidata_id = l['wikidata_id']
                wikidata_instancia_de_id_entity = l['wikidata_instancia_de_id_entity']
                wikidata_instancia_de_label = l['wikidata_instancia_de_label']
                id_instancia_de = self.add_wikidata_instancia_de(wikidata_instancia_de_id_entity,wikidata_instancia_de_label)
                id_entity = self.add_wikidata_id(wikidata_id, label, id_instancia_de)

        id_processed_reference = self.add_reference(type, label, url, id_entity, id_instancia_de)
        return id_processed_reference




    def update_reference_and_get_final_tag(self, tag):
        if isinstance(tag, list):
            new_list = []
            for t in tag:
                soup = BeautifulSoup(t, 'html.parser')
                if soup.find('a'):
                    for l in soup.find_all('a'):
                        id_reference = self.create_reference(l)
                        l.attrs = {}
                        l["id_reference"] = str(id_reference)
                        new_list.append(str(l))
                else:
                    id_reference = self.create_reference(t, True)
                    new_list.append('<a id_reference="' + str(id_reference) + '">' + t + '</a>')
            return new_list
        else:
            soup = BeautifulSoup(tag, 'html.parser')
            t = ''
            if soup.find('a'):
                for l in soup.find_all('a'):
                    id_reference = self.create_reference(l)
                    l.attrs = {}
                    l["id_reference"] = str(id_reference)
                return str(soup)
            else:
                id_reference = self.create_reference(tag, True)
                return '<a id_reference="' + str(id_reference) + '">' + tag + '</a>'


    def lookfor_next_concepts(self):
        query_solr = Solrindex('http://localhost:8983/solr/ND_processed')
        query = "SELECT id FROM processed_artworks WHERE processed_concepts=0 LIMIT " + str(self.limit)
        # print query
        results = MysqlND.execute_query(query, ())
        for id_artwork_processed in results:
            id_artwork_processed = int(id_artwork_processed[0])
            print("ID preprocessed: " + str(id_artwork_processed))
            results_solr = query_solr.search('id:' + str(id_artwork_processed))
            for result_solr in results_solr:
                name = result_solr['name']
                print("Artwork: " + str(name))

                if 'pais_de_origen' in result_solr:
                    result_solr['pais_de_origen'] = self.update_reference_and_get_final_tag(result_solr['pais_de_origen'])

                if 'genero_artistico' in result_solr:
                    result_solr['genero_artistico'] = self.update_reference_and_get_final_tag(result_solr['genero_artistico'])

                if 'instancia_de' in result_solr:
                    result_solr['instancia_de'] = self.update_reference_and_get_final_tag(result_solr['instancia_de'])

                if 'estilo' in result_solr:
                    result_solr['estilo'] = self.update_reference_and_get_final_tag(result_solr['estilo'])

                if 'procedencia' in result_solr:
                    result_solr['procedencia'] = self.update_reference_and_get_final_tag(result_solr['procedencia'])

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
                    result_solr['material_empleado'] = self.update_reference_and_get_final_tag(result_solr['material_empleado'])

                if 'pais' in result_solr:
                    result_solr['pais'] = self.update_reference_and_get_final_tag(result_solr['pais'])

                if 'keywords_mp' in result_solr:
                    result_solr['keywords_mp'] = self.update_reference_and_get_final_tag(result_solr['keywords_mp'])

                if 'tecnica' in result_solr:
                    result_solr['tecnica'] = self.update_reference_and_get_final_tag(result_solr['tecnica'])

                if 'coleccion' in result_solr:
                    result_solr['coleccion'] = self.update_reference_and_get_final_tag(result_solr['coleccion'])














    # #########################
    # Preprocess authors
    # #########################


