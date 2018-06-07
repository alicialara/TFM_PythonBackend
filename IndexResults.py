# coding=utf-8
import json
import urllib2
from pprint import pprint

from bs4 import BeautifulSoup

import functions
from MysqlND import MysqlND
from PreprocessInformation import PreprocessInformation
from Solrindex import Solrindex


class IndexResults(object):

    def __init__(self):
        self.segments_per_itinerarie = 10
        self.artworks_labels = functions.get_pickle('labels/labels_artworks.pickle')
        self.segments = functions.get_pickle('labels/segments.pickle')
        self.labels_references = functions.get_pickle('labels/labels_references.pickle')
        self.labels_characters = functions.get_pickle('labels/labels_characters.pickle')
        self.labels_events = functions.get_pickle('labels/labels_events.pickle')
        self.labels_ne_entities = functions.get_pickle('labels/labels_ne_entities.pickle')

    def index_itineraries(self, dir="single_topic_itineraries"):
        dirs = functions.read_dir("./" + dir)
        for file in dirs:
            ruta = './' + dir + '/' + file
            html = ''
            for line in open(ruta, 'r'):
                fila = line.rstrip()
                html += fila + ' '

            soup = BeautifulSoup(html, 'html.parser')
            title = soup.find('title').getText()
            paragraphs = ''
            count_p = 0
            for p in soup.find_all('p'):
                paragraphs += str(p)
                count_p += 1

            file_data = file.replace(".txt", "").split("_")
            type_ne = file_data[0]
            id_entity = file_data[1]

            label_entity = self.labels_ne_entities[file.replace(".txt", "")][0]

            names_ne = []
            if type_ne == 'CH':
                query = "SELECT label FROM processed_characters WHERE id_entity=" + str(id_entity)
                data = MysqlND.execute_query(query, ())
                if data.rowcount > 0:
                    for d in data:
                        names_ne.append(d[0])
            elif type_ne == 'REF':
                query = "SELECT label FROM processed_references WHERE id_entity=" + str(id_entity)
                data = MysqlND.execute_query(query, ())
                if data.rowcount > 0:
                    for d in data:
                        names_ne.append(d[0])
            elif type_ne == 'ARTW':
                names_ne.append(self.artworks_labels[id_entity])

            names_ne = ','.join(names_ne)

            q = "SELECT id FROM index_itineraries WHERE ne=%s"
            array_q = (label_entity,)
            results = MysqlND.execute_query(q, array_q)
            if results.rowcount == 0:
                query = "INSERT INTO index_itineraries (dir, ne, names_ne, title, count_paragraphs, html) VALUES (%s,%s,%s,%s,%s,%s); "
                result_q = MysqlND.execute_query(query, (dir, label_entity, names_ne, title, count_p, paragraphs))

    def update_artworks_basic_data(self):
        query = "SELECT id FROM processed_artworks"
        artworks = MysqlND.execute_query(query, ())
        for artwork in artworks:
            id = artwork[0]
            data = self.get_solr_artwork_data(id, multiple=False)
            if data:
                # label = data['name']
                # image = data['image']
                # update = "UPDATE processed_artworks SET name=%s,image=%s WHERE id=" + str(id)
                # MysqlND.execute_query(update, (label, image,))

                # del data['description_mp']
                # del data['description_wp']
                del data['image']
                del data['name']
                del data['lookfor']
                json_data = json.dumps(data, ensure_ascii=False)
                update = "UPDATE processed_artworks SET json_metadata=%s WHERE id=" + str(id)
                MysqlND.execute_query(update, (json_data,))

            else:
                print("NOPE" + str(id))

        # query = "SELECT id,label FROM processed_references WHERE type=2"
        # rs = MysqlND.execute_query(query, ())
        # for result in rs:
        #     results[result[0]] = result[1]

    def get_solr_artwork_data(self, query, multiple=False):
        query_solr_final = Solrindex('http://localhost:8983/solr/ND_final')
        if functions.represents_int(query):
            query = 'id:' + str(query)
        result = query_solr_final.search(query)
        if len(result.docs) > 0:
            if multiple:
                return result.docs
            else:
                return result.docs[0]
        return False

    def update_images_artworks(self):
        query = "SELECT processed_artworks.id,processed_artworks.image, dbp_urls.url FROM processed_artworks INNER JOIN dbp_urls ON processed_artworks.id_museodelprado = dbp_urls.id"
        artworks = MysqlND.execute_query(query, ())
        for artwork in artworks:
            id, image, url_museodelprado = artwork[0], artwork[1], artwork[2]
            # c = httplib.HTTPConnection(image)
            # c.request("HEAD", '')
            # if c.getresponse().status == 200:
            #     print('web site exists')
            # else:
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
            if soup.find('meta', attrs={"name": "twitter:image"}):
                image_s = soup.find('meta', attrs={"name": "twitter:image"})
                image_s = image_s['content']
                update = "UPDATE processed_artworks SET image=%s WHERE id=" + str(id)
                pprint(update)
                MysqlND.execute_query(update, (image_s,))
            else:
                print("NO ENCUENTRO imagen para " + url_museodelprado)


    def update_image_and_dates_from_wp_character(self, id_entity):
        if id_entity == -1:
            return True
        query = "SELECT id_entity FROM wikidata_entidades WHERE id = " + str(id_entity)
        results = MysqlND.execute_query(query, ())
        result = results.fetchone()
        wikidata_id_entity = result[0]
        pi = PreprocessInformation()
        fecha_nacimiento = pi.extract_property_in_wikidata_url(wikidata_id_entity, 'P569')
        lugar_nacimiento, id_entity_lugar_nacimiento = pi.extract_property_in_wikidata_url(wikidata_id_entity, 'P19', get_id_entity=True)
        fecha_fallecimiento = pi.extract_property_in_wikidata_url(wikidata_id_entity, 'P570')
        lugar_fallecimiento, id_entity_lugar_fallecimiento = pi.extract_property_in_wikidata_url(wikidata_id_entity, 'P20', get_id_entity=True)

        if id_entity_lugar_nacimiento != '':
            lugar_nacimiento = pi.get_label_from_wikidata(id_entity_lugar_nacimiento)
        if id_entity_lugar_fallecimiento != '':
            lugar_fallecimiento = pi.get_label_from_wikidata(id_entity_lugar_fallecimiento)

        image = pi.extract_property_in_wikidata_url(wikidata_id_entity, 'P18')
        if fecha_nacimiento != '':
            fecha_nacimiento_ = fecha_nacimiento.replace("Gregorian", "")
            fecha_nacimiento = functions.get_timestamp(fecha_nacimiento_).replace(" 00:00:00","")
        if fecha_fallecimiento != '':
            fecha_fallecimiento_ = fecha_fallecimiento.replace("Gregorian", "")
            fecha_fallecimiento = functions.get_timestamp(fecha_fallecimiento_).replace(" 00:00:00","")

        if fecha_nacimiento != '':
            update = "UPDATE processed_characters SET fecha_nacimiento=%s,fecha_fallecimiento=%s,lugar_nacimiento=%s,lugar_fallecimiento=%s,image=%s WHERE id_entity=" + str(id_entity)
            pprint(update)
            MysqlND.execute_query(update, (fecha_nacimiento, fecha_fallecimiento, lugar_nacimiento, lugar_fallecimiento, image,))
        elif image != '':
            update = "UPDATE processed_characters SET image=%s WHERE id_entity=" + str(id_entity)
            pprint(update)
            MysqlND.execute_query(update, (image,))

    def url_exists(self, url):
        return False
        # a = urllib.urlopen(url)
        # if a.getcode() != 404:
        #     return True
        # else:
        #     return False

    def update_images_data(self):
        pi = PreprocessInformation()
        query = "SELECT id,image, url_dbp_urls_wikipedia, url_dbp_urls_museodelprado, id_entity FROM processed_characters"
        results = MysqlND.execute_query(query, ())
        for res in results:
            id, image, url_dbp_urls_wikipedia, url_dbp_urls_museodelprado, id_entity = res[0], res[1], res[2], res[3], res[4]
            exists = False
            if image:
                exists = self.url_exists(image)
            if not exists:
                image_url = '/public/img/profile_default.png'
                if len(url_dbp_urls_wikipedia) > 3:
                    if id_entity != -1:
                        query = "SELECT id_entity FROM wikidata_entidades WHERE id = " + str(id_entity)
                        results = MysqlND.execute_query(query, ())
                        result = results.fetchone()
                        wikidata_id_entity = result[0]
                        image = pi.extract_property_in_wikidata_url(wikidata_id_entity, 'P18')
                        if len(image) > 3:
                            image_url = image
                            exists = True
                if len(url_dbp_urls_museodelprado) > 3:
                    if not exists:
                        hdr = {
                            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                            'Accept-Encoding': 'none',
                            'Accept-Language': 'en-US,en;q=0.8',
                            'Connection': 'keep-alive'}
                        req = urllib2.Request(url_dbp_urls_museodelprado, headers=hdr)
                        try:
                            page = urllib2.urlopen(req)
                        except urllib2.HTTPError, e:
                            print e.fp.read()
                        content = page.read()
                        soup = BeautifulSoup(content, 'html.parser')
                        if soup.find('meta', attrs={"name": "twitter:image"}):
                            image = soup.find('meta', attrs={"name": "twitter:image"})
                            image = image['content']
                            if len(image) > 3:
                                image_url = image
                                exists = True

                update = "UPDATE processed_artworks SET image=%s WHERE id=" + str(id)
                pprint(update)
                MysqlND.execute_query(update, (str(image_url),))


    def update_characters_data(self):
        query = "SELECT processed_characters.id, processed_characters.label, processed_characters.id_entity, dbp_urls1.id AS id_wp, dbp_urls1.url AS url_wp, dbp_urls2.id AS id_mp, dbp_urls2.url AS url_mp FROM processed_characters " \
                "LEFT JOIN dbp_urls dbp_urls1 ON dbp_urls1.id = processed_characters.id_dbp_urls_wikipedia " \
                "LEFT JOIN dbp_urls dbp_urls2 ON dbp_urls2.id = processed_characters.id_dbp_urls_museodelprado " \
                "WHERE (dbp_urls1.id IS NOT NULL OR dbp_urls2.url IS NOT NULL) and  (processed_characters.id> 1579)"
        results = MysqlND.execute_query(query, ())
        for res in results:
            data = {}
            id, label, id_entity, id_wp, url_wp, id_mp, url_mp = res[0], res[1], res[2], res[3], res[4], res[5], res[6]

            data['id'] = id
            data['label'] = label
            data['fecha_nacimiento'] = ''
            data['fecha_fallecimiento'] = ''
            data['lugar_nacimiento'] = ''
            data['lugar_fallecimiento'] = ''
            data['url_s'] = ''
            data['image'] = ''
            data['description_mp'] = ''
            data['description_wp'] = ''
            es_wp = False
            es_mp = False
            if functions.represents_int(id_wp):
                es_wp = True
            if functions.represents_int(id_mp):
                es_mp = True

            if es_wp:
                query = 'id:' + str(id_wp)
                self.update_image_and_dates_from_wp_character(id_entity)
                query_solr_final = Solrindex('http://localhost:8983/solr/ND_preprocessing')
                result = query_solr_final.search(query)
                if len(result.docs) > 0:
                    doc = result.docs[0]
                    texto = ''
                    for key, value in doc.iteritems():
                        if '_txt_es' in key:
                            texto += '<h3 class="wp_title">' + key.replace("_txt_es", "").replace("_", " ").replace("string", "").capitalize() + '</h3><p class="wp_paragraph">' + functions.cleanhtml(value) + '</p>'
                    data['description_wp'] = texto
                else:
                    if id_entity != -1:
                        preprocess_information = PreprocessInformation()
                        values = preprocess_information.search_label_in_wikidata_and_wikipedia(label)
                        if values:
                            id_wikipedia_dbp_urls, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label, wikidata_id_entity, wikidata_label = values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]

                            data['description'] = ''
                            if id_wikipedia_dbp_urls != -1:
                                query = 'id:' + str(id_wikipedia_dbp_urls)
                                query_solr_final = Solrindex('http://localhost:8983/solr/ND_preprocessing')
                                result = query_solr_final.search(query)
                                if len(result.docs) > 0:
                                    doc = result.docs[0]
                                    texto = ''
                                    for key, value in doc.iteritems():
                                        if '_string_txt_es' in key:
                                            texto += '<h3 class="wp_title">' + key.replace("_string_txt_es", "").replace("description", "descripción").replace("_", " ").capitalize() + '</h3><p class="wp_paragraph">' + functions.cleanhtml(value) + '</p>'
                                    data['description_wp'] = texto

            # if es_mp:
            #     query = 'id:' + str(id_mp)
            #     query_solr_final = Solrindex('http://localhost:8983/solr/ND_preprocessing_pruebas')
            #     result = query_solr_final.search(query)
            #     if len(result.docs) > 0:
            #         doc = result.docs[0]
            #         if 'p96_E67_p4_gave_birth_date_d' in doc:
            #             data['fecha_nacimiento'] = doc['p96_E67_p4_gave_birth_date_d']
            #         if 'p100i_E69_p4_death_date_d' in doc:
            #             data['fecha_fallecimiento'] = doc['p100i_E69_p4_death_date_d']
            #         if 'p96_E67_p7_gave_birth_place_s' in doc:
            #             data['lugar_nacimiento'] = doc['p96_E67_p7_gave_birth_place_s']
            #         if 'p100i_E69_p7_death_place_s' in doc:
            #             data['lugar_fallecimiento'] = doc['p100i_E69_p7_death_place_s']
            #         if 'p3_has_note_s' in doc:
            #             data['description_mp'] = doc['p3_has_note_s']
            #         if 'url_s' in doc:
            #             data['url_s'] = doc['url_s']
            #             hdr = {
            #                 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            #                 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            #                 'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            #                 'Accept-Encoding': 'none',
            #                 'Accept-Language': 'en-US,en;q=0.8',
            #                 'Connection': 'keep-alive'}
            #             req = urllib2.Request(doc['url_s'], headers=hdr)
            #             try:
            #                 page = urllib2.urlopen(req)
            #             except urllib2.HTTPError, e:
            #                 print e.fp.read()
            #             content = page.read()
            #             soup = BeautifulSoup(content, 'html.parser')
            #             if soup.find('meta', attrs={"name": "twitter:image"}):
            #                 image_s = soup.find('meta', attrs={"name": "twitter:image"})
            #                 image_s = image_s['content']
            #                 data['image'] = image_s
            #             else:
            #                 print("NO ENCUENTRO imagen para " + doc['url_s'])
            # update = "UPDATE processed_characters SET fecha_nacimiento=%s,fecha_fallecimiento=%s,lugar_nacimiento=%s,lugar_fallecimiento=%s,description_mp=%s,description_wp=%s,image=%s WHERE id=" + str(id)
            # pprint(update)
            # MysqlND.execute_query(update, (data['fecha_nacimiento'], data['fecha_fallecimiento'], data['lugar_nacimiento'], data['lugar_fallecimiento'], data['description_mp'], data['description_wp'], data['image'],))
            update = "UPDATE processed_characters SET description_wp=%s WHERE id=" + str(id)
            pprint(update)
            MysqlND.execute_query(update, (data['description_wp'],))


    def update_references_data(self):
        query = "SELECT id,url, label, id_entity FROM processed_references WHERE TYPE != 2 AND TYPE != 8 AND TYPE != 6 and id>12887"
        results = MysqlND.execute_query(query, ())
        for res in results:
            data = {}
            id, url, label, id_entity = res[0], res[1], res[2], res[3]
            pprint(id)
            data['id'] = id
            data['label'] = label
            data['url'] = url
            preprocess_information = PreprocessInformation()
            values = preprocess_information.search_label_in_wikidata_and_wikipedia(label)
            if values:
                id_wikipedia_dbp_urls, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label, wikidata_id_entity, wikidata_label = values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]
                data['description'] = ''
                if id_wikipedia_dbp_urls!=-1:
                    query = 'id:' + str(id_wikipedia_dbp_urls)
                    query_solr_final = Solrindex('http://localhost:8983/solr/ND_preprocessing')
                    result = query_solr_final.search(query)
                    if len(result.docs) > 0:
                        doc = result.docs[0]
                        texto = ''
                        for key, value in doc.iteritems():
                            if '_string_txt_es' in key:
                                texto += '<h3 class="wp_title">' + key.replace("_string_txt_es", "").replace("description", "descripción").replace("_", " ").capitalize() + '</h3><p class="wp_paragraph">' + functions.cleanhtml(value) + '</p>'
                        data['description'] = texto
                    update = "UPDATE processed_references SET description=%s WHERE id=" + str(id)
                    MysqlND.execute_query(update, (data['description'],))

    def fill_core_nes(self):
        query_solr_final = Solrindex('http://localhost:8983/solr/TFM')
        array_solr = []

        for id, name in self.artworks_labels.iteritems():
            key = 'ARTW_' + str(id)
            data = {}
            data['id'] = key
            data['labels'] = [name]
            # data['ids_ne'] = [int(id)]
            data['ids_segments'] = []
            data['tipo_ne'] = 'ARTW'

            result = query_solr_final.search('list_artworks_segment:' + str(id))
            if len(result.docs) > 0:
                for d in result.docs:
                    data['ids_segments'].append(d['id'])
            array_solr.append(data)
            print(data['tipo_ne'] + " : " + data['labels'][0])

        query_solr = Solrindex('http://localhost:8983/solr/TFM_index_ne')
        query_solr.add_data(array_solr)


        # for id_ne, label in self.labels_ne_entities.iteritems():
        #     data = {}
        #     data['id'] = id_ne
        #     key = id_ne.split("_")
        #
        #     data['tipo_ne'] = key[0]
        #     id_entity = key[1]
        #     # label_entity = self.get_label_from_entity(id_entity, label[0])
        #     data['labels'] = []
        #     data['labels'].append(label[0])
        #     data['labels'].append(label[2])
        #     if id_ne == 'REF_1229':
        #         print("para")
        #     # data['ids_ne'] = []
        #     data['ids_segments'] = []
            # if data['tipo_ne'] == 'REF':
            #     query = "SELECT id,label FROM processed_references WHERE id_entity="+str(id_entity)
            #     results = MysqlND.execute_query(query, ())
            #     for res in results:
            #         id_ne_, label = res[0], res[1]
            #         # data['ids_ne'].append(id_ne_)
            #         data['labels'].append(label)
            #         result_solr = query_solr_final.search('list_references_segment:' + str(id_ne_))
            #         if len(result_solr.docs) > 0:
            #             for d in result_solr.docs:
            #                 data['ids_segments'].append(d['id'])
            #     array_solr.append(data)
            #     print(data['tipo_ne'] + " : " + data['labels'][0])
            # if data['tipo_ne'] == 'CH':
            #     query = "SELECT id,label FROM processed_characters WHERE id_entity="+str(id_entity)
            #     results = MysqlND.execute_query(query, ())
            #     for res in results:
            #         id_ne_, label = res[0], res[1]
            #         # data['ids_ne'].append(int(id_ne_))
            #         data['labels'].append(label)
            #         result_solr = query_solr_final.search('list_characters_segment:' + str(id_ne_))
            #         if len(result_solr.docs) > 0:
            #             for d in result_solr.docs:
            #                 data['ids_segments'].append(d['id'])
            #     array_solr.append(data)
            #     print(data['tipo_ne'] + " : " + data['labels'][0])


        # query_solr = Solrindex('http://localhost:8983/solr/TFM_index_ne')
        # query_solr.add_data(array_solr)


        # query_solr.delete('*:*')

    def get_label_from_entity(self, id_entity, label):
        query = "SELECT label FROM wikidata_entidades WHERE id=" + str(id_entity)
        results = MysqlND.execute_query(query, ())
        if results.rowcount == 0:
            return label
        else:
            return results.fetchone()[0]