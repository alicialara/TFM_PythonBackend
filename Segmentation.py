# coding=utf-8
from __future__ import print_function

import re
import sys
import urllib2

from bs4 import BeautifulSoup, NavigableString
from stanfordcorenlp import StanfordCoreNLP

import functions
from MysqlND import MysqlND
from PreprocessInformation import PreprocessInformation
from Solrindex import Solrindex
from functions import represents_int, save_solr_registry, save_pickle, get_pickle, exists_pickle, merge_two_dicts

# from BeautifulSoup import BeautifulSoup, NavigableString

reload(sys)
sys.setdefaultencoding('utf8')


class Segmentation(object):

    def __init__(self):
        self.city = 'Madrid'
        self.language = 'es'
        self.preprocess = PreprocessInformation()
        self.jar = 'C:\stanford-postagger-full-2016-10-31\stanford-postagger.jar'
        # model = 'C:\stanford-postagger-full-2016-10-31\models\english-bidirectional-distsim.tagger'
        self.model = 'C:\stanford-postagger-full-2016-10-31\models\spanish-distsim.tagger'
        # remove_data_solr(core_solr='http://localhost:8983/solr/TFM')

    def annotate_next_mp_artwork_data(self):
        query = "SELECT id,id_wikidata,id_wikipedia,id_museodelprado FROM processed_artworks WHERE segmentated=0"
        artworks = MysqlND.execute_query(query, ())
        for artwork in artworks:
            id, id_wikidata, id_wikipedia, id_museodelprado = artwork[0], artwork[1], artwork[2], artwork[3]
            file = 'descriptions_processed_mp/' + str(id) + ".pickle"
            print(file)
            if not exists_pickle(file):
                data_artwork = self.get_solr_artwork_data(id)
                if not data_artwork:
                    print("Pasa de mi")
                else:
                    if 'description_wp' in data_artwork:
                        description_wikipedia = data_artwork['description_wp']
                    else:
                        description_wikipedia = ''

                    if 'description_mp' in data_artwork:
                        description_mp = data_artwork['description_mp']
                    else:
                        description_mp = ''
                    # 1. description_wikipedia -> limpiar la descr de wikipedia
                    description_wikipedia = self.clean_html_wikipedia(description_wikipedia, id)
                    # 2. description_mp -> anotar
                    self.preprocess.id_artwork_processed = id
                    description_mp = self.process_mp_description(description_mp, id_museodelprado=id_museodelprado)
                    data_dict = {"description_wikipedia": description_wikipedia, "description_mp": description_mp}
                    # print(description_mp)
                    save_pickle(file, data_dict)

    def segmentate_next_artwork_data(self):
        query = "SELECT id,id_wikidata,id_wikipedia,id_museodelprado FROM processed_artworks WHERE segmentated=0"
        artworks = MysqlND.execute_query(query, ())
        for artwork in artworks:
            id, id_wikidata, id_wikipedia, id_museodelprado = artwork[0], artwork[1], artwork[2], artwork[3]
            file = 'descriptions_processed_mp/' + str(id) + ".pickle"
            print(file)
            if exists_pickle(file):
                artwork_mp_description = get_pickle(file)
                description_mp = artwork_mp_description['description_mp']
                description_wikipedia = artwork_mp_description['description_wikipedia']
                description_mp_segmentated = self.segmentate(description_mp, id, 'mp')
                description_wikipedia_segmentated = self.segmentate(description_wikipedia, id, 'wp')
                segmentated = merge_two_dicts(description_mp_segmentated, description_wikipedia_segmentated)
                dict_solr = {'id_wikidata': id_wikidata, 'id_wikipedia': id_wikipedia, 'id_museodelprado': id_museodelprado}

                data_artwork = self.get_solr_artwork_data(id)
                dict_solr = self.process_metadata_to_dict(id, dict_solr,data_artwork)

                for key, value in segmentated.iteritems():
                    print(key + ": " + value)
                    dict_solr['id'] = key
                    dict_solr['text'] = value

                    dict_solr['list_artworks_segment'] = []
                    dict_solr['list_references_segment'] = []
                    dict_solr['list_characters_segment'] = []
                    dict_solr['list_events_segment'] = []

                    dict_solr = self.process_text_to_dict_narrative_elements(value, dict_solr)
                    save_solr_registry(dict_solr, core_solr='http://localhost:8983/solr/TFM')
                    MysqlND.execute_query("UPDATE processed_artworks SET segmentated=1 WHERE id=" + str(id), ())

    def process_text_to_dict_narrative_elements(self, text, dict_solr):
        soup = BeautifulSoup(text, 'html.parser')
        for link in soup.find_all('a'):
            id,type_ = self.get_id_from_processed_tag(str(link), withTypes=True)
            if type_ == 7:
                dict_solr['list_references_segment'].append(id)
            elif type_ == 6:
                dict_solr['list_characters_segment'].append(id)
            elif type_ == 2:
                dict_solr['list_artworks_segment'].append(id)
            elif type_ == 8:
                dict_solr['list_events_segment'].append(id)
        return dict_solr

    def process_metadata_to_dict(self, id_artwork, dict_solr,data_artwork):

        dict_solr['list_artworks'] = []
        dict_solr['list_references'] = []
        dict_solr['list_characters'] = []
        dict_solr['list_events'] = []

        dict_solr['name'] = data_artwork['name']
        if 'image' in data_artwork: dict_solr['image'] = data_artwork['image']
        if 'alto' in data_artwork: dict_solr['alto'] = data_artwork['alto']
        if 'ancho' in data_artwork: dict_solr['ancho'] = data_artwork['ancho']
        if 'date_created' in data_artwork: dict_solr['date_created'] = data_artwork['date_created']
        if 'authors' in data_artwork: dict_solr['authors'] = data_artwork['authors']

        # not multivalued
        if 'genero_artistico' in data_artwork: dict_solr['genero_artistico'] = self.get_id_from_processed_tag(data_artwork['genero_artistico'], withTypes=False)
        if 'material_empleado' in data_artwork: dict_solr['material_empleado'] = self.get_id_from_processed_tag(data_artwork['material_empleado'], withTypes=False)
        if 'tecnica' in data_artwork: dict_solr['tecnica'] = self.get_id_from_processed_tag(data_artwork['tecnica'], withTypes=False)
        if 'superficie' in data_artwork: dict_solr['superficie'] = self.get_id_from_processed_tag(data_artwork['superficie'], withTypes=False)
        if 'instancia_de' in data_artwork: dict_solr['instancia_de'] = self.get_id_from_processed_tag(data_artwork['instancia_de'], withTypes=False)

        if 'coleccion' in data_artwork: dict_solr['coleccion'] = self.get_id_from_processed_tag(data_artwork['coleccion'], withTypes=False)
        if 'ubicacion' in data_artwork: dict_solr['ubicacion'] = self.get_id_from_processed_tag(data_artwork['ubicacion'], withTypes=False)
        if 'estilo' in data_artwork: dict_solr['estilo'] = self.get_id_from_processed_tag(data_artwork['estilo'], withTypes=False)
        if 'pais_de_origen' in data_artwork: dict_solr['pais_de_origen'] = self.get_id_from_processed_tag(data_artwork['pais_de_origen'], withTypes=False)
        if 'pais' in data_artwork: dict_solr['pais'] = self.get_id_from_processed_tag(data_artwork['pais'], withTypes=False)

        # multivalued
        if 'procedencia' in data_artwork: dict_solr['procedencia'] = self.get_id_from_processed_tag(data_artwork['procedencia'], withTypes=False)
        if 'movimiento' in data_artwork: dict_solr['movimiento'] = self.get_id_from_processed_tag(data_artwork['movimiento'], withTypes=False)
        if 'representa_a' in data_artwork: dict_solr['representa_a'] = self.get_id_from_processed_tag(data_artwork['representa_a'], withTypes=False)

        keywords_wp = []
        if 'keywords_wp' in data_artwork: keywords_wp = self.get_id_from_processed_tag(data_artwork['keywords_wp'], withTypes=True)
        for kw in keywords_wp:
            id = kw[0]
            type_ = kw[1]
            if type_ == 7:
                dict_solr['list_references'].append(id)
            elif type_ == 6:
                dict_solr['list_characters'].append(id)
            elif type_ == 2:
                dict_solr['list_artworks'].append(id)
            elif type_ == 8:
                dict_solr['list_events'].append(id)

        keywords_mp = []
        if 'keywords_mp' in data_artwork: keywords_mp = self.get_id_from_processed_tag(data_artwork['keywords_mp'], withTypes=True)
        for kw in keywords_mp:
            id = kw[0]
            type_ = kw[1]
            if type_ == 7:
                dict_solr['list_references'].append(id)
            elif type_ == 6:
                dict_solr['list_characters'].append(id)
            elif type_ == 2:
                dict_solr['list_artworks'].append(id)
            elif type_ == 8:
                dict_solr['list_events'].append(id)
        return dict_solr


    def segmentate(self, text, id_artwork, type):
        soup = BeautifulSoup(text, 'html.parser')
        i = 0
        data = {}
        for paragraph in soup.find_all('p'):
            p = str(paragraph)
            key = str(id_artwork) + "_" + type + "_" + str(i)
            data[key] = p
            i += 1
        return data

    def save_list_of_narrative_elements(self):
        array_datos = []
        query = "SELECT id,id_entity,label FROM processed_characters"
        results = MysqlND.execute_query(query, ())
        for result in results:
            id, id_entity, label = result[0], result[1], result[2]
            array_datos.append([label, id, id_entity, 'processed_characters'])

        query = "SELECT id,id_entity,label FROM wikidata_entidades"
        results = MysqlND.execute_query(query, ())
        for result in results:
            id, id_entity, label = result[0], result[1], result[2]
            array_datos.append([label, id, id_entity, 'wikidata_entidades'])

        query = "SELECT id,id_entity,label FROM wikidata_instancias_de"
        results = MysqlND.execute_query(query, ())
        for result in results:
            id, id_entity, label = result[0], result[1], result[2]
            array_datos.append([label, id, id_entity, 'wikidata_instancias_de'])

        query = "SELECT id,id_entity,label FROM processed_references"
        results = MysqlND.execute_query(query, ())
        for result in results:
            id, id_entity, label = result[0], result[1], result[2]
            array_datos.append([label, id, id_entity, 'processed_references'])

        save_pickle('narrative_elements', array_datos)

    def get_list_of_narrative_elements(self):
        return get_pickle('narrative_elements')

    def process_mp_description(self, description, id_museodelprado=-1):
        array_elements_processed = []
        # 1. get all tagged keywords

        soup = BeautifulSoup(description, 'html.parser')
        paragraphs = soup.find("p")
        if paragraphs == None:
            # get MP description...
            soup = BeautifulSoup(self.get_mp_description_from_id(id_museodelprado), 'html.parser')
            description = str(soup)

        for tag in soup.find_all('em'):
            tag_name = tag.getText()
            id, type = self.preprocess.create_reference(tag_name, is_string=True, createIfNotExistsWikidataEntity=False)
            tag.name = 'a'
            tag.attrs = {}
            if id != -1:
                array_elements_processed.append(tag_name)
                if type == 2:
                    tag["id_artwork"] = str(id)
                elif type == 3 or type == 6:
                    tag["id_character"] = str(id)
                elif type == 8:
                    tag["id_event"] = str(id)
                else:
                    tag["id_reference"] = str(id)
            else:
                tag.name = 'em'
        description = str(soup)

        # description_str = soup.getText()
        description_str = description
        # 2. find concepts by list of narrative elements
        narrative_elements = self.get_list_of_narrative_elements()
        array_ne = [x for x in narrative_elements if x[0] in description_str and len(x[0]) > 3 and not represents_int(x[0])]
        # pprint(array_ne)

        for label, id, id_entity, table in array_ne:
            if label not in array_elements_processed:
                # print(label)
                if table == 'processed_characters':
                    insensitive_hippo = re.compile(re.escape(" " + label + " "), re.IGNORECASE)
                    description_str = insensitive_hippo.sub(' <a id_character="' + str(id) + '">' + str(label) + '</a> ', description_str)
                elif table == 'wikidata_entidades':
                    data = self.get_id_and_table_from_id_entity(id, id_entity, label)
                    if data:
                        id, label_, type_table = data[0], data[1], data[2]
                        insensitive_hippo = re.compile(re.escape(" " + label + " "), re.IGNORECASE)
                        description_str = insensitive_hippo.sub(' <a ' + type_table + '="' + str(id) + '">' + str(label) + '</a> ', description_str)
                elif table == 'wikidata_instancias_de':
                    data = self.get_id_and_table_from_id_entity(id, id_entity, label)
                    if data:
                        id, label_, type_table = data[0], data[1], data[2]
                        insensitive_hippo = re.compile(re.escape(" " + label + " "), re.IGNORECASE)
                        description_str = insensitive_hippo.sub(' <a ' + type_table + '="' + str(id) + '">' + str(label) + '</a> ', description_str)
                elif table == 'processed_references':
                    insensitive_hippo = re.compile(re.escape(" " + label + " "), re.IGNORECASE)
                    description_str = insensitive_hippo.sub(' <a id_reference="' + str(id) + '">' + str(label) + '</a> ', description_str)
            array_elements_processed.append(label)

        # 3. Stanford POS tagger

        nlp = StanfordCoreNLP(r'C:\stanford-corenlp-full-2018-02-27', lang='es')
        ner = nlp.ner(description_str)
        nlp.close()
        last_word = ''
        array_processed = []
        i = 0
        for c in ner:
            word = c[0]
            type = c[1]
            if len(ner) == i + 1:
                next_type = ''
            else:
                next_type = ner[i + 1][1]
            last_word = last_word + " " + word
            if next_type != type:
                data = [last_word, type]
                array_processed.append(data)
                last_word = ''
            i = i + 1

        array_ner = [x for x in array_processed if x[1] != 'O']
        array_ner = [x for x in array_ner if x[0] not in array_elements_processed]
        # pprint(array_ner)
        people = [x for x in array_ner if x[1] == 'PERSON']
        numbers = [x for x in array_ner if x[1] == 'NUMBER']
        dates = [x for x in array_ner if x[1] == 'DATE']
        references = [x for x in array_ner if x[1] != 'DATE' and x[1] != 'NUMBER' and x[1] != 'PERSON']

        people = self.remove_redundant_ner_elements(people)
        numbers = self.remove_redundant_ner_elements(numbers)
        dates = self.remove_redundant_ner_elements(dates)
        references = self.remove_redundant_ner_elements(references)

        for person in people:
            person_name = person[0].strip()
            data = self.preprocess.exists_already_label(person_name)
            if data:
                id, type = data[0], data[1]
            else:
                data = self.preprocess.search_label_in_wikipedia(person_name)
                if data:
                    label, type, url, id_entity, id_instancia_de, id_ = data[0], data[1], data[2], data[3], data[4], \
                                                                        data[5]
                    if id_instancia_de == '812':
                        id = self.preprocess.add_character(type, label, -1, url, id_entity, id_instancia_de)
                    else:
                        id = -1
            if data and id != -1:
                if type == 3 or type == 6:
                    description_str = description_str.replace(" " + person_name + " ", ' <a id_character="' + str(id) + '">' + str(
                        person_name) + '</a> ')

        for w in numbers:
            name = w[0].strip()
            if len(name) == 4 and represents_int(name):
                data = self.preprocess.create_reference(name, is_string=True, createIfNotExistsWikidataEntity=False)
                if data:
                    id, type = data[0], data[1]
                    if type == 8 and id != -1:
                        description_str = description_str.replace(" " + name + " ",
                                                                  ' <a id_event="' + str(id) + '">' + name + '</a> ')

        for w in dates:
            name = w[0].strip()
            if len(name) == 4 and represents_int(name):
                data = self.preprocess.create_reference(name, is_string=True, createIfNotExistsWikidataEntity=False)
                if data:
                    id, type = data[0], data[1]
                    if type == 8 and id != -1:
                        description_str = description_str.replace(" " + name + " ", ' <a id_event="' + id + '">' + name + '</a> ')

        for w in references:
            name = w[0].strip()
            if len(name) > 3:
                data = self.preprocess.exists_already_label(name)
                if data:
                    id, type = data[0], data[1]
                else:
                    data = self.preprocess.search_label_in_wikipedia(name)
                    if data:
                        label, type, url, id_entity, id_instancia_de, id_ = data[0], data[1], data[2], data[3], data[4], \
                                                                            data[5]
                        id = self.preprocess.add_reference(type, label, url, id_entity, id_instancia_de)
                if data:
                    if type == 7:
                        description_str = description_str.replace(" " + name + " ", ' <a id_reference="' + str(id) + '">' + str(
                            name) + '</a> ')

        return description_str

    def get_mp_description_from_id(self, id_museodelprado):
        query = "SELECT url FROM dbp_urls WHERE id=" + str(id_museodelprado)
        results = MysqlND.execute_query(query, ())
        url_museodelprado = results.fetchone()[0]

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
            print(e.fp.read())
        content = page.read()
        soup = BeautifulSoup(content, 'html.parser')
        description = ''
        if soup.find('div', attrs={"class": "obra"}).find("div", attrs={"property": "cidoc:p3_has_note"}):
            description = soup.find('div', attrs={"class": "obra"}).find("div", attrs={"property": "cidoc:p3_has_note"})
            description = ' '.join([str(e) for e in description.contents])
        return str(description)

    def get_id_and_table_from_id_entity(self, id, id_entity, label):
        query = "SELECT id,label FROM processed_characters WHERE id_entity=%s LIMIT 1"
        results = MysqlND.execute_query(query, (id,))
        if results.rowcount > 0:
            result = results.fetchone()
            id, label = result[0], result[1]
            return id, label, 'id_character'

        query = "SELECT id FROM dbp_urls WHERE url = %s LIMIT 1"
        results = MysqlND.execute_query(query, ('http://www.wikidata.org/entity/' + id_entity,))
        if results.rowcount > 0:
            result = results.fetchone()
            id_dbp = result[0]
            query = "SELECT id FROM processed_artworks WHERE id_wikidata = %s LIMIT 1"
            results = MysqlND.execute_query(query, (id_dbp,))
            if results.rowcount > 0:
                result = results.fetchone()
                id = result[0]
                return id, label, 'id_artwork'

        query = "SELECT id,label FROM processed_references WHERE id_entity=%s LIMIT 1"
        results = MysqlND.execute_query(query, (id,))
        if results.rowcount > 0:
            result = results.fetchone()
            id, label = result[0], result[1]
            return id, label, 'id_reference'
        return False

    def remove_redundant_ner_elements(self, list_):
        newlist = []
        for i in list_:
            value = i[0].strip()
            if len(value) > 2:
                esta = False
                for j in newlist:
                    if value in j:
                        esta = True
                        break
                if not esta:
                    newlist.append([i[0].strip(), i[1]])

        return newlist

    def clean_html_wikipedia(self, description_wikipedia, id_artwork):
        soup = BeautifulSoup(description_wikipedia, 'html.parser')
        if soup.find('a'):
            for link in soup.find_all('a'):
                # print(str(link))
                type = link['type']
                if type == '7':
                    wikidata_label = link['title']
                    query = "SELECT id FROM processed_references WHERE label=%s LIMIT 1"
                    reference = MysqlND.execute_query(query, (wikidata_label,))
                    if reference.rowcount > 0:
                        reference = reference.fetchone()
                        id = reference[0]
                        link.attrs = {}
                        link['id_reference'] = str(id)
                    else:
                        # create reference
                        query = "SELECT id, id_entity,id_instancia_de FROM wikidata_entidades WHERE label=%s"
                        entidad = MysqlND.execute_query(query, (wikidata_label,))
                        if entidad.rowcount > 0:
                            entidad = entidad.fetchone()
                            id, id_entity, id_entity_instancia_de = entidad[0], entidad[1], entidad[2]
                            insert = "INSERT INTO processed_references(label, url, id_entity, id_entity_instancia_de) VALUES (%s,%s,%s,%s)"
                            array = (wikidata_label, '', id_entity, id_entity_instancia_de)
                            result_insert = MysqlND.execute_query(insert, array)
                            id_reference = result_insert.lastrowid
                            link.attrs = {}
                            link['id_reference'] = str(id_reference)
                        else:
                            if 'instancia_de' in link.attrs:
                                entity_instancia_de = link['instancia_de']
                                if entity_instancia_de != '-1' and entity_instancia_de != '':
                                    label_instancia_de = link['instancia_de_title']
                                    id_instancia_de = self.add_wikidata_instancia_de(entity_instancia_de,
                                                                                     label_instancia_de)
                                else:
                                    id_instancia_de = -1
                                entity = link['wikidata_id']
                                if entity != '-1' and entity != '':
                                    id_entity = self.add_wikidata_id(entity, wikidata_label, id_instancia_de)
                                else:
                                    id_entity = -1
                                insert = "INSERT INTO processed_references(label, url, id_entity, id_entity_instancia_de) VALUES (%s,%s,%s,%s)"
                                array = (wikidata_label, '', id_entity, id_instancia_de)
                                result_insert = MysqlND.execute_query(insert, array)
                                id_reference = result_insert.lastrowid
                                link.attrs = {}
                                link['id_reference'] = str(id_reference)
                            else:
                                print("PARA E IMPLEMENTA")
                elif type == '8':
                    year = link['title']
                    query = "SELECT id FROM processed_events WHERE year=%s AND id_artwork_related=%s"
                    event = MysqlND.execute_query(query, (year, id_artwork,))
                    if event.rowcount > 0:
                        event = event.fetchone()
                        id = event[0]
                        link.attrs = {}
                        link['id_event'] = str(id)
                    else:
                        # create event
                        query_insert = "INSERT INTO processed_events(year,label,event_type,id_artwork_related) VALUES(%s,%s,%s,%s)"
                        insert = MysqlND.execute_query(query_insert,
                                                       (year, 'Relacionado con obra', 'related_to', id_artwork))
                        link.attrs = {}
                        link['id_event'] = str(insert.lastrowid)
                elif type == '6':
                    wikidata_id = str(link['wikidata_id'])
                    id_dbp_urls_wikipedia = link['href']
                    label = link['title']
                    if wikidata_id != '-1' and wikidata_id != '' and wikidata_id != -1:
                        query = "SELECT processed_characters.* FROM processed_characters " \
                                "INNER JOIN wikidata_entidades ON wikidata_entidades.id = processed_characters.id_entity " \
                                "WHERE wikidata_entidades.id_entity=%s "
                        character = MysqlND.execute_query(query, (wikidata_id,))
                        if character.rowcount > 0:
                            character = character.fetchone()
                            id = character[0]
                            link.attrs = {}
                            link['id_character'] = str(id)
                        else:
                            id_entity = self.add_wikidata_id(wikidata_id, label, '812')
                            insert = "INSERT INTO processed_characters(label,id_dbp_urls_wikipedia, url_dbp_urls_wikipedia, " \
                                     "url_dbp_urls_museodelprado,id_dbp_urls_museodelprado, id_entity, id_entity_instancia_de) " \
                                     "VALUES (%s,%s, %s,%s,%s,%s,%s)"

                            character = MysqlND.execute_query(insert, (
                                label, id_dbp_urls_wikipedia, '', '', -1, id_entity, '812'))
                            link.attrs = {}
                            link['id_character'] = str(character.lastrowid)
                    else:
                        print("PARA E IMPLEMENTA")
                else:
                    print("PaRA E IMPLEMENTA CON TIPO :" + str(type))
        return str(self.remove_trash(soup))

    def strip_tags(self, soup, valid_tags=['p', 'a', 'em']):
        for tag in soup.findAll(True):
            if tag.name not in valid_tags:
                s = ""
                for c in tag.contents:
                    if not isinstance(c, NavigableString):
                        c = self.strip_tags(unicode(c), valid_tags)
                    s += unicode(c)

                tag.replaceWith(s)

        return soup

    def remove_trash(self, soup, valid_tags=['p', 'a', 'em']):
        for tag in soup.findAll(True):
            if tag.name not in valid_tags:
                tag.replaceWith('')
        return soup

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

    def segmentate_text(self, text):
        array = []
        soup = BeautifulSoup(text, 'html.parser')
        if soup.find('p'):
            i = 1
            for paragraph in soup.find_all('p'):
                array_solr = {}
                # classification_label = key.replace("_html", "")
                # id_classification = self.get_classification_id(classification_label)
                # id_paragraph = id_entity + "_C" + str(id_classification) + "_" + str(i)
                #
                # array_solr['id'] = id_paragraph
                # array_solr['id_classification'] = id_classification
                # array_solr['id_entity'] = id_entity
                # array_solr['number_paragraph'] = i
                # array_solr['id_entity_city'] = id_entity_city
                # array_solr['name_monument'] = label
                # array_solr['image'] = image
                # array_solr['sitelink'] = sitelink
                # array_solr['code_bin'] = code_bin
                # array_solr['id_geonames'] = id_geonames
                # array_solr['coordinates'] = coordinates
                #
                # text_paragraph = str(paragraph)
                #
                # array_solr['text_paragraph'] = text_paragraph
                # # print(text_paragraph)
                #
                # id_references = self.extract_elements_from_paragraph(text_paragraph, 'id_reference')
                # id_characters = self.extract_elements_from_paragraph(text_paragraph, 'id_character')
                # id_events = self.extract_elements_from_paragraph(text_paragraph, 'id_event')
                #
                # array_solr['id_references'] = id_references
                # array_solr['id_characters'] = id_characters
                # array_solr['id_events'] = id_events

                i += 1
                array.append(array_solr)
        save_solr_registry(array)

    def get_solr_artwork_data(self, id):
        query_solr_final = Solrindex('http://localhost:8983/solr/ND_final')
        result = query_solr_final.search('id:' + str(id))
        if len(result.docs) > 0:
            return result.docs[0]
        return False

    @staticmethod
    def set_time_saved_solr_paragraph(id_entity_monument):
        """

        :param id_entity_monument:
        """
        query = "UPDATE monuments SET date_segmentation = CURRENT_TIMESTAMP WHERE id_entity = %s"
        MysqlND.execute_query_tourism(query, (id_entity_monument,))

    @staticmethod
    def extract_elements_from_paragraph(p, type_element):
        """

        :param p:
        :param type_element:
        :return:
        """
        results = []
        soup = BeautifulSoup(p, 'html.parser')
        if soup.find('a'):
            for link in soup.find_all('a'):
                if type_element in link.attrs:
                    results.append(link[type_element])
        return results

    @staticmethod
    def get_id_from_processed_tag(tag, withTypes=True):
        """

        :param tag:
        :param withTypes:
        :return:
        """
        if not isinstance(tag, list):
            tag_ = [tag]
        else:
            tag_ = tag
        results = []
        for t in tag_:
            soup = BeautifulSoup(t, 'html.parser')
            link = soup.find('a')
            if link is None:
                if not isinstance(tag, list):
                    if withTypes:
                        return False, 0
                    else:
                        return False
            else:
                if 'id_reference' in link.attrs:
                    type = 7
                    id = link['id_reference']
                elif 'id_character' in link.attrs:
                    type = 6
                    id = link['id_character']
                elif 'id_artwork' in link.attrs:
                    type = 2
                    id = link['id_artwork']
                elif 'id_event' in link.attrs:
                    type = 8
                    id = link['id_event']
                else:
                    if not isinstance(tag, list):
                        if withTypes:
                            return False, 0
                        else:
                            return False
                if not isinstance(tag, list):
                    if withTypes:
                        return id, type
                    else:
                        return id
                else:
                    if withTypes:
                        results.append([id, type])
                    else:
                        results.append(id)
        return results

    @staticmethod
    def define_classification_types():
        """

        """
        query = "SELECT text,COUNT(text) AS cuenta FROM log GROUP BY text ORDER BY cuenta DESC"
        results = MysqlND.execute_query_tourism(query, ())
        for res in results:
            text = res[0]
            cuenta = res[1]
            text_pretty = text.replace("_", " ").capitalize()
            text = text.replace("ñ", 'n').strip()
            query = "INSERT INTO classification_segments(label,label_pretty,count) VALUES(%s,%s,%s)"
            MysqlND.execute_query_tourism(query, (text, text_pretty, cuenta,))

    def get_classification_id(self, label):
        # print(label)
        if label == 'description':
            return 0
        label = label.replace("-", "–")
        label = label.replace("–", "-")
        query = "SELECT id FROM classification_segments WHERE label = %s LIMIT 1"

        result = MysqlND.execute_query_tourism(query, (label,)).fetchone()
        return result[0]

    def get_city(self):
        return self.city

    def set_city(self, city):
        self.city = city

    def get_language(self):
        return self.language

    def set_language(self, language):
        self.language = language

    def count_times_characters_appears(self):
        query = "SELECT id FROM processed_characters WHERE id = 100"
        results = MysqlND.execute_query_tourism(query, ())
        array = {}
        array_grandes = {}
        for result in results:
            id = result[0]
            query_solr = Solrindex('http://localhost:8983/solr/Tourism')
            results_solr = query_solr.search('id_characters:' + str(id))
            documents = results_solr.docs
            count = len(documents)
            if count > 10:
                array_grandes[id] = count
            array[id] = [count]
            if count > 0:
                extra_data = {}
                docus = {}
                for doc in documents:
                    id_entity = doc['id_entity']
                    if id_entity not in docus:
                        docus[id_entity] = 1
                    else:
                        docus[id_entity] += 1
                total_monuments = len(docus)
                extra_data['monuments'] = docus
                extra_data['total_monuments'] = total_monuments
                array[id].append(extra_data)
        # pprint(array_grandes)

    def get_solr_segments_data(self, query, multiple=False):
        query_solr_final = Solrindex('http://localhost:8983/solr/TFM')
        if functions.represents_int(query):
            query = 'id:' + str(query)
        result = query_solr_final.search(query)
        if len(result.docs) > 0:
            if multiple:
                return result.docs
            else:
                return result.docs[0]
        return False
    def delete_id_solr(self, id):
        query_solr_final = Solrindex('http://localhost:8983/solr/TFM')
        query_solr_final.delete('id:' + id)


    def clean_final_data(self):
        segments = self.get_solr_segments_data("*:*", multiple=True)
        for sg in segments:
            text = sg['text']
            text = re.sub(r'\([^)]*\)', '', text)
            if len(text) < 30:
                self.delete_id_solr(sg['id'])
        # soup = BeautifulSoup(self.get_mp_description_from_id(id_museodelprado), 'html.parser')
        # description = str(soup)