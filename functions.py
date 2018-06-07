# coding=utf-8
import datetime
import json
import os
import pickle
import re
import urllib
import urllib2
from math import radians, cos, sin, asin, sqrt
from os import remove, close
from shutil import move
from tempfile import mkstemp

import numpy as np
import requests
import unidecode
from SPARQLWrapper import SPARQLWrapper, JSON
from bs4 import BeautifulSoup

from InformationExtractor import InformationExtractor
from MysqlND import MysqlND
from Solrindex import Solrindex


def get_dbp_url_info(id_dbp_urls, add_solr_info=True):
    query = "SELECT id,url,type FROM dbp_urls WHERE id =" + str(id_dbp_urls)
    results = MysqlND.execute_query(query, ())
    if results.rowcount > 0:
        result = results.fetchone()
        id, url, type = result[0], result[1], result[2]
        data = {
            'id': id,
            'url': url,
            'type': type
        }
        if add_solr_info:
            query_solr = Solrindex()
            results_solr = query_solr.search('id:' + str(id))
            if len(results_solr) == 1:
                results_solr_ = results_solr.docs

                data.update(results_solr_[0])
                data.update({'type': type})
        return data
    else:
        return False


def represents_int(s):
    if s is None:
        return False
    try:
        int(s)
        return True
    except ValueError:
        return False


def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext


def get_id_entity_wikidata_from_id_entidad(id_wikidata_entidades):
    query = "SELECT id_entity, label, id_instancia_de FROM wikidata_entidades WHERE id =" + str(id_wikidata_entidades)
    results = MysqlND.execute_query(query, ())
    if results.rowcount > 0:
        result = results.fetchone()
        return result[0], result[1], result[2]
    else:
        return -1, '', -1


def get_id_entity_wikidata_from_id_artwork(id_artwork):
    query = "SELECT id_wikidata FROM processed_artworks WHERE id =" + str(id_artwork)
    results = MysqlND.execute_query(query, ())
    if results.rowcount > 0:
        result = results.fetchone()
        id_dbp_urls_artwork = result[0]
        query = "SELECT url FROM dbp_urls WHERE id =" + str(id_dbp_urls_artwork)
        results = MysqlND.execute_query(query, ())
        if results.rowcount > 0:
            result = results.fetchone()
            url = result[0]
            id_wikidata = url.replace("http://www.wikidata.org/entity/", "")
            query = "SELECT id_entity, label, id_instancia_de FROM wikidata_entidades WHERE id_entity = '" + str(id_wikidata) + "'"
            results = MysqlND.execute_query(query, ())
            if results.rowcount > 0:
                result = results.fetchone()
                return result[0], result[1], result[2]
            else:
                wikidata_url = "http://www.wikidata.org/entity/" + str(id_wikidata)
                id_wikipedia_, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label = extract_wikipedia_url(id_wikidata, wikidata_url)

                id_instancia_de = add_wikidata_instancia_de(wikidata_instancia_de_id_entity, wikidata_instancia_de_label)
                id_entity = add_wikidata_id(id_wikidata, wikidata_label, id_instancia_de)

                return id_entity, wikidata_label, id_instancia_de
        else:
            return -1, '', -1
    else:
        return -1, '', -1


def get_id_entity_wikidata_from_id_character(id_character):
    query = "SELECT id_entity, id_entity_instancia_de FROM processed_characters WHERE id =" + str(id_character)
    results = MysqlND.execute_query(query, ())
    if results.rowcount > 0:
        result = results.fetchone()
        id_entity = result[0]
        return get_id_entity_wikidata_from_id_entidad(id_entity)
    else:
        return -1, '', -1


def get_id_event_associated_with_element(year, id_reference, type):
    if type == 3 or type == 6:
        column = 'id_character_related'
    elif type == 2:
        column = 'id_artwork_related'
    else:
        column = 'id_reference_related'
    query = "SELECT id FROM processed_events WHERE year =" + str(year) + " AND " + str(column) + " = " + str(id_reference)
    results = MysqlND.execute_query(query, ())
    if results.rowcount > 0:
        result = results.fetchone()
        return result[0]
    else:
        return -1


def extract_wikipedia_url(wikidata_id_entity, url_wikidata):
    information_extraction = InformationExtractor()
    type = 7
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
            if represents_int(wikidata_label):
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
                    id_wikipedia = add_url_to_database(type, wikipedia_url)
                    information_extraction.extract_general_info_from_wikipedia_to_solr(type, id_wikipedia)
                else:
                    id_wikipedia = -1
            except requests.ConnectionError:
                print("Failed to connect: " + wikipedia_url)
            # print wikipedia_title

    return id_wikipedia, id_url_wikipedia_author, type, wikidata_label, wikidata_instancia_de_id_entity, wikidata_instancia_de_label


def add_url_to_database(type, url):
    # add url to database
    q = "SELECT count(*) as cuenta,id FROM dbp_urls WHERE TYPE=" + str(type) + " AND url LIKE %s"
    if type == 4 or type == 5 or type == 6 or type == 7 or type == 8:
        if 'https://es.wikipedia.org' not in url:
            url = 'https://es.wikipedia.org' + url
    url = url.split("#", 1)[0]
    array_q = (url,)
    cuenta = MysqlND.execute_query(q, array_q).fetchone()
    if cuenta[0] == 0:
        query = "INSERT INTO dbp_urls(type, url) VALUES(" + str(type) + ", %s)"
        result_q = MysqlND.execute_query(query, array_q)
        return result_q.lastrowid
    else:
        return cuenta[1]


def extract_general_info_from_wikipedia_to_solr(type, id_url_database):
    query_search_url = "SELECT id,url,extracted_wikipedia_info FROM dbp_urls WHERE id=" + str(id_url_database)
    results = MysqlND.execute_query(query_search_url, ())
    array_solr = {}
    for id_wikipedia, url_wikipedia, extracted_wikipedia_info in results:
        if extracted_wikipedia_info == 0:
            print(str(id_wikipedia) + " - " + url_wikipedia)
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
            save_solr_registry(array_solr)  # [{'id': 93L}, {u'numero_de_inventario_s': u'P02824'}, {u'a
            set_url_visited(id_wikipedia, 'extracted_wikipedia_info')


def save_solr_registry(data, core_solr='http://localhost:8983/solr/ND_preprocessing'):
    query_solr = Solrindex(core_solr)
    return query_solr.add_data(data)


def remove_data_solr(query='*:*', core_solr='http://localhost:8983/solr/TFM'):
    query_solr = Solrindex(core_solr)
    query_solr.delete(query)


def set_url_visited(id, type):
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


def add_wikidata_instancia_de(wikidata_instancia_de_id_entity, wikidata_instancia_de_label):
    query = "SELECT id FROM wikidata_instancias_de WHERE id_entity = '" + str(wikidata_instancia_de_id_entity) + "'"
    results = MysqlND.execute_query(query, ())
    if results.rowcount == 0:
        insert = "INSERT INTO wikidata_instancias_de(id_entity, label) VALUES (%s,%s)"
        array = (wikidata_instancia_de_id_entity, wikidata_instancia_de_label,)
        result_insert = MysqlND.execute_query(insert, array)
        return result_insert.lastrowid
    else:
        res = results.fetchone()
        return res[0]


def add_wikidata_id(wikidata_id_entity, wikidata_label, id_instancia_de):
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


# https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers is 6371
    km = 6371 * c
    return km


def save_pickle(file_, array):
    """

    :param file_:
    :param array:
    """
    with open(file_, 'w') as f:
        pickle.dump(array, f)


def get_pickle(file_):
    """

    :param file_:
    :return:
    """
    # Recupero de 'objs.pickle' el contenido de las variables
    with open(file_) as f:
        return pickle.load(f)


def exists_pickle(file_):
    return os.path.isfile(file_)


def read_dir(path):
    print "Voy a leer todos los nombres de archivos dentro de la ruta --> " + path
    for dirname, dirnames, filenames in os.walk(path):
        for subdirname in dirnames:
            print(os.path.join(dirname, subdirname))
        print "path to all filenames."
        list = []
        for filename in filenames:
            list.append(os.path.join(filename))
        return filenames
    # for file in os.listdir(path):
    #     # if file.endswith(".txt"):
    #     print(os.path.join(path, file))


def replace(file_path, pattern, subst):
    # Create temp file
    fh, abs_path = mkstemp()
    with open(abs_path, 'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                new_file.write(line.replace(pattern, subst))
    close(fh)
    # Remove original file
    remove(file_path)
    # Move new file
    move(abs_path, file_path)


def create_dir_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def rename_extension_file(dir, old_pattern, new_pattern):
    for filename in os.listdir(dir):
        filename_ = filename.replace(old_pattern, new_pattern)
        os.rename(filename, filename_)


def get_timestamp(date_str):
    if date_str == '':
        return ''
    if '.' in date_str:
        return ''
    date_str = date_str.replace("Gregorian", "").replace("Julian", "")
    if 'BCE' in date_str:
        date_str = "-" + date_str.replace("BCE", "")
    # Probably not necessary
    date_str = str(date_str)
    date_str = date_str.strip()
    # Remove + sign
    if date_str[0] == '+':
        date_str = date_str[1:]
    if date_str[0] == '-':
        date_str = date_str[1:]
    # Remove missing month/day
    date_str = date_str.split('-00')
    if len(date_str) > 1:
        date_str = date_str[0]
    else:
        year = date_str[0].split("-")
        if len(year[0]) == 3:
            date_str[0] = '0' + date_str[0]

    date_str_splitted = date_str[0].split(" ")
    if len(date_str_splitted) == 3:
        if represents_int(date_str_splitted[0]) and not represents_int(date_str_splitted[1]) and represents_int(date_str_splitted[2]):
            if len(date_str_splitted[2]) == 2:
                date_str_splitted[2] = '0' + date_str_splitted[2]
            if len(date_str_splitted[2]) == 3:
                date_str_splitted[2] = '0' + date_str_splitted[2]
            date_mod = datetime.datetime.strptime(' '.join(date_str_splitted), "%d %B %Y").isoformat()
            date_str[0] = date_mod
    elif len(date_str_splitted) == 2:
        if not represents_int(date_str_splitted[0]) and represents_int(date_str_splitted[1]):
            date_mod = datetime.datetime.strptime(date_str[0], "%B %Y").isoformat()
            date_str[0] = date_mod
        elif not represents_int(date_str_splitted[0]) and not represents_int(date_str_splitted[1]):
            return ''
    elif len(date_str_splitted) == 1:
        if represents_int(date_str_splitted[0]):
            if int(date_str[0]) > 100:
                date_mod = datetime.datetime.strptime(date_str[0], "%Y").isoformat()
                date_str[0] = date_mod
            else:
                return ''
        else:
            return ''

    # Parse date

    dt = np.datetime64(date_str[0])
    # As Unix timestamp (choose preferred datetype)
    time_ = dt.astype('<M8[s]').astype(np.int64)

    # date('Y-m-d H:i:s', '1299762201428')

    dt = str(dt).replace("T", " ")
    # % b % d % Y % H: % M

    a = datetime.datetime.strptime(dt, "%Y-%m-%d %X").isoformat()

    # iso = utctime.isoformat()
    # tokens = iso.strip().split("T")
    #
    # ts = time.time()
    # timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    a = str(a).replace("T", " ")
    return a


def get_year(datetime):
    datetime = datetime.split("-")
    return datetime[0]


def merge_two_dicts(x, y):
    z = x.copy()  # start with x's keys and values
    z.update(y)  # modifies z with y's keys and values & returns None
    return z


def clean_labels_for_graph(label):
    # return "prueba"
    if label == None or len(label) == 0:
        label = "no_label"
    label_unicode = label.decode()
    label_unicode = label_unicode.replace(",", "").replace(".", "").replace("-", "_").strip()[0:20]
    unnacented = unidecode.unidecode(label_unicode)
    lower = unnacented.lower()
    label = str(lower).replace(" ", "_")
    label = ''.join(e for e in label if e.isalnum())
    return label


# .replace("(","_").replace(")","_").replace("[","_").replace("]","_").replace("/","_").replace("?","_").replace("¿","_")

def replace(file_path, pattern, subst):
    # Create temp file
    fh, abs_path = mkstemp()
    with open(abs_path, 'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                new_file.write(line.replace(pattern, subst))
    os.close(fh)
    # Remove original file
    os.remove(file_path)
    # Move new file
    move(abs_path, file_path)


def replace_if_type_node(file_path, pattern, subst, color_before, color_after):
    # Create temp file
    fh, abs_path = mkstemp()
    with open(abs_path, 'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                new_line = line.replace(pattern, subst)
                if pattern in line:
                    new_line = new_line.replace(color_before, color_after)
                new_file.write(new_line)
    os.close(fh)
    # Remove original file
    os.remove(file_path)
    # Move new file
    move(abs_path, file_path)


def save_text_to_file(text, file_):
    """

    :param text:
    :param file_:
    """
    with open(file_, "a") as text_file:
        text_file.write("%s \n" % text)


