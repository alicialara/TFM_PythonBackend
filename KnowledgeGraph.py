# coding=utf-8
import operator
import random
from heapq import heappush
from pprint import pprint

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
from bs4 import BeautifulSoup

import functions
from MysqlND import MysqlND
from Solrindex import Solrindex
from functions import clean_labels_for_graph


class KnowledgeGraph(object):

    def __init__(self, directed=True, type='seg_ne', clean_cache_elements=False, clean_cache_graph=False):
        self.type = type
        self.segments_per_itinerarie = 10
        if directed:
            self.graph = nx.DiGraph()
            self.type += '_directed'
        else:
            self.graph = nx.Graph()
            self.type += '_notdirected'
        self.clean_cache_elements = clean_cache_elements
        self.clean_cache_graph = clean_cache_graph
        self.create_graph()

        self.matrix_simple_topics = {}

    def create_graph(self):
        if self.clean_cache_elements:
            self.save_pickle_label_artworks()
            self.save_pickle_segments_data()
            self.save_pickle_label_characters()
            self.save_pickle_label_references()
            self.save_pickle_label_events()
            self.save_pickle_ne_entities()

            self.get_pickled_data()
        else:
            self.get_pickled_data()

        if self.clean_cache_graph:
            if 'ne_e_io' in self.type:
                self.add_nodes_references()
                self.add_nodes_characters()
                # self.add_nodes_events()
            elif 'seg_' in self.type:
                self.add_segments()
            else:
                self.add_nodes_references()
                self.add_nodes_characters()
                self.add_nodes_events()
                self.add_segments()
            self.save_pickle_graph(file='graphs/graph_' + self.type + '.pickle')
        else:
            self.get_pickle_graph(file='graphs/graph_' + self.type + '.pickle')

    def export_graph_to_pajek(self, file=False, graph=False):
        if not file:
            file_pajek = 'graphs/graph_' + self.type + '.net'
        else:
            file_pajek = file
        if not graph:
            graph = self.graph
        print("export_graph_to_pajek - " + str(file_pajek))
        # pos = nx.spring_layout(self.graphs)
        # nx.draw_networkx_nodes(self.graphs, pos, node_size=100, node_color='red')
        # nx.draw_networkx_edges(self.graphs, pos)
        # nx.draw_networkx_labels(self.graphs, pos, font_size='8', font_color='blue')
        # plt.show()
        nx.write_pajek(graph, file_pajek)
        # functions.replace(file_pajek, '0.0 0.0 ellipse', '')
        functions.replace(file_pajek, '0.0 0.0 ellipse', '" 0.0 0.0 box ic Red fos 25')
        functions.replace_if_type_node(file_pajek, ' SEG_', ' "SEG_', 'Red', 'Red')
        functions.replace_if_type_node(file_pajek, ' IO_', ' "IO_', 'Red', 'Grey')
        functions.replace_if_type_node(file_pajek, ' ARTW_', ' "ARTW_', 'Red', 'Blue')
        functions.replace_if_type_node(file_pajek, ' CH_', ' "CH_', 'Red', 'Orange')
        functions.replace_if_type_node(file_pajek, ' REF_', ' "REF_', 'Red', 'Purple')
        functions.replace_if_type_node(file_pajek, ' EV_', ' "EV_', 'Red', 'Yellow')
        functions.replace_if_type_node(file_pajek, ' E_', ' "E_', 'Red', 'Grey')
        # plt.savefig('labels.png')

    def add_nodes_references(self):
        print("add_nodes_references")
        query = """
        SELECT processed_references.id,processed_references.label,processed_references.id_entity,
processed_references.id_entity_instancia_de, wikidata_entidades.label AS label_entity, wikidata_instancias_de.label AS label_instancia_de
FROM processed_references
LEFT JOIN wikidata_entidades ON wikidata_entidades.id = processed_references.id_entity
LEFT JOIN wikidata_instancias_de ON wikidata_instancias_de.id = processed_references.id_entity_instancia_de 
        """
        results = MysqlND.execute_query(query, ())

        for result in results:
            id, label, id_entity, id_entity_instancia_de, label_entity, label_instancia_de = result[0], result[1], result[2], result[3], result[4], result[5]
            label = clean_labels_for_graph(label)

            key = "REF_" + str(id) + "_" + label.replace(" ", "_")

            # self.graph_ids.add_node(id, label=label)
            self.graph.add_node(key)
            if id_entity != -1:
                label_entity = clean_labels_for_graph(label_entity)
                key_entity = "E_" + str(id_entity) + "_" + label_entity
                self.graph.add_node(key_entity)
                self.graph.add_edge(key_entity, key)
                if id_entity_instancia_de != -1:
                    label_instancia_de = clean_labels_for_graph(label_instancia_de)
                    key_instance_of = "IO_" + str(id_entity_instancia_de) + "_" + label_instancia_de
                    self.graph.add_node(key_instance_of)
                    self.graph.add_edge(key_instance_of, key_entity)

    def add_nodes_characters(self):
        print("add_nodes_characters")
        query = """
            SELECT processed_characters.id,processed_characters.label,processed_characters.id_entity,
    processed_characters.id_entity_instancia_de, wikidata_entidades.label AS label_entity, wikidata_instancias_de.label AS label_instancia_de
    FROM processed_characters
    LEFT JOIN wikidata_entidades ON wikidata_entidades.id = processed_characters.id_entity
    LEFT JOIN wikidata_instancias_de ON wikidata_instancias_de.id = processed_characters.id_entity_instancia_de 
            """
        results = MysqlND.execute_query(query, ())

        for result in results:
            id, label, id_entity, id_entity_instancia_de, label_entity, label_instancia_de = result[0], result[1], result[2], result[3], result[4], result[5]
            label = clean_labels_for_graph(label)
            key = "CH_" + str(id) + "_" + label.replace(" ", "_")

            # self.graph_ids.add_node(id, label=label)
            self.graph.add_node(key)
            if id_entity != -1:
                label_entity = clean_labels_for_graph(label_entity)
                key_entity = "E_" + str(id_entity) + "_" + label_entity
                self.graph.add_node(key_entity)
                self.graph.add_edge(key_entity, key)
                if id_entity_instancia_de != -1:
                    label_instancia_de = clean_labels_for_graph(label_instancia_de)
                    key_instance_of = "IO_" + str(id_entity_instancia_de) + "_" + label_instancia_de
                    self.graph.add_node(key_instance_of)
                    self.graph.add_edge(key_instance_of, key_entity)

    def add_nodes_artworks(self):
        print("add_nodes_artworks")
        query = """SELECT id FROM processed_artworks  """
        results = MysqlND.execute_query(query, ())
        for result in results:
            id = result[0]
            label_artwork = self.artworks_labels[id]
            label_artwork = clean_labels_for_graph(label_artwork).replace(" ", "_")
            key = "ARTW_" + str(id) + "_" + label_artwork.replace(" ", "_")
            self.graph.add_node(key)

    def add_nodes_events(self):
        print("add_nodes_events")
        query = """
            SELECT processed_events.id,processed_events.year,processed_events.label,processed_events.event_type,processed_events.place,processed_events.init,processed_events.end,
processed_events.id_artwork_related,processed_events.id_character_related,processed_events.id_reference_related,processed_characters.label as label_character, processed_references.label as label_reference
FROM processed_events 
left JOIN processed_artworks ON  processed_artworks.id = processed_events.id_artwork_related
left JOIN processed_characters ON  processed_characters.id = processed_events.id_character_related
left JOIN processed_references ON  processed_references.id = processed_events.id_reference_related
WHERE (processed_events.id_artwork_related != -1 OR processed_events.id_character_related != -1 OR processed_events.id_reference_related != -1) 
            """
        results = MysqlND.execute_query(query, ())

        for result in results:
            id, year, label, event_type, place, init, end, id_artwork_related, id_character_related, id_reference_related, label_character, label_reference = result[0], result[1], result[2], result[3], result[4], result[5], result[6], result[7], result[8], result[9], result[10], result[11]
            label = clean_labels_for_graph(label)

            key = "EV_" + str(id) + "_" + label.replace(" ", "_")

            # self.graph_ids.add_node(id, label=label)
            self.graph.add_node(key)
            if id_artwork_related != -1:
                label_artwork = self.artworks_labels[id_artwork_related]
                label_artwork = clean_labels_for_graph(label_artwork).replace(" ", "_")
                key_element = "ARTW_" + str(id_artwork_related) + "_" + label_artwork
                self.graph.add_node(key_element)
                self.graph.add_edge(key, key_element)
            elif id_character_related != -1:
                label_character = clean_labels_for_graph(label_character).replace(" ", "_")
                key_element = "CH_" + str(id_character_related) + "_" + label_character
                self.graph.add_node(key_element)
                self.graph.add_edge(key_element, key)
            elif id_reference_related != -1:
                label_reference = clean_labels_for_graph(label_reference).replace(" ", "_")
                key_element = "REF_" + str(id_reference_related) + "_" + label_reference
                self.graph.add_node(key_element)
                self.graph.add_edge(key_element, key)

    def add_segments(self, graph_='global', segments=False):
        print("add_segments")
        if not segments:
            segments = self.segments
        if graph_ == 'global':
            graph = self.graph
        elif graph_ == 'local':
            graph = nx.Graph()
        else:
            graph = nx.DiGraph()
        text = ''
        for sg in segments:
            key = "SEG_" + str(sg['id'])
            text += str(sg['text']) + "\n"
            graph.add_node(key)
            if 'seg_refs' in self.type and graph_ == 'global':
                sg['list_characters_segment'] = []
                sg['list_events_segment'] = []
                sg['list_artworks_segment'] = []
            elif 'seg_ch_e_io' in self.type and graph_ == 'global':
                sg['list_references_segment'] = []
                sg['list_events_segment'] = []
            else:
                if len(sg['list_characters_segment']) > 0:
                    for id_element in sg['list_characters_segment']:
                        if 'seg_e_' in self.type and graph_ == 'global':
                            if "CH_" + str(id_element) in self.labels_ne_entities:
                                data_value = self.labels_ne_entities["CH_" + str(id_element)]
                                label_entity = clean_labels_for_graph(data_value[2])
                                key_entity = "CH_" + str(data_value[1])
                                graph.add_node(key_entity)
                                graph.add_edge(key_entity, key)
                            else:
                                print("ERROR key " + "CH_" + str(id_element))
                        else:
                            label = clean_labels_for_graph(self.labels_characters[id_element])
                            key_element = "CH_" + str(id_element) + "_" + label.replace(" ", "_")
                            graph.add_node(key_element)
                            graph.add_edge(key_element, key)
                if len(sg['list_references_segment']) > 0:
                    for id_element in sg['list_references_segment']:
                        if 'seg_e_' in self.type and graph_ == 'global':
                            if "REF_" + str(id_element) in self.labels_ne_entities:
                                data_value = self.labels_ne_entities["REF_" + str(id_element)]
                                label_entity = clean_labels_for_graph(data_value[2])
                                key_entity = "REF_" + str(data_value[1])
                                graph.add_node(key_entity)
                                graph.add_edge(key_entity, key)
                            else:
                                print("ERROR key " + "REF_" + str(id_element))
                        else:
                            label = clean_labels_for_graph(self.labels_references[id_element])
                            key_element = "REF_" + str(id_element) + "_" + label.replace(" ", "_")
                            graph.add_node(key_element)
                            graph.add_edge(key_element, key)
                if len(sg['list_events_segment']) > 0:
                    for id_element in sg['list_events_segment']:
                        if 'seg_e_' in self.type and graph_ == 'global':
                            if "EV_" + str(id_element) in self.labels_ne_entities:
                                data_value = self.labels_ne_entities["EV_" + str(id_element)]
                                label_entity = clean_labels_for_graph(data_value[2])
                                key_entity = "EV_" + str(data_value[1])
                                graph.add_node(key_entity)
                                graph.add_edge(key_entity, key)
                            # else:
                            #     print("ERROR key " + "EV_" + str(id_element))
                        else:
                            label = clean_labels_for_graph(self.labels_events[id_element])
                            key_element = "EV_" + str(id_element) + "_" + label.replace(" ", "_")
                            graph.add_node(key_element)
                            graph.add_edge(key_element, key)
                if len(sg['list_artworks_segment']) > 0:
                    for id_element in sg['list_artworks_segment']:
                        if 'seg_e_' in self.type and graph_ == 'global':
                            if "ARTW_" + str(id_element) in self.labels_ne_entities:
                                data_value = self.labels_ne_entities["ARTW_" + str(id_element)]
                                label_entity = clean_labels_for_graph(data_value[2])
                                key_entity = "ARTW_" + str(data_value[1])
                                graph.add_node(key_entity)
                                graph.add_edge(key_entity, key)

                        else:
                            label = clean_labels_for_graph(self.artworks_labels[id_element])
                            key_element = "ARTW_" + str(id_element) + "_" + label.replace(" ", "_")
                            graph.add_node(key_element)
                            graph.add_edge(key, key_element)
                if len(sg['authors']) > 0:
                    for id_element in sg['authors']:
                        if id_element != -1:
                            if 'seg_e_' in self.type and graph_ == 'global':
                                if "CH_" + str(id_element) in self.labels_ne_entities:
                                    data_value = self.labels_ne_entities["CH_" + str(id_element)]
                                    label_entity = clean_labels_for_graph(data_value[2])
                                    key_entity = "CH_" + str(data_value[1])
                                    graph.add_node(key_entity)
                                    graph.add_edge(key_entity, key)
                                else:
                                    print("ERROR key " + "CH_" + str(id_element))
                            else:
                                label = clean_labels_for_graph(self.labels_characters[id_element])
                                key_element = "CH_" + str(id_element) + "_" + label.replace(" ", "_")
                                graph.add_node(key_element)
                                graph.add_edge(key_element, key)
        if not graph:
            self.graph = graph
        return graph, text

    def save_pickle_segments_data(self):
        data = []
        segments = self.get_solr_segments_data("*:*", multiple=True)
        for sg in segments:
            results = {}
            results['id'] = sg['id']
            results['text'] = sg['text']

            results['list_characters_segment'] = []
            if 'list_characters_segment' in sg:
                results['list_characters_segment'] = sg['list_characters_segment']

            results['list_references_segment'] = []
            if 'list_references_segment' in sg:
                results['list_references_segment'] = sg['list_references_segment']

            results['list_artworks_segment'] = []
            if 'list_artworks_segment' in sg:
                results['list_artworks_segment'] = sg['list_artworks_segment']

            results['list_events_segment'] = []
            if 'list_events_segment' in sg:
                results['list_events_segment'] = sg['list_events_segment']

            results['authors'] = []
            if 'authors' in sg:
                results['authors'] = sg['authors']
            data.append(results)
        functions.save_pickle('labels/segments.pickle', data)

    def save_pickle_label_artworks(self):
        results = {}
        query = "SELECT id FROM processed_artworks"
        artworks = MysqlND.execute_query(query, ())
        for artwork in artworks:
            id = artwork[0]
            data = self.get_solr_artwork_data(id, multiple=False)
            if data:
                label = data['name']
                results[id] = label

        query = "SELECT id,label FROM processed_references WHERE type=2"
        rs = MysqlND.execute_query(query, ())
        for result in rs:
            results[result[0]] = result[1]

        functions.save_pickle('labels/labels_artworks.pickle', results)

    def save_pickle_label_characters(self):
        data = {}
        query = "SELECT id,label FROM processed_characters"
        results = MysqlND.execute_query(query, ())
        for result in results:
            data[result[0]] = result[1]

        query = "SELECT id,label FROM processed_references WHERE type=6"
        results = MysqlND.execute_query(query, ())
        for result in results:
            data[result[0]] = result[1]

        functions.save_pickle('labels/labels_characters.pickle', data)

    def save_pickle_label_references(self):
        data = {}
        query = "SELECT id,label FROM processed_references WHERE type!=6 and type!=2 AND type!=8 AND type!=3"
        results = MysqlND.execute_query(query, ())
        for result in results:
            data[result[0]] = result[1]
        functions.save_pickle('labels/labels_references.pickle', data)

    def save_pickle_label_events(self):
        data = {}
        query = "SELECT id,label FROM processed_events"
        results = MysqlND.execute_query(query, ())
        for result in results:
            data[result[0]] = result[1]
        functions.save_pickle('labels/labels_events.pickle', data)

    def save_pickle_ne_entities(self):
        data = {}
        query = """
                SELECT processed_references.id,processed_references.label,processed_references.id_entity,
        processed_references.id_entity_instancia_de, wikidata_entidades.label AS label_entity, wikidata_instancias_de.label AS label_instancia_de
        FROM processed_references
        LEFT JOIN wikidata_entidades ON wikidata_entidades.id = processed_references.id_entity
        LEFT JOIN wikidata_instancias_de ON wikidata_instancias_de.id = processed_references.id_entity_instancia_de 
        WHERE processed_references.type != 6 AND processed_references.type != 2 AND processed_references.type != 8
                """
        results = MysqlND.execute_query(query, ())
        for result in results:
            id, label, id_entity, id_entity_instancia_de, label_entity, label_instancia_de = result[0], result[1], result[2], result[3], result[4], result[5]
            key = "REF_" + str(id_entity)
            data[key] = [label, id_entity, label_entity, id_entity_instancia_de, label_instancia_de]

        query = """
         SELECT processed_characters.id,processed_characters.label,processed_characters.id_entity,
    processed_characters.id_entity_instancia_de, wikidata_entidades.label AS label_entity, wikidata_instancias_de.label AS label_instancia_de
    FROM processed_characters
    LEFT JOIN wikidata_entidades ON wikidata_entidades.id = processed_characters.id_entity
    LEFT JOIN wikidata_instancias_de ON wikidata_instancias_de.id = processed_characters.id_entity_instancia_de
         """
        results = MysqlND.execute_query(query, ())
        for result in results:
            id, label, id_entity, id_entity_instancia_de, label_entity, label_instancia_de = result[0], result[1], result[2], result[3], result[4], result[5]
            key = "CH_" + str(id_entity)
            data[key] = [label, id_entity, label_entity, id_entity_instancia_de, label_instancia_de]

        query = """
                SELECT processed_references.id,processed_references.label,processed_references.id_entity,
        processed_references.id_entity_instancia_de, wikidata_entidades.label AS label_entity, wikidata_instancias_de.label AS label_instancia_de
        FROM processed_references
        LEFT JOIN wikidata_entidades ON wikidata_entidades.id = processed_references.id_entity
        LEFT JOIN wikidata_instancias_de ON wikidata_instancias_de.id = processed_references.id_entity_instancia_de 
        WHERE processed_references.type = 6
                """
        results = MysqlND.execute_query(query, ())
        for result in results:
            id, label, id_entity, id_entity_instancia_de, label_entity, label_instancia_de = result[0], result[1], result[2], result[3], result[4], result[5]
            key = "CH_" + str(id_entity)
            data[key] = [label, id_entity, label_entity, id_entity_instancia_de, label_instancia_de]

        query = """ SELECT id,label FROM wikidata_entidades WHERE id_instancia_de=818 """
        results = MysqlND.execute_query(query, ())
        for result in results:
            id_entity, label = result[0], result[1]
            key = "ARTW_" + str(id_entity)
            data[key] = [label, id_entity, '818', 'cuadro']

        functions.save_pickle('labels/labels_ne_entities.pickle', data)

    def get_pickled_data(self):
        self.artworks_labels = functions.get_pickle('labels/labels_artworks.pickle')
        self.segments = functions.get_pickle('labels/segments.pickle')
        self.labels_references = functions.get_pickle('labels/labels_references.pickle')
        self.labels_characters = functions.get_pickle('labels/labels_characters.pickle')
        self.labels_events = functions.get_pickle('labels/labels_events.pickle')
        self.labels_ne_entities = functions.get_pickle('labels/labels_ne_entities.pickle')

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

    def save_pickle_graph(self, file='final_graph.pickle'):
        nx.write_gpickle(self.graph, file)

    def get_pickle_graph(self, file='final_graph.pickle'):
        self.graph = nx.read_gpickle(file)

    def get_frequencies_references_artworks(self):
        row = ''
        query_solr_final = Solrindex('http://localhost:8983/solr/ND_final')
        for key, value in self.labels_references.iteritems():
            query = 'keywords_mp:/.*id_reference=\\"' + str(key) + '\\".*/'
            result = query_solr_final.search(query)
            cuenta = len(result.docs)
            row += "'" + str(key) + "':'" + str(cuenta) + "'\n"

        pprint(row)

    def get_matrix_frequencies_references_segments(self):
        row = ''
        query_solr_final = Solrindex('http://localhost:8983/solr/ND_final')
        for key, value in self.labels_references.iteritems():
            query = 'keywords_mp:/.*id_reference=\\"' + str(key) + '\\".*/'
            result = query_solr_final.search(query)
            cuenta = len(result.docs)
            row += "'" + str(key) + "':'" + str(cuenta) + "'\n"
        pprint(row)

    def save_segments_per_entity(self):
        data = {}
        for key, value in self.labels_ne_entities.iteritems():
            label_entity, id_entity, id_instance_of, label_instance_of = value[0], value[1], value[2], value[3]
            print label_entity
            splitted = key.split("_")
            key_entity = splitted[0] + "_" + str(id_entity)
            if key_entity not in self.graph:
                neighbors = []
            else:
                neighbors = self.get_neighbors(key_entity)
                neighbors = {x[1] for x in neighbors if 'SEG_' in x[1]}
                neighbors = list(neighbors)
            data[key_entity] = neighbors

        functions.save_pickle('labels/segments_per_entity.pickle', data)

    def filter_data(self):
        print(nx.info(self.graph))
        # grades = nx.degree(self.graph)
        # pprint(grades)

        # ordered = sorted(grades, key=lambda x: x[1], reverse=True)
        # list_grades = list(ordered)
        # pprint(list_grades[:50])

        self.segments_per_entity = functions.get_pickle('labels/segments_per_entity.pickle')
        # count_segments_per_entity = {}
        # for ne, segments_list in self.segments_per_entity.iteritems():
        #     if '-1' not in ne:
        #         count_segments_per_entity[ne] = len(segments_list)
        # first_filter, second_filter = self.get_graphic_data(count_segments_per_entity, file_='filter_3_median.png')
        # mean, median, maximum, minimum, filter_ = first_filter[0], first_filter[1], first_filter[2], first_filter[3], first_filter[4]
        # mean_2, median_2, maximum_2, minimum_2, filter_2 = second_filter[0], second_filter[1], second_filter[2], second_filter[3], second_filter[4]
        # print("Mean: " + str(mean) + ", median: " + str(median) + ", maximum: " + str(maximum) + ", minimum: " + str(minimum) + ", filter: " + str(filter_))
        # print("Mean_2: " + str(mean_2) + ", median_2: " + str(median_2) + ", maximum_2: " + str(maximum_2) + ", minimum_2: " + str(minimum_2) + ", filter_2: " + str(filter_2))

        count_segments_per_entity = {}
        for ne, segments_list in self.segments_per_entity.iteritems():
            if '-1' not in ne:
                segments_list_artworks = set()
                for sl in segments_list:
                    aux = sl.split("_")[1]
                    segments_list_artworks.add(aux)
                count_segments_per_entity[ne] = len(segments_list_artworks)
        first_filter, second_filter = self.get_graphic_data(count_segments_per_entity, file_='filter_4_prueba_2.png')
        # first_filter, second_filter = self.get_graphic_data(count_segments_per_entity, file_='')
        # first_filter, second_filter = self.get_graphic_data(count_segments_per_entity, file_='')
        mean, median, maximum, minimum, filter_ = first_filter[0], first_filter[1], first_filter[2], first_filter[3], first_filter[4]
        mean_2, median_2, maximum_2, minimum_2, filter_2 = second_filter[0], second_filter[1], second_filter[2], second_filter[3], second_filter[4]
        print("Mean: " + str(mean) + ", median: " + str(median) + ", maximum: " + str(maximum) + ", minimum: " + str(minimum) + ", filter: " + str(filter_))
        print("Mean_2: " + str(mean_2) + ", median_2: " + str(median_2) + ", maximum_2: " + str(maximum_2) + ", minimum_2: " + str(minimum_2) + ", filter_2: " + str(filter_2))
        count_segments_per_entity_ = {x: y for x, y in count_segments_per_entity.iteritems() if y >= minimum_2 and y <= maximum_2}
        count_segments_per_entity = sorted(count_segments_per_entity_.items(), key=operator.itemgetter(1))
        pprint("Total elementos seleccionados: " + str(len(count_segments_per_entity)))
        subgraph = nx.DiGraph()
        for ne_s in count_segments_per_entity:
            ne = ne_s[0]
            neighbors = self.get_neighbors(ne)
            neighbors = {x[1] for x in neighbors if 'SEG_' in x[1]}
            neighbors = list(neighbors)
            subgraph.add_node(ne)
            for neighbor in neighbors:
                subgraph.add_node(neighbor)
                subgraph.add_edge(ne, neighbor)

        nx.write_gpickle(subgraph, 'graphs/subgraph_topics_filtered.pickle')
        self.export_graph_to_pajek(file='graphs/subgraph_topics_filtered.net', graph=subgraph)

    def filter_cuartil(self):
        def select_tramas(self):
            # subgraph = nx.read_gpickle("graphs/subgraph_topics_filtered.pickle")
            # print(nx.info(subgraph))

            self.segments_per_entity = functions.get_pickle('labels/segments_per_entity.pickle')
            count_segments_per_entity = {}
            for ne, segments_list in self.segments_per_entity.iteritems():
                if '-1' not in ne:
                    if 'CH_1787' in ne:
                        print("para")
                    segments_list_artworks = set()
                    for sl in segments_list:
                        aux = sl.split("_")[1]
                        segments_list_artworks.add(aux)
                    label = self.labels_ne_entities[ne]
                    count_segments_per_entity[ne + "_" + label[0]] = len(segments_list_artworks)

            count_segments_per_entity = {x: y for x, y in count_segments_per_entity.iteritems() if y > self.segments_per_itinerarie}
            count_segments_per_entity_ = count_segments_per_entity
            count_segments_per_entity = sorted(count_segments_per_entity.items(), key=operator.itemgetter(1), reverse=True)
            total = len(count_segments_per_entity)
            selected = (25 * total) / 100
            count_segments_per_entity_ = {x: y for x, y in count_segments_per_entity_.iteritems() if y < selected}
            count_segments_per_entity_ = sorted(count_segments_per_entity_.items(), key=operator.itemgetter(1), reverse=True)
            pprint(count_segments_per_entity_)

    def get_graphic_data(self, count_segments_per_entity_, file_=''):
        count_segments_per_entity_ = {x: y for x, y in count_segments_per_entity_.iteritems() if y > 3}

        count_segments_per_entity = sorted(count_segments_per_entity_.items(), key=operator.itemgetter(1))
        pprint("Total elementos primer filtro: " + str(len(count_segments_per_entity)))
        x, y = zip(*count_segments_per_entity)
        maximum = count_segments_per_entity[-1]
        minimum = count_segments_per_entity[1]
        mean = np.mean(y)
        median = np.median(y)
        # filter_ = (mean + maximum[1]) / 2
        filter_ = (median + maximum[1]) / 2

        # elements_removed = {x: y for x, y in count_segments_per_entity_.iteritems() if y > filter_}
        # elements_removed = sorted(elements_removed.items(), key=operator.itemgetter(1), reverse=True)
        # pprint(elements_removed)

        count_segments_per_entity_second_filter = {x: y for x, y in count_segments_per_entity_.iteritems() if y > self.segments_per_itinerarie}
        count_segments_per_entity_second_filter = {x: y for x, y in count_segments_per_entity_second_filter.iteritems() if y < filter_}

        count_segments_per_entity_second_filter = sorted(count_segments_per_entity_second_filter.items(), key=operator.itemgetter(1))
        pprint("Total elementos segundo filtro: " + str(len(count_segments_per_entity_second_filter)))



        maximum_second_filter = count_segments_per_entity_second_filter[-1]
        minimum_second_filter = count_segments_per_entity_second_filter[1]
        x_2, y_2 = zip(*count_segments_per_entity_second_filter)
        mean_2 = np.mean(y_2)
        median_2 = np.median(y_2)
        # second_filter_ = (mean_2 + maximum_second_filter[1]) / 2
        second_filter_ = (median_2 + maximum_second_filter[1]) / 2

        first_filter = [mean, median, maximum[1], minimum[1], filter_]
        second_filter = [mean_2, median_2, maximum_second_filter[1], minimum_second_filter[1], second_filter_]
        if file_ != '':
            plt.plot(x, y)
            first_element_y = [i for i, x in enumerate(y) if x > mean]
            first_element_x = count_segments_per_entity[first_element_y[0]][0]
            plt.annotate('Media (' + str(mean) + ')', xy=(first_element_x, mean), xytext=(first_element_x, mean + 1000), arrowprops=dict(arrowstyle="->",                                                                                                                  connectionstyle="arc3,rad=.2"),
                         horizontalalignment='right', verticalalignment='top', fontsize=8)
            first_element_y = [i for i, x in enumerate(y) if x > median]
            first_element_x = count_segments_per_entity[first_element_y[0]][0]
            plt.annotate('Mediana (' + str(median) + ')', xy=(first_element_x, median), xytext=(first_element_x, median + 800), arrowprops=dict(arrowstyle="->",                                                                                                                     connectionstyle="arc3,rad=.2"),
                        horizontalalignment='right', verticalalignment='top', fontsize=8)
            plt.annotate('Max (' + str(maximum[1]) + ')', xy=(maximum[0], maximum[1]), xytext=(maximum[0], maximum[1] - 30), arrowprops=dict(arrowstyle="->",                                                                                                                connectionstyle="arc3,rad=.4"),
                         horizontalalignment='right', verticalalignment='top', fontsize=8)
            plt.annotate('Min (' + str(minimum[1]) + ')', xy=(minimum[0], minimum[1]), xytext=(minimum[0], minimum[1] + 500), arrowprops=dict(arrowstyle="->"),
                         horizontalalignment='right', verticalalignment='top', fontsize=8)

            first_element_y = [i for i, x in enumerate(y) if x > filter_]
            first_element_x = count_segments_per_entity[first_element_y[0]][0]
            plt.annotate('Filtro general (' + str(filter_) + ')', xy=(first_element_x, second_filter_), xytext=(first_element_x, filter_ + 500), arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=.1"),
                         horizontalalignment='right', verticalalignment='top', fontsize=8)

            first_element_y = [i for i, x in enumerate(y) if x > second_filter_]
            first_element_x = count_segments_per_entity[first_element_y[0]][0]
            plt.annotate('Filtro 2 (' + str(second_filter_) + ')', xy=(first_element_x, second_filter_), xytext=(first_element_x, second_filter_ + 800), arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=.2"),
                         horizontalalignment='right', verticalalignment='top', fontsize=8)

            plt.xlabel('Entidades (Wikidata)')
            plt.ylabel('Num de obras')
            plt.title('Entidades / Num de obras')
            # plt.grid(True)

            init_y = [i for i, x in enumerate(y) if x > filter_]
            init_x = count_segments_per_entity[init_y[0]][0]

            # plt.axvspan(minimum[0], minimum_second_filter[0], alpha=0.5)
            # plt.axvspan(init_x, maximum[0], alpha=0.5)
            # print("MIN: " + str(minimum[0]) + " - MAX: " + str(minimum_second_filter[0]))
            # print("MIN: " + str(minimum[1]) + " - MAX: " + str(minimum_second_filter[1]))
            # print("MIN: " + str(init_x) + " - MAX: " + str(maximum[0]))
            # print("MIN: " + str(count_segments_per_entity[init_y[0]][1]) + " - MAX: " + str(maximum[1]))

            plt.axvspan(minimum_second_filter[0], init_x, alpha=0.5)
            print("MIN: " + str(minimum_second_filter[1]) + " - MAX: " + str(count_segments_per_entity[init_y[0]][1]))

            plt.savefig(file_)

        return first_filter, second_filter


    def biggest_degree(self):
        return max(self.graph, key=self.graph.degree)

    def get_neighbors(self, node, graph_=False):
        if not graph_:
            graph_ = self.graph
        all_neighbors = nx.all_neighbors(graph_, node)
        paths = list()
        for neighbor in all_neighbors:
            try:
                length = nx.shortest_path_length(
                    graph_,
                    neighbor,
                    node,
                )
                heappush(paths, (length, neighbor))
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                pass
        return paths

    def calculate_itineraries(self):
        subgraph = nx.read_gpickle("graphs/subgraph_topics_filtered.pickle")
        print(nx.info(subgraph))
        print("Segmentos seleccionados / itinerario : " + str(self.segments_per_itinerarie))
        # paso 1. para cada personaje, extraer posibles segmentos
        if self.clean_cache_elements or not functions.exists_pickle('matrix_simple_topics.pickle'):
            self.create_matrix_simple_topics(subgraph)

        self.matrix_simple_topics = functions.get_pickle('matrix_simple_topics.pickle')
        # selected_narrative_elements_simple = {x: y for x, y in self.matrix_simple_topics.iteritems() if len(y) >= self.segments_per_itinerarie}
        # pprint(selected_narrative_elements_simple)
        topics = self.create_basic_topics(self.matrix_simple_topics)
        pprint(topics)

    def create_basic_topics(self, selected_narrative_elements_simple):
        data = {}
        if len(self.matrix_simple_topics) > 0:
            for ne, seg in selected_narrative_elements_simple.iteritems():
                if not functions.exists_pickle('single_topic_itineraries/' + ne + ".txt"):
                    topic_name = self.get_topic_name(ne)
                    graph, text = self.get_segments_formatted(seg, ne)
                    if not graph:
                        print("No creo el tema de " + ne)
                    else:
                        data[topic_name] = [graph, text]
                        self.export_graph_to_pajek(file='single_topic_graphs/' + ne + ".net", graph=graph)
                        text = '<title>' + topic_name + '</title><br>' + text
                        functions.save_text_to_file(text, 'single_topic_itineraries/' + ne + ".txt")
        return data

    def get_topic_name(self, ne):
        topic = 'Sin nombre'
        if not isinstance(ne, list):

            if 'CH_' in ne:
                label_name = self.labels_ne_entities[ne][2]
                topic = 'Cuadros relacionados con el personaje ' + label_name
            elif 'REF_' in ne:
                label_name = self.labels_ne_entities[ne][2]
                topic = 'Cuadros relacionados con ' + label_name
            elif 'ARTW_' in ne:
                label_name = self.labels_ne_entities[ne][0]
                topic = 'Cuadros relacionados con la obra ' + label_name
        return topic

    def get_segments_formatted(self, list_segments, ne):
        list_segments = [x.replace("SEG_", "") for x in list_segments]
        segments = [x for x in self.segments if x['id'] in list_segments]
        graph, text = self.add_segments(graph_='local', segments=segments)
        jaccard = nx.jaccard_coefficient(graph)
        text = ''

        data = {}
        for u, v, p in jaccard:
            if 'SEG_' in u and 'SEG_' in v:
                if float(p) < float(0.85):
                    text += u + " - " + v + " -> " + str(p) + '\n'
                    data[u + " - " + v] = p
        # data = {x:y for x,y in data.iteritems() if float(y) < float(0.9)}
        if len(data) > self.segments_per_itinerarie:
            functions.save_text_to_file(text, 'single_topic_jaccard/' + ne + '.txt')
            mean = np.mean(data.values())
            data_sorted = sorted(data.items(), key=operator.itemgetter(1))
            data_sorted = [i for i in data_sorted if i[1] >= mean][0]
            mid_data_label = data_sorted[0]
            path = self.calculate_path(mid_data_label,data)
            if len(path) == 0:
                return False, ''
            text = self.get_text_from_path(path)
            return graph, text
        else:
            return False, ''

    def get_text_from_path(self, path):
        text = ''
        for d in path:
            d = d.replace("SEG_", "")
            segment = [x for x in self.segments if x['id'] == d]
            text_aux = str(segment[0]['text'])
            soup = BeautifulSoup(text_aux, 'html.parser')
            paragraph = soup.find("p")

            key = d.split("_")
            paragraph.attrs = {}
            paragraph['id_artwork'] = key[0]
            paragraph['proc'] = key[1]
            paragraph['num_paragraph'] = key[2]
            text += str(soup) + "\n <br>"
        return text

    def calculate_path(self, mid_data_label, data):
        labels = []
        init_label, second_label = self.split_labels_nodes(mid_data_label)
        labels.append(init_label)
        labels.append(second_label)
        label_prev = second_label
        data = {x: y for x, y in data.iteritems() if ' - ' + init_label not in x}
        data = {x: y for x, y in data.iteritems() if ' - ' + second_label not in x}
        mid_data_label_reverse = second_label + " - " + init_label
        data = {x: y for x, y in data.iteritems() if x != mid_data_label and x != mid_data_label_reverse}
        while len(labels) <= self.segments_per_itinerarie:
            if len(data) == 0:
                return []
            label_new = self.get_next_node(label_prev, data, labels)
            label_a = label_new + " - " + label_prev
            label_reverse = label_prev + " - " + label_new
            data = {x: y for x, y in data.iteritems() if ' - ' + label_new not in x}
            data = {x: y for x, y in data.iteritems() if ' - ' + label_prev not in x}
            data = {x: y for x, y in data.iteritems() if x != label_a and x != label_reverse}
            labels.append(label_new)
            label_prev = label_new
        pprint(labels)
        return labels


    def split_labels_nodes(self, label):
        l = label.split(" - ")
        return l[0], l[1]

    def get_next_node(self, label, data_, labels, isiterate=False):
        key = label + ' - '
        data = {x:y for x,y in data_.iteritems() if key in x}
        if len(data) > 0:
            key = ' - ' + label
            data = {x: y for x, y in data_.iteritems() if key in x}
            if len(data) > 0:
                d = data
            else:
                if isiterate:
                    d = data_
                else:
                    randm = random.choice(data_.keys())
                    x, y = self.split_labels_nodes(randm)
                    if x not in labels:
                        return self.get_next_node(x, data_, labels, isiterate=True)
                    elif y not in labels:
                        return self.get_next_node(y, data_, labels, isiterate=True)
                    else:
                        randm = random.choice(data_.keys())
                        x, y = self.split_labels_nodes(randm)
                        return self.get_next_node(x, data_, labels, isiterate=True)
        else:
            if isiterate:
                d = data_
            else:
                randm = random.choice(data_.keys())
                x, y = self.split_labels_nodes(randm)
                if x not in labels:
                    return self.get_next_node(x, data_, labels, isiterate=True)
                elif y not in labels:
                    return self.get_next_node(y, data_, labels, isiterate=True)
                else:
                    randm = random.choice(data_.keys())
                    x, y = self.split_labels_nodes(randm)
                    return self.get_next_node(x, data_, labels, isiterate=True)
        mean = np.mean(d.values())
        data_sorted = sorted(d.items(), key=operator.itemgetter(1))
        data_sorted = [i for i in data_sorted if i[1] >= mean]
        mid_data_label = data_sorted[0][0]
        x, y = self.split_labels_nodes(mid_data_label)
        return y

    def create_matrix_simple_topics(self, subgraph=False):
        # idea -> for each cada NE, get segments with it
        if not subgraph:
            subgraph = self.graph
        narrative_elements = {x for x in subgraph.nodes if 'SEG_' not in x}
        print("Narrative elements => " + str(len(narrative_elements)))
        print(narrative_elements)
        narrative_elements = list(narrative_elements)
        # narrative_elements = narrative_elements[0:10]
        line = ''
        data = {}
        for narrative_element in narrative_elements:
            neighbors = self.get_neighbors(narrative_element)
            neighbors = {x[1] for x in neighbors if 'SEG_' in x[1]}
            neighbors = list(neighbors)
            line += """'""" + narrative_element + """':'""" + ','.join(neighbors) + """':""" + str(len(neighbors)) + """\n"""
            data[narrative_element] = neighbors
        functions.save_text_to_file(line, 'matrix_simple_topics.txt')
        functions.save_pickle('matrix_simple_topics.pickle', data)
        # print(line)


    def set_segments_per_itinerarie(self, segments_per_itinerarie):
        self.segments_per_itinerarie = segments_per_itinerarie



