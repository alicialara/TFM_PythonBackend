# coding=utf-8
from __future__ import print_function

# 1. Extract information from: Wikidata, Wikipedia, Museo del Prado

# extract = InformationExtractor()
# preprocess = PreprocessInformation()
# preprocess.extract_all_wars()
# preprocess.extract_all_characters_from_wikidata()
# preprocess.create_events_from_all_characters()

# 2. Segmentate information

# sg = Segmentation()
# sg.annotate_next_mp_artwork_data()
# sg.segmentate_next_artwork_data()
# sg.save_list_of_narrative_elements()
# sg.clean_final_data()

# 3. Create graphs

# graph = KnowledgeGraph(directed=False, type='seg_ne', clean_cache_elements=False, clean_cache_graph=False)
# graph = KnowledgeGraph(directed=True, type='seg_ne', clean_cache_elements=False, clean_cache_graph=False)

# graph = KnowledgeGraph(directed=False, type='seg_refs', clean_cache_elements=False, clean_cache_graph=False)
# graph = KnowledgeGraph(directed=True, type='seg_refs', clean_cache_elements=False, clean_cache_graph=False)

# graph = KnowledgeGraph(directed=True, type='ne_e_io', clean_cache_elements=False, clean_cache_graph=False)
# graph = KnowledgeGraph(directed=False, type='ne_e_io', clean_cache_elements=False, clean_cache_graph=False)

# graph = KnowledgeGraph(directed=True, type='seg_e', clean_cache_elements=False, clean_cache_graph=False)
# graph = KnowledgeGraph(directed=False, type='seg_e', clean_cache_elements=False, clean_cache_graph=False)

# graph = KnowledgeGraph(directed=False, type='seg_ch_e_io', clean_cache_elements=False, clean_cache_graph=False)
# graph = KnowledgeGraph(directed=True, type='seg_ch_e_io', clean_cache_elements=False, clean_cache_graph=False)

# graph = KnowledgeGraph(directed=False, type='ch_e_io', clean_cache_elements=False, clean_cache_graph=False)
# graph = KnowledgeGraph(directed=True, type='ch_e_io', clean_cache_elements=False, clean_cache_graph=False)
#
# graph.export_graph_to_pajek()
# graph.save_pickle_ne_entities()
# graph.save_segments_per_entity()
# graph.filter_data()

# 4. Calculate itineraries

# graph.segments_per_itinerarie = 10
# graph.calculate_itineraries()

# graphs.get_frequencies_references_artworks()

# 5. Index results for web use

# index = IndexResults()
# index.index_itineraries()
# index.update_artworks_basic_data()
# index.update_images_artworks()
# index.update_characters_data()
# index.update_references_data()
# index.fill_core_nes()
# index.update_images_data()
