# coding=utf-8
from __future__ import print_function

import pysolr


class Solrindex:

    def __init__(self, host='http://localhost:8983/solr/ND_preprocessing'):
        self.host = host
        self.documents = []
        self.results = []
        self.similar_results = []
        self.solr = pysolr.Solr(self.host, timeout=10)

    def add_data(self, list_data):
        """
            Add Solr data. Example of array (list_data):
            [{"id": "doc_1","title": "A test document",},{"id": "doc_2","title": "The Banana: Tasty or Dangerous?",},]
        :param list_data: array
        """
        if isinstance(list_data, (list, tuple)):
            self.documents = list_data
        else:
            self.documents.append(list_data)
        self.solr.add(self.documents)

    def search(self, data):
        """
            Solr query. # Later, searching is easy. In the simple case, just a plain Lucene-style
                        # query is fine.
            # For a more advanced query, say involving highlighting, you can pass
            # additional options to Solr.
            results = solr.search('bananas', **{
                'hl': 'true',
                'hl.fragsize': 10,
            })

            # The ``Results`` object stores total results found, by default the top
            # ten most relevant results and any additional data like
            # facets/highlighting/spelling/etc.
            print("Saw {0} result(s).".format(len(results)))

            # Just loop over it to access the results.
            for result in results:
                print("The title is '{0}'.".format(result['title']))
        :param key: string
        :param value: string
        :return:
        """
        self.results = self.solr.search(data, rows=1000000)

        return self.results

    def get_similar(self):
        # You can also perform More Like This searches, if your Solr is configured
        # correctly.
        self.similar_results = self.solr.more_like_this(q='id:doc_2', mltfl='text')
        return self.similar_results

    def delete(self, params='*:*'):
        """
            delete all documents by default, query if param
            self.solr.delete(id=id_doc)
        """
        self.solr.delete(q=params)



