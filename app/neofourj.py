import os

from neo4j import GraphDatabase


class NeoFourJ:
    NEO4J_URI = os.environ['NEO4J_URI']
    NEO4J_USER = os.environ['NEO4J_USER']
    NEO4J_PASS = os.environ['NEO4J_PASS']

    def __init__(self):
        self.driver = GraphDatabase.driver(self.NEO4J_URI, auth=(self.NEO4J_USER, self.NEO4J_PASS))

    def close(self):
        self.driver.close()

    def get_rank(self, param: object) -> object:
        with self.driver.session() as session:
            rank = session.write_transaction(self._get_rank, param)
            return rank

    @staticmethod
    def _get_rank(tx, param):
        result = tx.run(
            'MATCH (parent:Taxonomies)-[:CHILD]->(child:Taxonomies) where parent.name=~' '"' '.*' + param + '.*' '" RETURN parent')
        return result.single()[0]
