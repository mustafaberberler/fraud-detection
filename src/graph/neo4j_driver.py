from neo4j import GraphDatabase

class Neo4jDriver:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_write(self, query: str, params: dict = None):
        with self.driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(query, params or {})
            )
