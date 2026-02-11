from src.graph.neo4j_driver import Neo4jDriver

def bootstrap_m1():
    driver = Neo4jDriver()

    # =========================
    # ENTITY (M1)
    # =========================
    entities = ["Customer", "Merchant"]

    for e in entities:
        driver.execute_write("""
            MERGE (ent:Entity {name: $name})
            MERGE (ent)-[:INSTANCE_OF]->(root:ENTITY {name:'ENTITY'})
        """, {"name": e})

    # =========================
    # EVENT (M1)
    # =========================
    driver.execute_write("""
        MERGE (evt:Event {name:'FinancialTransaction'})
        MERGE (evt)-[:INSTANCE_OF]->(root:EVENT {name:'EVENT'})
    """)

    # =========================
    # ATTRIBUTE (M1)
    # =========================
    attributes = ["amount", "country"]

    for attr in attributes:
        driver.execute_write("""
            MERGE (a:Attribute {name: $name})
            MERGE (a)-[:INSTANCE_OF]->(root:ATTRIBUTE {name:'ATTRIBUTE'})
        """, {"name": attr})

    # =========================
    # DATA SOURCE (M1)
    # =========================
    driver.execute_write("""
        MERGE (ds:DataSource {name:'SyntheticGenerator'})
        MERGE (ds)-[:INSTANCE_OF]->(root:DATA_SOURCE {name:'DATA_SOURCE'})
    """)

    driver.close()
    print("✅ M1 fraud domain bootstrapped successfully.")

if __name__ == "__main__":
    bootstrap_m1()

