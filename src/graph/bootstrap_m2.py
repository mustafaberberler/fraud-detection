from src.graph.neo4j_driver import Neo4jDriver

M2_NODES = [
    "ENTITY",
    "ATTRIBUTE",
    "RELATIONSHIP",
    "EVENT",
    "FEATURE",
    "MODEL",
    "RISK_SIGNAL",
    "DATA_SOURCE"
]

def bootstrap_m2():
    driver = Neo4jDriver()

    # 1️⃣ Root nodes
    for name in M2_NODES:
        driver.execute_write(
            """
            MERGE (n {name: $name})
            SET n:M2
            """,
            {"name": name}
        )

    # 2️⃣ Core relationships (metamodel semantics)
    driver.execute_write("""
        MATCH (e:M2 {name:'ENTITY'}), (a:M2 {name:'ATTRIBUTE'})
        MERGE (e)-[:HAS_ATTRIBUTE]->(a)
    """)

    driver.execute_write("""
        MATCH (e:M2 {name:'ENTITY'}), (ev:M2 {name:'EVENT'})
        MERGE (e)-[:PARTICIPATES_IN]->(ev)
    """)

    driver.execute_write("""
        MATCH (ev:M2 {name:'EVENT'}), (rs:M2 {name:'RISK_SIGNAL'})
        MERGE (ev)-[:GENERATES]->(rs)
    """)

    driver.execute_write("""
        MATCH (m:M2 {name:'MODEL'}), (f:M2 {name:'FEATURE'})
        MERGE (m)-[:USES_FEATURE]->(f)
    """)

    driver.close()
    print("✅ M2 metamodel bootstrapped successfully.")

if __name__ == "__main__":
    bootstrap_m2()

