from src.graph.neo4j_driver import Neo4jDriver

def bootstrap_constraints():
    driver = Neo4jDriver()

    queries = [
        # Uniqueness constraints
        """
        CREATE CONSTRAINT customer_id_unique
        IF NOT EXISTS
        FOR (c:Customer)
        REQUIRE c.id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT merchant_id_unique
        IF NOT EXISTS
        FOR (m:Merchant)
        REQUIRE m.id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT event_name_unique
        IF NOT EXISTS
        FOR (e:Event)
        REQUIRE e.name IS UNIQUE
        """,
        """
        CREATE CONSTRAINT attribute_name_unique
        IF NOT EXISTS
        FOR (a:Attribute)
        REQUIRE a.name IS UNIQUE
        """,
        """
        CREATE CONSTRAINT datasource_name_unique
        IF NOT EXISTS
        FOR (d:DataSource)
        REQUIRE d.name IS UNIQUE
        """,

        # Indexes
        """
        CREATE INDEX transaction_id_index
        IF NOT EXISTS
        FOR (t:TransactionInstance)
        ON (t.id)
        """,
        """
        CREATE INDEX transaction_time_index
        IF NOT EXISTS
        FOR (t:TransactionInstance)
        ON (t.timestamp)
        """
    ]

    for q in queries:
        driver.execute_write(q)

    driver.close()
    print("✅ Constraints and indexes bootstrapped successfully.")

if __name__ == "__main__":
    bootstrap_constraints()
