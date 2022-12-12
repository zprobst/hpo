import json
from getpass import getpass

from neo4j import GraphDatabase


DATA_FILE = "hpo.json"

CONSTRAINT_QUERY = """
CREATE CONSTRAINT hpo_term_unique IF NOT EXISTS
FOR (n:HPO)
REQUIRE n.HPO_term IS UNIQUE
"""

CREATE_NODE_QUERY = """
MERGE (n:HPO{HPO_term: $id}) 
ON MATCH SET n += $properties
ON CREATE SET n += $properties
"""

CREATE_EDGE_QUERY = """
MATCH (a:HPO{HPO_term: $id_a})
MATCH (b:HPO{HPO_term: $id_b})
MERGE (a)-[:IS_DESCENDANT_OF]->(b)
"""

def uri_to_hpo(uri):
    return uri.split("/")[-1].replace("_", ":")


def get_driver():
    database_hostname = input("Database Uri (Enter bolt://localhost for local): ")
    username = input("Username: ")
    password = getpass()

    return GraphDatabase.driver(database_hostname, auth=(username, password))


def create_nodes(data, driver):
    with driver.session() as session: 
        session.run(CONSTRAINT_QUERY)
        nodes_data = data["graphs"][0]["nodes"]
        hpo_nodes = list(n for n in nodes_data if "HP" in n["id"])
        print(len(hpo_nodes))
        for i, n in enumerate(hpo_nodes):
            id = uri_to_hpo(n["id"])
            properties = dict()
            properties["label"] = n["lbl"]
            properties["defintion"] = n.get("meta", {}).get("definition", {}).get("val")
            session.run(CREATE_NODE_QUERY, id=id, properties=properties)

            if i % 100 == 0:
                print(f"Processed {i} hpo nodes")
            

def create_rels(data, driver):
    with driver.session() as session: 
        rels_data = data["graphs"][0]["edges"]
        for i, rel in enumerate(rels_data):
            id_a = uri_to_hpo(rel["sub"])
            id_b = uri_to_hpo(rel["obj"])
            session.run(CREATE_EDGE_QUERY, id_b=id_b, id_a=id_a)

            if i % 100 == 0:
                print(f"Processed {i} hpo relationships")

def main():
    with open(DATA_FILE) as fp:
        data = json.load(fp)

    driver = get_driver()
    create_nodes(data, driver)
    create_rels(data, driver)


if __name__ == "__main__":
    main()
