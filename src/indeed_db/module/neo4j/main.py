from typing import Union, Dict, List, Any

import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, CypherSyntaxError, DatabaseError


class Neo4j:
    def __init__(
        self,
        hostname:str,
        username:str,
        password:str,
        port:Union[str,int] = 7687,
        db_name:str="neo4j",
        url_scheme:str = "bolt",
        auth_scheme:str = "basic",
        ) -> None: 
        self.db_name = db_name
        self.username = username
        
        self.url = f"{url_scheme}://{hostname}:{port}"
        auth = (auth_scheme, username, password)
        try:
            self.driver = GraphDatabase.driver(self.url, auth=auth)
        except Exception as e:
            self.driver = None
            logging.error(f"Failed to create driver: {e}")
            # print("Failed to create driver:", e)
    
    def __str__(self):
        return f"{__class__.__name__} (uri: {self.url}, username: '{self.username}')"
    
    def changeDatabase(self, db_name:str):
        self.db_name = db_name
        logging.info(f"The database has changed to {db_name}")

    def init_ontology(self):
        # check if the neccessary config already existed. if not, create.
        query_string = "CALL db.constraints()"        
        n10_constrait = any(constrait["name"] == "n10s_unique_uri" for constrait in self.query(query_string))
        if not n10_constrait:
            self.query(
                "CREATE CONSTRAINT n10s_unique_uri ON (r:Resource) "
                "ASSERT r.uri IS UNIQUE " 
            )

        # check if the neccessary config already existed. if not, create.
        query_string = "match(n) return count(n)"
        count = self.query(query_string)[0]["count(n)"]
        if count == 0:
            self.query("CALL n10s.graphconfig.init()")

    def uploadFile(self, file_path, file_type="RDF/XML"): #uploading Turtle files
        # Common file type: JSON-LD, Turtle, RDF/XML, N-Triples and  TriG
        query_string = f"CALL n10s.rdf.import.fetch('{file_path}', {file_type})"
        return self.query(query_string)

    def query(self, query:str, parameters:Dict=None) -> List[Dict[str, Any]]: 
        # query commands of choice.
        assert self.driver is not None, logging.error("Driver not initialized!")
        session, data = None, None
        try:
            session = self.driver.session(database=self.db_name)
            response = session.run(query, parameters=parameters)
            data = response.data()
            logging.info("Sucessfully excecute")
        except ServiceUnavailable as e:
            data = [{"Error": f"Service Unavailable! -> {e}"}]
            logging.info(data)
            raise(data)
        except CypherSyntaxError as e:
            data = [{"Error": f"Invalid Cypher Syntax! Try adjusting Matches. -> {e}"}]
            logging.info(data)
            raise(data)
        finally:
            if session is not None:
                session.close()
        return data

    def close(self):
        if self.driver is not None:
            self.driver.close()


