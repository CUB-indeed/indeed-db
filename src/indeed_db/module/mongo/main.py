from typing import Union

import logging
import urllib
from pymongo import MongoClient


class MongoDB:
    def __init__(
        self,
        hostname:str,
        username:str,
        password:str,
        port:Union[str,int] = 27017,
        db_name:str="delfine"
        ) -> None:
        self.client = MongoClient(
            host=hostname,
            port=int(port),
            username=urllib.parse.quote_plus(username),
            password=urllib.parse.quote_plus(password),
        )
        self.db = self.client[db_name]
            
    def changeDatabase(self, db_name:str):
        self.db = self.client[db_name]
        logging.info(f"The database has changed to {db_name}")

    def close(self):
        if self.client is not None:
            self.client.close()


