import cx_Oracle
from pymongo import MongoClient


def get_oracle_connection(user, password, host, port, service):
    dsn = cx_Oracle.makedsn(host, port, service_name=service)
    return cx_Oracle.connect(user, password, dsn)


def get_mongo_connection(uri, db_name):
    client = MongoClient(uri)
    return client[db_name]["DBMS"]
