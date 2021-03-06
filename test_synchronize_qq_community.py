# -*- coding: utf-8 -*-
import sqlalchemy as db
import logging
import sys

sys.path.append("..")
import synchronize

logging.info('Create independent engine.')
connection_uri = "mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}".format(**{
    "user": "root",
    "password": "mysql123456",
    "host": "localhost",
    "port": "3306",
    "dbname": "xinguan"
})
engine = db.create_engine(connection_uri)
synchronize.synchronize_qq_community(engine)
