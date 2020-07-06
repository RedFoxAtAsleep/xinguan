# -*- coding: utf-8 -*-
import sqlalchemy as db
import logging
from synchronize import synchronize_qq_community

if __name__ == '__main__':
    # engine
    logging.info('Create independent engine.')
    connection_uri = "mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}".format(**{
        "user": "root",
        "password": "mysql123456",
        "host": "localhost",
        "port": "3306",
        "dbname": "xinguan"
    })
    engine = db.create_engine(connection_uri)
    synchronize_qq_community(engine)
