# -*- coding: utf-8 -*-
import logging
import time
import akshare as ak
import sqlalchemy as db  # Version Check
from sqlalchemy import create_engine  # Connecting
from sqlalchemy import MetaData, Table, Sequence, Column, Integer, String, PrimaryKeyConstraint, ForeignKey  # Define and Create Tables
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

# logging.basicConfig()函数是一个一次性的简单配置工具使
logging.basicConfig(**{
    'level': logging.INFO,
    'format': "%(asctime)s - %(levelname)s - %(message)s",
    'datefmt': "%m/%d/%Y %H:%M:%S %p"
})


# orm: metadata(or Base)/engine/session
# metadata


metadata = MetaData()
'''
engine为空或者绑定
metadata是sqlalchemy.schema.MetaData的实例
declarative_base应用传入的MetaData实例或是新建MetaData实例

Base for ORM & Base Instance for SQL Expression

Base和MetaData一致：
metadata = MetaData()  # 新建
Base = declarative_base(metadata=metadata)  # 绑定

MetaData和Base一致：
Base = declarative_base()  # 没有绑定，新建
metadata = Base.metadata
'''

t_district = Table(
    'qq_community_u2d',
    metadata,
    Column('id', Integer, Sequence('district_id_seq'), primary_key=True),
    Column('province', String(32)),
    Column('city', String(32)),
    Column('district', String(32))
)


t_district_info = Table(
    'qq_community_district',
    metadata,
    Column('id', Integer, Sequence('district_info_id_seq'), primary_key=True),
    Column('province', String(32)),
    Column('city', String(32)),
    Column('district', String(32)),
    Column('show_address', String(64)),
    Column('full_address', String(64)),
    Column('cnt_sum_certain', Integer)  # -1: 表示有确诊但是确诊人数不详
)

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

    metadata.create_all(engine)
    # synchronize_district_data()











