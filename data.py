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


# orm: metadata/engine/session
# metadata

Base = declarative_base()


class District(Base):
    __tablename__ = 'district'
    __table_args__ = {'extend_existing': True}
    id = Column(Integer, Sequence('district_id_seq'), primary_key=True)
    province = Column(String(32), primary_key=True)
    city = Column(String(32), primary_key=True)
    district = Column(String(32), primary_key=True)

    def __repr__(self):
        return '<District(province={province};city={city};district={district})>'.format(**{
            'province': self.province,
            'city': self.city,
            'district': self.district
        })

# SQLAlchemy的MapperObject必须要配置主键
# 配置没有主键的MapperObject，联合主键，如所有的字段构成主键

# 在Sqlalchemy中使用联合主键
# https://zhuanlan.zhihu.com/p/106568544
# ORM配置 | 翻译
# https://www.osgeo.cn/sqlalchemy/faq/ormconfiguration.html
# ORM Configuration
# https://docs.sqlalchemy.org/en/13/faq/ormconfiguration.html#how-do-i-map-a-table-that-has-no-primary-key

# 联合主键定义方式一
# class District(Base):
#     __tablename__ = 'district'
#     __table_args__ = {'extend_existing': True}
#     province = Column(String(32), primary_key=True)
#     city = Column(String(32), primary_key=True)
#     district = Column(String(32), primary_key=True)
#
#     def __repr__(self):
#         return '<District(province={province};city={city};district={district})>'.format(**{
#             'province': self.province,
#             'city': self.city,
#             'district': self.district
#         })


# 联合主键定义方式二
# class District(Base):
#     __tablename__ = 'district'
#     __table_args__ = (
#         PrimaryKeyConstraint('province', 'city', 'district'),
#         {'extend_existing': True},
#     )
#     province = Column(String(32))
#     city = Column(String(32))
#     district = Column(String(32))
#
#     def __repr__(self):
#         return '<District(province={province};city={city};district={district})>'.format(**{
#             'province': self.province,
#             'city': self.city,
#             'district': self.district
#         })


class DistrictInfo(Base):
    __tablename__ = 'district_info'
    __table_args__ = {'extend_existing': True}
    column_not_exist_in_db = Column(Integer, primary_key=True)
    id = Column(Integer, Sequence('district_info_id_seq'), primary_key=True)
    province = Column(String(32))
    city = Column(String(32))
    district = Column(String(32))
    show_address = Column(String(64))
    full_address = Column(String(64))
    cnt_sum_certain = Column(Integer)  # -1: 表示有确诊但是确诊人数不详


def synchronize_district_data(manner='init', endurance=100):
    '''
    merge, init
    '''
    if manner == 'init':
        logging.info('Synchronize District.')
        covid_19_area_all_df = ak.covid_19_area_all()
        covid_19_area_all_df.to_sql(
            District.__tablename__,
            con=engine,
            if_exists='replace',
            index=True,
            index_label='id'
        )
        logging.info('Synchronize DistrictInfo.')
        # covid_19_area_detail_df = ak.covid_19_area_detail()
        # covid_19_area_detail_df.to_sql(
        #     DistrictInfo.__tablename__,
        #     con=engine,
        #     if_exists='replace',
        #     index=False,
        # )
        # session.execute(
        #     # Base.metadata.tables[DistrictInfo.__tablename__].delete()
        #     DistrictInfo.__table__delete()
        #
        # )
        DistrictInfo.__table__.drop(engine, checkfirst=True)

        dfs = []
        retry = 10
        for i, (province, city, district) in enumerate(covid_19_area_all_df.to_records(index=False)):
            covid_19_area_search_df = None
            for j in range(retry):
                try:
                    covid_19_area_search_df = ak.covid_19_area_search(province=province, city=city, district=district)
                    logging.info("成功WEB：{0},{1},{2}".format(province, city, district))
                    time.sleep(0.1)
                    break
                except Exception as e:
                    if j == retry - 1:
                        logging.info(e)
                        logging.info("失败WEB：{0},{1},{2}".format(province, city, district))
                    logging.info(e)
                    logging.info("重试WEB：{0},{1},{2}".format(province, city, district))

            dfs.append(covid_19_area_search_df)

            if len(dfs) == endurance:
                for j in range(retry):
                    try:
                        pd.concat(dfs).to_sql(
                            DistrictInfo.__tablename__,
                            con=engine,
                            if_exists='append',
                            index=False
                        )
                        logging.info("成功DB：{0},{1},{2}".format(province, city, district))
                        time.sleep(0.1)
                        break
                    except Exception as e:
                        if j == retry-1:
                            logging.info("失败DB：{0},{1},{2}".format(province, city, district))
                        logging.info("重试DB：{0},{1},{2}".format(province, city, district))
                        logging.info(e)
                logging.info(endurance)
                time.sleep(0.3)
                dfs = []
        for j in range(retry):
            try:
                pd.concat(dfs).to_sql(
                    DistrictInfo.__tablename__,
                    con=engine,
                    if_exists='append',
                    index=False
                )
                logging.info("成功DB：{0},{1},{2}".format(province, city, district))
                time.sleep(0.1)
                break
            except Exception as e:
                if j == retry - 1:
                    logging.info("失败DB：{0},{1},{2}".format(province, city, district))
                logging.info("重试DB：{0},{1},{2}".format(province, city, district))
                logging.info(e)
    if manner == 'merge':
        pass


# 同步数据周期，增量更新，全量更新
# 周期内数据缓存
# 网络问题、数据问题


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

    Base.metadata.create_all(engine)
    synchronize_district_data()








