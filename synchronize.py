# -*- coding: utf-8 -*-
import logging
import time
import akshare as ak
import sqlalchemy as db  # Version Check
import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from data import District, DistrictInfo, Base


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

    # District.__table__.create(engine)
    # DistrictInfo.__table__.create(engine)
    synchronize_district_data()