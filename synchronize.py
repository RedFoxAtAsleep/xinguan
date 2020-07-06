# -*- coding: utf-8 -*-
import logging
import time
from datetime import datetime, date, timedelta, timezone, tzinfo
import akshare as ak
import sqlalchemy as db  # Version Check
import pandas as pd
import numpy as np
from numpy import nan
import logging
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from data import District, DistrictInfo, Base
from table import t_district, t_district_info, metadata, t_daily
from sqlalchemy.sql import select
from urllib.error import HTTPError, URLError


def str2str(s='Mon Jul 06 11:06:15 2020', strp='%a %b %d %H:%M:%S %Y', strf='%a %b %d %H:%M:%S %Y'):
    return time.strftime(
        strf,
        time.strptime(
            s,
            strp)
    )


def str2struct(s, strp='%Y-%m-%d'):
    return time.strptime(s, strp)


def struct2str(struct=None, strf='%Y-%m-%d', local=True):
    # time.asctime(struct=time.localtime())
    if local:
        return time.strftime(strf, struct or time.localtime())
    else:
        return time.strftime(strf, struct or time.gmtime())


def stamp2struct(stamp=None, local=True):
    # time.localtime([secs])方法是将一个时间戳转换为当前时区的struct_time。secs参数未提供，则以当前时间为准。
    # time.gmtime([secs])方法是将一个时间戳转换为UTC时区（0时区）的struct_time。secs参数未提供，则以当前时间为准。
    if local:
        return time.localtime(stamp or time.time())
    else:
        return time.gmtime(stamp or time.time())


def struct2stamp(struct=None, local=True):
    if local:
        return time.mktime(struct or time.localtime())
    else:
        return time.mktime(struct or time.gmtime())


def str2stamp(s, strp='%a %b %d %H:%M:%S %Y', local=True):
    return struct2stamp(str2struct(s, strp), local)


def stamp2str(stamp=None, strf='%a %b %d %H:%M:%S %Y', local=True):
    # time.ctime(stamp=time.time()),
    return struct2str(stamp2struct(stamp, local), strf, local)


def synchronize_qq_community(engine, manner='init', endurance=100):
    """
    merge, init
    """
    if manner == 'init':
        logging.info('Synchronize District.')
        t_district.drop(engine, checkfirst=True)
        t_district.create(engine)
        covid_19_area_all_df = ak.covid_19_area_all()
        covid_19_area_all_df.to_sql(
            t_district.name,
            con=engine,
            if_exists='append',
            index=False,
        )

        logging.info('Synchronize DistrictInfo.')
        t_district_info.drop(engine, checkfirst=True)
        t_district_info.create(engine)
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
                            t_district_info.name,
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
                    t_district_info.name,
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


def synchronize_csse_daily(engine, manner='append', endurance=15):
    retry = 100
    conn = engine.connect()

    if manner == 'init':
        t_daily.drop(engine, checkfirst=True)
        t_daily.create(engine)

    sql = select([t_daily.c.report_day], distinct=True)
    r = conn.execute(sql)  # iterator
    r = list(r)
    r = r or ['2020-01-21']

    lte_pre = max(r, key=lambda x: str2struct(x, strp='%Y-%m-%d'))
    lte_pre = str2stamp(lte_pre, strp='%Y-%m-%d')
    lte_pre = datetime.fromtimestamp(lte_pre)

    lte_now = datetime.now()

    dfs = []
    while lte_pre <= lte_now:
        lte_pre += timedelta(days=1)

        # 获取数据
        report_day = struct2str(lte_pre.timetuple(), '%Y-%m-%d')
        df = None
        for j in range(retry):
            try:
                df = ak.covid_19_csse_daily(date=report_day)
                logging.info("成功WEB：{0}".format(report_day))
                time.sleep(0.1)
                break
            except HTTPError as e:
                raise Exception("未来数据当前不可得")
            except URLError as e:
                if j == retry - 1:
                    logging.info(e)
                    raise Exception("失败WEB：{0}".format(report_day))
                logging.info(e)
                logging.info("重试WEB：{0}".format(report_day))

        # 修改字段
        df.rename(columns={
            'Province/State': 'higher',
            'Country/Region': 'lower',
            'Last Update': 'last_update',
            'Confirmed': 'confirmed',
            'Deaths': 'dead',
            'Recovered': 'recovered'
        }, inplace=True)

        df.rename(columns={
            'Province/State': 'higher',
            'Country/Region': 'lower',
            'Last Update': 'last_update',
            'Latitude': 'latitude',
            'Longitude': 'longitude',
            'Confirmed': 'confirmed',
            'Deaths': 'dead',
            'Recovered': 'recovered'
        }, inplace=True)

        df.rename(columns={
            'FIPS': 'us_only_county_code_fips',
            'Admin2': 'us_only_county_name',
            'Province_State': 'higher',
            'Country_Region': 'lower',
            'Last_Update': 'last_update',
            'Lat': 'latitude',
            'Long_': 'longitude',
            'Confirmed': 'confirmed',
            'Deaths': 'dead',
            'Recovered': 'recovered',
            'Active': 'active',
            'Combined_Key': 'district'
        }, inplace=True)

        # 统一DataFrame结构
        df = df.reindex(columns=[c.name for c in t_daily.columns])

        # 处理空值
        df.fillna(value={
            'us_only_county_code_fips': 0,
            'us_only_county_name': 'null',
            'higher': 'null',
            'lower': 'null',
            'district': 'null',
            'latitude': 0,
            'longitude': 0,
            'confirmed': 0,
            'dead': 0,
            'recovered': 0,
            'active': 0,
        }, inplace=True)

        # 数据持久化
        dfs.append(df)
        if len(dfs) == endurance:
            for j in range(retry):
                try:
                    pd.concat(dfs).to_sql(
                        t_daily.name,
                        con=engine,
                        if_exists='append',
                        index=False
                    )
                    logging.info("成功DB：{0}".format(report_day))
                    time.sleep(0.1)
                    break
                except Exception as e:
                    if j == retry - 1:
                        raise Exception("失败DB：{0}".format(report_day))
                    logging.info(e)
                    logging.info("重试DB：{0}".format(report_day))
            logging.info(endurance)
            time.sleep(0.3)
            dfs = []
    for j in range(retry):
        try:
            pd.concat(dfs).to_sql(
                t_daily.name,
                con=engine,
                if_exists='append',
                index=False
            )
            logging.info("成功DB：{0}".format(report_day))
            time.sleep(0.1)
            break
        except Exception as e:
            if j == retry - 1:
                raise Exception("失败DB：{0}".format(report_day))
            logging.info(e)
            logging.info("重试DB：{0}".format(report_day))

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
    # synchronize_district_data(engine)
    synchronize_csse_daily()