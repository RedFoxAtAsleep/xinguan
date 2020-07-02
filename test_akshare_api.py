import urllib
import akshare as ak
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL
from sqlalchemy.orm import sessionmaker
# brew install unixodbc
# /usr/local/Cellar/unixodbc/2.3.7


covid_19_163_df = ak.covid_19_163(indicator="中国历史时点数据")
print(covid_19_163_df)

if __name__ == '__main__':

    connection_string = (
        'DRIVER=local;'
        'SERVER=120.78.123.249;'
        'PORT=3306;'
        'DATABASE=sakila;'
        'UID=root;'
        'PWD=mysql123456;'
        'charset=utf8mb4;'
    )

    connection_uri = "mysql+mysqldb://{user}:{password}@{host}:{port}/{dbname}".format(**{
        "user": "root",
        "password": "mysql123456",
        "host": "localhost",
        "port": "3306",
        "dbname": "Oracle_HR"
    })
    engine = create_engine(connection_uri, echo=True)
    Base = declarative_base()
    Session = sessionmaker(bind=engine)
    session = Session()

    class Job(Base):

        __tablename__ = 'JOBS'

        job_id = Column(String, primary_key=True, nullable=False)
        job_title = Column(String, nullable=False)
        min_salary = Column(DECIMAL)
        max_salary = Column(DECIMAL)

        def __repr__(self):
            return "<User(id={}, title={}, min={}, max={})>".format(
                self.job_id,
                self.job_title,
                self.min,
                self.max
            )

    for instance in session.query(Job).order_by(Job.job_id):
        print(instance)


