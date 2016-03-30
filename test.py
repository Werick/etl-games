from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pandas import DataFrame
import datetime

# We are connecting to an existing service
dbconn = 'mysql://root:root@127.0.0.1:3306/ke'
engine = create_engine(dbconn, echo=False, pool_recycle=3600)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# And we want to query an existing table
tablename = Table('person', 
    Base.metadata, 
    autoload=True, 
    autoload_with=engine, 
    schema='ke')

# These are the "Where" parameters, but I could as easily 
# create joins and limit results
#us = tablename.c.country_code.in_(['US','MX'])
#dc = tablename.c.locn_name.like('%DC%')
#dt = tablename.c.arr_date >= datetime.date.today() # Give me convenience or...

#q = session.query(tablename).\
#            filter(us & dc & dt) # That's where the magic happens!!!
q = session.query(tablename)

def querydb(query):
    """
    Function to execute query and return DataFrame.
    """
    df = DataFrame(query.all());
    df.columns = [x['name'] for x in query.column_descriptions]
    return df

print querydb(q)