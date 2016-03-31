#this ian an example using SQLachemy to access Mysql db and build tables
#Update data in a table
#query data in a table
#This example also utilizes pandas python library to for data analaysis
#This expample uses python 2.7 and anaconda package management library

#Load SQLalchemy and Pandas libryary
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, DateTime, Text, Index, String, text
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pandas import DataFrame
import datetime

# We are connecting to an existing database
f = open("runtime.properties","r")
s = f.read()
dbconn = s
engine = create_engine(dbconn, echo=True, pool_recycle=3600)
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

def querydbRawSql(query):
    """
    Function to execute query and return DataFrame.
    """
    df = DataFrame(query.fetchall());
    for x in df:
        print df[x]
    #df.columns = [x['name'] for x in query.column_descriptions]
    return df

def createFlatObsTable():
    print "start creating table" 
    #Option 1
    etl_flat_obs = Table('etl_flat_obs',
        Base.metadata, 
        Column('person_id', Integer),
        Column('encounter_id', Integer, primary_key = True), #add primary key field constraint to a field in line
        Column('encounter_datetime', DateTime),
        Column('encounter_type', Integer),
        Column('location_id', Integer),
        Column('obs', Text),
        Column('obs_datetimes', Text),
        Column('max_date_created', DateTime, index = True), # add index to field inline
        #add an index to a single column
        Index('encounter_id', 'encounter_id'),
    
        #add an index to two or more columns
        Index('person_date', 'person_id', 'encounter_datetime'),
        Index('person_enc_id', 'person_id', 'encounter_id')
        )

    #create table using the session object
    etl_flat_obs.create(engine)
    #session.commit()

#print querydb(q)
#createFlatObsTable()

#Create table option 2 : Preffered method
class FlatObs(Base):
    __tablename__ = 'etl_flat_obs'
    # Here we define columns for the table etl_person
    # Notice that each column is also a normal Python instance attribute.
    encounter_id = Column('encounter_id', Integer, primary_key = True) #add primary key field constraint to a field in line
    encounter_datetime =    Column('encounter_datetime', DateTime)
    encounter_type =    Column('encounter_type', Integer)
    location_id =    Column('location_id', Integer)
    obs =    Column('obs', Text)
    obs_datetimes =    Column('obs_datetimes', Text)
    max_date_created =    Column('max_date_created', DateTime, index = True)

    Index('encounter_id', 'encounter_id')
    #add an index to two or more columns
    Index('person_date', 'person_id', 'encounter_datetime')
    Index('person_enc_id', 'person_id', 'encounter_id')

Base.metadata.create_all(engine)

def getFlatObsData():
    #geting data using raw sql
    sqlQuery = text ("select "
    "o.person_id, "
    "o.encounter_id, " 
    "encounter_datetime, "
    "encounter_type, "
    "e.location_id, "
    "group_concat( "
     "   case "  
      "when value_coded is not null then concat('!!',o.concept_id,'=',value_coded,'!!') "      
      "when value_numeric is not null then concat('!!',o.concept_id,'=',value_numeric,'!!') "      
      "when value_datetime is not null then concat('!!',o.concept_id,'=',date(value_datetime),'!!') "      
      "when value_boolean is not null then concat('!!',o.concept_id,'=',value_boolean,'!!') "      
      "when value_text is not null then concat('!!',o.concept_id,'=',value_text,'!!') "      
      "when value_drug is not null then concat('!!',o.concept_id,'=',value_drug,'!!') "      
      "when value_modifier is not null then concat('!!',o.concept_id,'=',value_modifier,'!!') "      
      "end "  
      "order by concept_id,value_coded "  
      "separator ' ## ' "  
    ") as obs, "
    "group_concat( "
        "case  "
            "when value_coded is not null or value_numeric is not null or value_datetime is not null or value_boolean is not null or value_text is not null or value_drug is not null or value_modifier is not null  "
            "then concat('!!',o.concept_id,'=',date(o.obs_datetime),'!!') "
        "end "
        " order by o.concept_id,value_coded "
        " separator ' ## '  "
    ") as obs_datetimes, "
    "max(o.date_created) as max_date_created "
    "from obs o  "   
        "join encounter e using (encounter_id) "
     "where o.encounter_id > 0 "
        "and o.voided=0 "
    "group by o.encounter_id"
        )
    sqlQuery = sqlQuery.columns(title=String)
    q = session.execute(sqlQuery)
    df = querydbRawSql(q);

    # print df
    return df

obsData = getFlatObsData()
# print obsData
# for index, row in obsData:
#     print row[0], index

# session.close()