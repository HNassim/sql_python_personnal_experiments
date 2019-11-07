from sqlalchemy import create_engine, event, pool
from dotenv import load_dotenv
import pandas as pd
import pandas.io.sql as psql
from tqdm import tqdm
import os, glob, sys, csv
from io import StringIO
import logging
from columns import *
log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"),
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

load_dotenv()

postgres_credentials = {key : os.getenv(key) for key in
                       ['user',
                        'password',
                        'host',
                        'port',
                        'database']}


def get_engine(params=postgres_credentials, **kwargs):
    engine=create_engine('postgres://{user}:{password}@{host}:{port}/{database}'.format(**params), **kwargs)
    
    return engine


def read_sql_query(sql_query, chunksize=None):
    try:
        if chunksize is None:
            engine = get_engine()
        else :
            engine = get_engine(poolclass=pool.QueuePool,
                           max_overflow=10,
                           pool_size=int(5),
                           pool_recycle=10000,
                           pool_timeout=300,
                           execution_options={'stream_results': True, 
                                              'max_row_buffer':chunksize})
        log.debug("Reading {} table to dataframe for".format(sql_query))
        if chunksize is None:
            df_dwh = pd.read_sql_query(sql=sql_query, con=engine, chunksize=chunksize)
            return df_dwh
        else: 
            chunks = []
            for chunk in tqdm(pd.read_sql_query(sql_query, con=engine, chunksize = chunksize)):
                chunks.append(chunk)
            df_dwh = pd.concat(chunks)
            del chunks
            return df_dwh
    except Exception as e:
        log.exception("Exception {}".format(e))
        log.debug("Disposing engine")
        engine.dispose()
    finally:
        log.debug("Disposing engine")
        engine.dispose()


def execute_sql_query(sql_query):
    try:
        engine = get_engine()
        log.debug("Executing {} table to dataframe for".format(sql_query))
        engine.execute(sql_query)
    except Exception as e:
        log.exception("Exception {}".format(e))
        log.debug("Disposing engine")
        engine.dispose()
    finally:
        log.debug("Disposing engine")
        engine.dispose()
        
def transfer_sas_to_csv_helper(chunk, table):
    chunk.to_csv(os.path.join(os.getcwd(), '{}_{}.csv'.format(table, os.getpid()) ), 
                  index=False, mode='a+')
    
def erease_files(name):
    try:
        for f in glob.glob(name):
            os.remove(f)
    except FileNotFoundError:
        print("file doesn't exist")

def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)
        
        
def transfer_sas_to_sql(path, table, schema='usr_presta03', chunksize=10000, total=None, encodage='latin', subset_columns=None):
    execute_sql_query('DROP TABLE if exists {}.{};'.format(schema, table))
    try:
        engine = get_engine()
        for chunk in tqdm(pd.read_sas('{}/{}.sas7bdat'.format(path, table),
                                      encoding=encodage,
                                      chunksize=chunksize), 
                          total=int(total/chunksize)):
            if (subset_columns != None) or (subset_columns != []):
                chunk.to_sql('{}'.format(table), con=engine, schema=schema, if_exists='append', method=psql_insert_copy, index=False)
            else:
                chunk[subset_columns].to_sql('{}'.format(table), con=engine, schema=schema, if_exists='append', method=psql_insert_copy, index=False)
    except Exception as e:
        raise
    finally:
        engine.dispose()
        
        
def transfer_csv_to_sql(path, table, schema='usr_presta03', chunksize=10000, total=None):
    execute_sql_query('DROP TABLE if exists {}.{};'.format(schema, table))
    try:
        engine = get_engine()
        for chunk in tqdm(pd.read_csv('{}/{}.csv'.format(path, table),
                                      low_memory=False,
                                      error_bad_lines=False,
                                      warn_bad_lines=True,
                                      chunksize=chunksize), 
                          total=int(total/chunksize)):
            try:
                chunk.to_sql('{}'.format(table), con=engine, schema=schema, if_exists='append', method=psql_insert_copy, index=False)
            except Exception as e:
                print(e)
                continue
    except StopIteration:
        raise
    except Exception as e:
        print(sys.exc_info()[0])
        print(e)
        pass
    finally:
        engine.dispose()
        

def transfer_sql_to_sql(from_table, from_schema, to_table, to_schema='usr_presta03', chunksize=10000, total=None):
    execute_sql_query('DROP TABLE IF EXISTS {}.{};'.format(to_schema, to_table))
    try:
        engine = get_engine()
        sql = '''select {} from {}.{} t
            '''.format(columns.get(from_table, '*'), from_schema, from_table)
        pd.read_sql_query(sql+' limit  1;',
                                          con=engine).to_sql(
            '{}'.format(to_table), con=engine, schema=to_schema, method=psql_insert_copy, index=False)
        #execute_sql_query('CREATE TABLE {}.{} AS TABLE {}.{} with no data;'.format(to_schema, to_table, from_schema, from_table))
        execute_sql_query('TRUNCATE TABLE {}.{}'.format(to_schema, to_table))
        engine.dispose()
        
        engine = get_engine(poolclass=pool.QueuePool,
                           max_overflow=10,
                           pool_size=int(5),
                           pool_recycle=10000,
                           pool_timeout=300,
                           execution_options={'stream_results': True, 'max_row_buffer':chunksize}) #
        for chunk in tqdm(pd.read_sql_query(sql,
                                          con=engine,
                                          #parse_dates=parse_date.get(from_table, None),
                                          chunksize=chunksize), 
                              total=total if total is None else int(total/chunksize)):
            chunk.to_sql('{}'.format(to_table), con=engine, schema=to_schema, if_exists='append', method=psql_insert_copy, index=False)
    except Exception as e:
        print(sys.exc_info()[0])
        print(e)
        pass
    finally:
        engine.dispose()
        

def drop_and_copy_table(from_table, from_schema, to_table, to_schema='usr_presta03'):
    execute_sql_query('DROP TABLE IF EXISTS {}.{}'.format(to_schema, to_table))
    execute_sql_query('''CREATE TABLE {}.{} as TABLE {}.{};'''.format(to_schema, to_table, from_schema, from_table ))

    
def drop_and_create_table(sql, to_table, to_schema='usr_presta03'):
    execute_sql_query('DROP TABLE IF EXISTS {}.{}'.format(to_schema, to_table))
    execute_sql_query('''CREATE TABLE {}.{} as ({})'''.format(to_schema, to_table, sql))
    

def create_indexes(table, list_, schema='usr_presta03'):
    for item in list_:
        execute_sql_query('CREATE INDEX ON {}.{} ({});'.format(schema, table, item))
