import psycopg2
import psycopg2.extras##import os, tempfile, io
import  multiprocessing as mp
import datetime

t= datetime.datetime.now()
_constring="dbname='Magwerks' user='' host='localhost' password='' sslmode='disable'"

_sql_create_t =  """ drop table if exists TempNestview; create unlogged table  TempNestview ( 
    nitem_id integer, 
    xtindentrole integer,
    nview_id integer
    );"""

_sql_nestlist = """set statement_timeout to 0; commit; 
                                select xmag.nestcitemlist_3(%s, %s, %s, %s);
                select * from TempNestview;"""


_sql_get_headers =  """
    select item_id, item_number, split_part(item_number, '-', 1) as item_key, 
        split_part(item_number, '-', 1)::integer * 100000  as sort_key,
        false as processed
                from item 
        where item_classcode_id = 48  
        order by string_to_array( REGEXP_REPLACE(item_number, '[^0-9-]+', '0', 'g'), '-')::int[], item_number
"""



def buildnest(item_key , xtindentrole, item_id, sort_key, _constring,  _nestlist ):
    t= datetime.datetime.now()
    #print('starting to process %s'% item_key)
    _pgcon = psycopg2.connect(_constring)
    _pgcon.autocommit = True
    _pgcur = _pgcon.cursor()
    _pgcur.execute("insert into TempNestview values ( %s, %s, %s )", (item_id, 0, sort_key) )
    _pgcur.execute(_nestlist, (item_key, xtindentrole, item_id, sort_key) )
    _results = _pgcur.fetchall()
    _pgcon.close()
    print('Finished Processing parent %s, time to complete %s'%(item_key, (datetime.datetime.now() - t)) )

    return _results

def call_back (_results):
    global bb
    bb += _results.get()

