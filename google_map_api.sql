
CREATE OR REPLACE FUNCTION xmag.mapit(
	toaddr text,
	fromaddr text)
    RETURNS text
    LANGUAGE 'plpython3u'

    COST 100
    VOLATILE 
AS $BODY$

from datetime import datetime
import time
import urllib.request as url
import urllib.parse as parse

rsettings = plpy.execute("""select set_name, set_value from settings where 
    set_name in ('GoogleMap_API', 'GoogleMap_URL_Directions') order by 1;""")

if len(rsettings) == 0:
	return ("failed to find GOOGLE Map API Setting")
	
key = rsettings[0]['set_name']
url = rsettings[1]['set_name']

##directions_result = url.urlopen(url + parse.quote_plus(toaddr)  + '/' + parse.quote_plus(fromaddr) ).read()

return str(url + parse.quote_plus(fromaddr)   + '/' + parse.quote_plus(toaddr) )

$BODY$;


CREATE TABLE public.settings
(
    set_id serial  primary key ,
    set_name text COLLATE pg_catalog."default" NOT NULL,
    set_value text COLLATE pg_catalog."default",
    set_module text COLLATE pg_catalog."default",
    set_meaningofvalue text COLLATE pg_catalog."default",
    
    CONSTRAINT settings_set_namekey UNIQUE (set_name)
);

Insert into set value (default, 'GoogleMap_API', 'api key', 'system', 
'Path to share directly that Unix/Linux postgresql function and bulk upload files to the database')

Insert into set value (default, 'GoogleMap_URL_Directions', 'https://google_api.com/', 'system', 
'Path to share directly that Windows postgresql function and bulk upload files to the database')
