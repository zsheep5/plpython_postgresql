--function assums certain tables contain settings, and the files to upload/copy to the database are  
--in a directory that Postgresql Has Read and Delete rights.  the files are deleted after being copied
-- to the database. 
CREATE OR REPLACE FUNCTION file_load_from_directory(
	pff_id integer)
    RETURNS boolean
    LANGUAGE 'plpython3u'

    COST 100
    VOLATILE 
AS $BODY$
import datetime
import hashlib 
import os
import platform

running_on = platform.system()
if  running_on == 'Linux' :
	spath =plpy.execute ("""select  fetchmetrictext('File_Path_Postgresql_UL') as path, current_user""" )
else :
	spath =plpy.execute ("""select fetchmetrictext('File_Path_Postgresql') as path, current_user """ )

returned = True 
#this assumes the upload directory structure is SharedDir/PostgresUsers/Upload
thefile = None
bpath = spath[0]['path']+ '/' +spath[0]['current_user'] +'/upload' 
#plpy.notice(bpath)
for files in os.listdir(bpath):
	timestampme = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d time %H:%M:%S.%f')
	try :	
		#start the process of opening the file create hash to see if the file was changed from the one in the database
		thefile = open(bpath + '/' + files , 'r+b')
	except :
		message = "File: " + files + " location: " +bpath + " Is locked as of " + timestampme
		#plpy.notice( message ) 
		lcsql = "Insert into file_log values (default, default, $message$" + message + "$message$)";
		plpy.execute(lcsql)
		return False 
		
	thefile.seek(0)
	#update the file with the new file
	
	plan = plpy.prepare("""Insert into file (file_id, file_title, file_stream, file_descrip, file_type, file_ff_id ) 
		values(
			default, $1,  $2, $3, $4, $5
		)
	 	returning file_id as theid """, ['text', 'bytea', 'text', 'text', 'integer'])
	result = plpy.execute(plan, [files, thefile.read(),  files,  files[-3:], pff_id] )
	
	#insert into the log file
	if len(result) > 0:
		lcsql = ''
		message = "File_id: " + str(result[0]["theid"]) + " File Name: " + files 
		message = message + " Was added by user current_user on Date:" + timestampme
		if pff_id >= 0 :
			lcsql = "Insert into  file_log values (default, default, $message$" + message + "$message$ )";
		else :
			lcsql = "Insert into docass values (default, %s, 'I', %s, 'FILE', 'S', 0); " % (pff_id, result[0]["theid"])
			lcsql = lcsql + " Insert into file_log values (default, default, $message$" + message + "$message$ )";
		##lpy.error(lcsql) 
		plpy.execute(lcsql)
		
		#close and delete the file
		thefile.close()
		os.remove(bpath + '/' + files)
	
return True 

$BODY$;

CREATE TABLE public.file
(
    file_id integer serial primary key,
    file_title text COLLATE pg_catalog."default" NOT NULL,
    file_stream bytea,
    file_descrip text COLLATE pg_catalog."default" NOT NULL,
    file_type text COLLATE pg_catalog."default",
    file_checkedout boolean NOT NULL DEFAULT false,
    file_ff_id integer NOT NULL DEFAULT 0,
    file_ver integer DEFAULT 0,
    file_date_created timestamp with time zone DEFAULT now(),
    file_hashkey text COLLATE pg_catalog."default",
    file_tsvector tsvector,
    file_sort_order integer DEFAULT 100
);

CREATE TABLE file_log
(
    fl_id serial primary key,
    fl_datetime timestamp without time zone DEFAULT now(),
    fl_logdetails text COLLATE pg_catalog."default"
);

CREATE TABLE public.settings
(
    set_id serial  primary key ,
    set_name text COLLATE pg_catalog."default" NOT NULL,
    set_value text COLLATE pg_catalog."default",
    set_module text COLLATE pg_catalog."default",
    set_meaningofvalue text COLLATE pg_catalog."default",
    
    CONSTRAINT metric_metric_name_key UNIQUE (metric_name)
);

CREATE TABLE filefolders
(
    ff_id serial primary key,
    ff_name text COLLATE pg_catalog."default",
    ff_parent_id integer NOT NULL DEFAULT 0,

    CONSTRAINT filefolders_ff_name_ff_parent_id_key UNIQUE (ff_name, ff_parent_id)
);

Insert into set value (default, 'File_Path_Postgresql_UL', '/home/postgresq/files_to_load/', 'system', 
'Path to share directly that Unix/Linux postgresql function and bulk upload files to the database')

Insert into set value (default, 'File_Path_Postgresql', 'c:\\postgresql\\share\\files_to_load\\', 'system', 
'Path to share directly that Windows postgresql function and bulk upload files to the database')
