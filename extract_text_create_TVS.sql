Below Functions extract texts from multiple file formats stored in Postgrsql database

CREATE OR REPLACE FUNCTION file_extract_(
	pfile_id integer,
	pfile_extension text)
    RETURNS text
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

  	begin 
  		--this just driver function to figure out which function to call
		if lower(pfile_extension) = 'pdf' then
			return xmag.file_extract_pdf_text(pfile_id);
		ELSIF  lower(pfile_extension) in ( 'doc', 'docx', 'rtf', 'odf', 'otf') then
			return xmag.file_extract_officedocs_text(pfile_id);
		ELSIF  lower(pfile_extension) in  ('txt', 'text') then
			return xmag.file_extract_plain_text(pfile_id);
		ELSIF  lower(pfile_extension) in  ('htm', 'html') then
			return xmag.file_extract_html_text(pfile_id);
		ELSIF  lower(pfile_extension) in  ('bmp', 'jpg', 'jpeg', 'png', 'tif') then 
			return xmag.file_extract_image_text(pfile_id);
		ELSIF  lower(pfile_extension) in ( 'xls', 'xlsx', 'xlsm', 'xlt', 'ods' , 'ots' ) then
			return xmag.file_extract_officedocs_text(pfile_id);
  		else
			return '';
		end if;
	end;

$BODY$;

CREATE OR REPLACE FUNCTION file_extract_html_text(
	pfile_id integer)
    RETURNS text
    LANGUAGE 'plpython3u'

    COST 100
    VOLATILE 
AS $BODY$

from bs4 import BeautifulSoup,  NavigableString

_record = plpy.execute("select file_stream, file_title, file_descrip, file_type from file where file_id = " + str(pfile_id))

if len(_record) == 0:
	return ''
postpage = BeautifulSoup(_record[0]['file_stream'], 'html.parser')

##this takes an html formated page converts it into  python list
text = postpage.findAll(text=lambda text:isinstance(text, NavigableString))
## take the list and turn it into a string.
text2 =	 u" ".join(text)
## take a string remove tabs newlines  carriage returns and other special characters put it into a list
## then it joins it into a string to be returned.  
text3 =  u" ".join(text2.split())
return _record[0]['file_title'] + _record[0]['file_descrip'] +' '+ _record[0]['file_type'] + ' ' +  text3

$BODY$;

CREATE OR REPLACE FUNCTION xmag.file_extract_image_text(
	pfile_id integer)
    RETURNS text
    LANGUAGE 'plpython3u'

    COST 100
    VOLATILE 
AS $BODY$

import fnmatch
import os
from PIL import Image
import pytesseract
from sys import platform
import tempfile
import time
import uuid

running_on = platform.system()
if  running_on == 'Linux' :
	spath ='/postgresql/'
	tesseract_p = '/path_to_/tesseract'
else :
	spath ='c:\\windows\\'
	tesseract_p  = 'c:\\path_to_\\tesseract.exe'

pytesseract.pytesseract.tesseract_cmd = tesseract_p

_record = plpy.execute("select file_stream, file_title, file_descrip, file_type from file where file_id = " + str(pfile_id))

if len(_record) == 0:
	return ''

tempdir = tempfile.TemporaryDirectory()

tempfilename = tempdir.name+ '/' + uuid.uuid4().hex + '.' + _record[0]['file_type']
# write the file out 
thefile = open(tempfilename, 'w+b')
thefile.write(_record[0]["file_stream"])
thefile.close()

pictext = pytesseract.image_to_string(Image.open(tempfilename))
												  
tempdir.cleanup()
return _record[0]['file_title'] + _record[0]['file_descrip'] +' '+ _record[0]['file_type'] + ' ' +  pictext
$BODY$;

CREATE OR REPLACE FUNCTION file_extract_officedocs_text(
	pfile_id integer)
    RETURNS text
    LANGUAGE 'plpython3u'

    COST 100
    VOLATILE 
AS $BODY$

import tempfile
import uuid
import os, fnmatch
import subprocess 
from sys import platform
import time

running_on = platform.system()

if  running_on == 'Linux' :
	spath ='/postgresql/'
	office_path = '\_LibreOffice_Path_UL\'
else :
	spath ='c:\\windows\\'
	office_path  = 'c:\\_LibreOffice_Path_UL.exe'
_record = plpy.execute("select file_stream, file_title, file_descrip, file_type from file where file_id = " + str(pfile_id))

if len(_record) == 0:
	return ''

tempdir = tempfile.TemporaryDirectory()
unique_name = uuid.uuid4().hex
tempfilename = tempdir.name+ '/' + unique_name + '.' + _record[0]['file_type']
# write the file out 
thefile = open(tempfilename, 'w+b')
thefile.write(_record[0]["file_stream"])
thefile.close()

args = [ office_path], 
		'--headless', 
		'--convert-to', 
		'txt',  tempfilename.replace('\\', '/'),
		'--outdir',
		tempdir.name.replace('\\','/')]

plpy.notice( args)
subprocess.call(args, shell=False)
#time.sleep(120)
thefile = open(tempdir.name+ '/' + unique_name + '.txt' , 'rt')
filetext = thefile.read()
thefile.close()				  
					  
tempdir.cleanup()
return _record[0]['file_title'] + _record[0]['file_descrip'] +' '+ _record[0]['file_type'] + ' ' +  filetext

$BODY$;

CREATE OR REPLACE FUNCTION xmag.file_extract_pdf_text(
	pfile_id integer)
    RETURNS text
    LANGUAGE 'plpython3u'

    COST 100
    VOLATILE 
AS $BODY$
import sys
import logging
import six
import pdfminer.settings
pdfminer.settings.STRICT = False
import pdfminer.high_level
import pdfminer.layout
from pdfminer.image import ImageWriter
import tempfile
import uuid
import time
import pytesseract
from PIL import Image
import os, fnmatch

def extract_text(files=[], outfile='-',
            _py2_no_more_posargs=None,  #Python2 needs a shim
            no_laparams=False, all_texts=None, detect_vertical=None, # LAParams
            word_margin=None, char_margin=None, line_margin=None, boxes_flow=None, # LAParams
            output_type='text', codec='utf-8', strip_control=False,
            maxpages=0, page_numbers=None, password="", scale=1.0, rotation=0,
            layoutmode='normal', output_dir=None, debug=False,
            disable_caching=False, **other):
    if _py2_no_more_posargs is not None:
        raise ValueError("Too many positional arguments passed.")
    if not files:
        raise ValueError("Must provide files to work upon!")

    # If any LAParams group arguments were passed, create an LAParams object and
    # populate with given args. Otherwise, set it to None.
    if not no_laparams:
        laparams = pdfminer.layout.LAParams()
        for param in ("all_texts", "detect_vertical", "word_margin", "char_margin", "line_margin", "boxes_flow"):
            paramv = locals().get(param, None)
            if paramv is not None:
                setattr(laparams, param, paramv)
    else:
        laparams = None

    imagewriter = None
    if output_dir:
        imagewriter = ImageWriter(output_dir)

    if output_type == "text" and outfile != "-":
        for override, alttype in (  (".htm", "html"),
                                    (".html", "html"),
                                    (".xml", "xml"),
                                    (".tag", "tag") ):
            if outfile.endswith(override):
                output_type = alttype
    import tempfile
    outfp = tempfile.SpooledTemporaryFile(0, 'w+b')

    p = open(outfile, "wb")
    for fname in files:
        with open(fname, "rb") as fp:
                pdfminer.high_level.extract_text_to_fp(fp, **locals())
        return outfp

_record = plpy.execute("select file_stream, file_title, file_descrip from file where file_id = " + str(pfile_id))

if len(_record) == 0:
	return ''

tempdir = tempfile.TemporaryDirectory()
plpy.notice(tempdir.name)
tempfilename = tempdir.name+ '/' + uuid.uuid4().hex+ '.pdf'

#plpy.notice(tempfilename)
tempfile = open(tempfilename, "w+b")
#write the contents from the DB into tempfile PDF miner does not allow passing file pointer in just name of the file to open.  it allot nicer to do this all in memory oh-welll
tempfile.write(_record[0]['file_stream'])
tempfile.close()
outfp = None
try:
	outfp = extract_text(files=[tempfilename], output_dir=tempdir.name )
except :
	return '' 
	
outfp.seek(0)

_text = outfp.read()
#plpy.notice(_text)

##extracte images from the pdf then extracting text from those it depends; this function depends on the tesseract.exe and pytesseract 

tesseract_p  = plpy.execute("select metric_value::TEXT as path FROM metric WHERE metric_name = 'Magwerks_Tesseract_cmd_path' ")
pytesseract.pytesseract.tesseract_cmd = tesseract_p[0]['path']

picext = ['*.jpg','*.bmp','*.gif','*.tif', '*.png', ]
pictext = ''
for root, dirs, files in os.walk(tempdir.name, topdown=True):
    for name in files:
        plpy.notice(' processing file_id: ' +str(pfile_id)+ ' filename = ' + name )
        for pattern in picext :
            if fnmatch.fnmatch(name, pattern):
                try:
                    pictext += pytesseract.image_to_string(Image.open(os.path.join(root, name)))
                except :
                    plpy.notice("failed to process image file: "+ name )
                #pictext += pytesseract.image_to_string(Image.open(root+"\\"+name))
                #plpy.notice(pictext)
tempdir.cleanup()
return _record[0]['file_title'] + _record[0]['file_descrip'] + str(_text) + pictext

$BODY$;

CREATE OR REPLACE FUNCTION xmag.file_extract_plain_text(
	pfile_id integer)
    RETURNS text
    LANGUAGE 'plpython3u'

    COST 100
    VOLATILE 
AS $BODY$


_record = plpy.execute("select file_stream, file_title, file_descrip, file_type from file where file_id = " + str(pfile_id))

if len(_record) == 0:
	return ''
	   
value = _record[0]['file_stream']
plpy.notice(value)
try:
	return value.decode('UTF-8')
except :
	return "" 
					   
$BODY$;


CREATE TABLE public.settings
(
    set_id serial  primary key ,
    set_name text COLLATE pg_catalog."default" NOT NULL,
    set_value text COLLATE pg_catalog."default",
    set_module text COLLATE pg_catalog."default",
    set_meaningofvalue text COLLATE pg_catalog."default",
    
    CONSTRAINT metric_metric_name_key UNIQUE (metric_name)
);

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
