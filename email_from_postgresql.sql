---This email function requires a couple of tables to work.
--This creates an email based off table called batch,  then a related talbe has the instructions on how to create PDF.
--From the resulting file is then attached and email directly sent out to the destination.  It is also able to extract files stored
-- the database 

-- the key difference with this function it does not just send the email to a forwarding SMTP server
-- but looks up the MX records sends it to finial destination.
-- This way the Postgresql server can to its client immediately if the email has failed and the User can take
-- corrective action.. 

CREATE TYPE email_batch_result AS
(
	message text,
	status integer
);

CREATE OR REPLACE FUNCTION email_batch(
	pbatch_id integer)
    RETURNS email_batch_result
    LANGUAGE 'plpython3u'

    COST 100
    VOLATILE 
AS $BODY$

import dns.resolver, re, tempfile, os, sys, time
from smtplib import SMTP, SMTPConnectError, SMTPServerDisconnected, SMTPResponseException, SMTPSenderRefused, SMTPRecipientsRefused, SMTPDataError, SMTPHeloError
from string import Template
from email.mime.text import MIMEText
from email.utils import parseaddr
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email import encoders
from time import gmtime, strftime

global SMTP_CODES_RETRY, _status
SMTP_CODES_RETRY= [450, 451, 452, 441, 442, 432, 421, 101, 111, 1]
_status = 0  ##global varaible to keep track if function or other error has occured and send it back to the client

#define functions used by the driving script below these functions 
######################################################################################################################
def getServers_valid_Emails (pemails):  #this iterates over a coma delimted list of emails extracts the domain looks up the MX records
##the return type  is list of SMTP servers with list emails addresses for that domain,  
## the second return type is string of all the parsed emails,
## the third return type is string containing error messages and/or list of Bad Emails .
	global _status
	return_message = ''
	dirty_addresses = list(set(pemails.split(',')))
	addresses = [x.strip(' \t\n\r') for x in dirty_addresses]
	emailaddresses = []
	smtp_servers_and_to = []
	EMAIL_REGEX = re.compile("@[\w.]+")
	domains = list(set( [x.split('@')[1] for x in addresses if  EMAIL_REGEX.search(x)] ))
	for domain in domains :
		try :
			ans = sorted(dns.resolver.query(domain, 'MX')) ##hope this works have to sort the responses to weed out strange responses on invalid responds such as msXXXX.msv1.invalid 
			ans_server = str(ans[0].exchange)[0:-1]
			if ans_server != '0.0.0.0' or ans_server != '0:0:0:0:0:0:0:0' :
				ans.pop(0) ## remove the first SMTP server in the list so it not iterated over again in latter code
				smtp_servers_and_to.append( [ ans_server , [x for x in addresses if re.search(domain, x)], 
										[ str(bs.exchange)[0:-1] for bs in ans if bs] ])
				emailaddresses += [x for x in addresses if re.search(domain, x)]
		except dns.resolver.NXDOMAIN:
			return_message = return_message + "Failed not a valid Domain %s \r\n " % (domain)
			_status = -1
		except dns.resolver.NoAnswer :
			return_message = return_message + "Failed DNS no answer for domain %s \r\n " % (domain)
			_status = -1
		except dns.exception.Timeout :
			return_message = return_message + "Failed DNS timeout for domain %s \r\n " % (domain)
			_status = -1
		except :
			return_message = return_message + "Failed DNS other errors for domain %s \r\n " % (domain)
			_status = -1
	bad_emails = list(set(addresses) - set(emailaddresses))
	for be in bad_emails :
		if be.strip() : 
			_status = -1
			return_message += "Failed Malformed Emails or bad emails: " + be  + '\r\n'
	return smtp_servers_and_to, emailaddresses, return_message
										   
######################################################################################################################
def send_message(smtpserver, fromaddr, toaddr, msg):  #sends the message to a connected email server and discounnects from the server. 
#return types are string and complete communication log between this function and destination smtp server.
	global _status
	return_message = ''
	smessage = ''
	smtpserver.set_debuglevel(1)
	t = tempfile.TemporaryFile()
	available_fd = t.fileno()
	t.close()

	# now make a copy of stderr                                                                                                      
	os.dup2(2,available_fd)

	# Now create a new tempfile and make Pythons stderr go to that file                                                             
	t = tempfile.TemporaryFile()
	os.dup2(t.fileno(),2)
						
	try :
		smessage = smtpserver.sendmail(fromaddr, toaddr,  msg)
		return_message += " Success sent message to " + ','.join(sservers[1]) + "\r\n"
	except SMTPServerDisconnected as err:
		return_message += "Failed disconnected from SMTP server Domain %s \r\n " % (smtpserver._host)
		_status = -1
	except SMTPResponseException as err:
		return_message += "Failed got a respondes erre %s the server will retry in a few minutes \r\n " % (smtpserver._host)
		reschedule_email( toaddr, error_message = return_message, error_code = err.smtp_code )
	except SMTPSenderRefused as err:
		return_message += "Failed SMTP Server %s rejected the sender email address %s \r\n full error message: %s \r\n" % (smtpserver._host, err.sender, err)
		_status = -1
	except SMTPRecipientsRefused as err :
		return_message += "Failed the following emails %s  the server will retry in a few minutes \r\n " % (', '.join(err.recipients))
		reschedule_email( toaddr, error_message=return_message, error_code=1 )	
	except SMTPDataError as err :
		return_message += "Failed the STMP %s did not accept data transmitted \r\n " % (smtpserver._host)
		_status = -1
	except SMTPHeloError as err :
		return_message += "Failed SMTP %s failed on the Helo Command the server will retry in a few minutes \r\n " % (smtpserver._host)
		reschedule_email( toaddr )	
	except :
		return_message += "Unknown errors trying to send email for domain %s \r\n " % (smtpserver._host)
		_status = -1
	# Grab the stderr from the temp file  
	try :
		smtpserver.quit()
		time.sleep(0.5) ##added so the stderr has time to process the responds from the destination .  
		## if not parts of the logs were being dumped into the next log entry making it harder to read the logs.
	except :
		_status = -1	
		return_message += 'The SMTP Quit Command Failed Should have not have happened need to look at the log '
	sys.stderr.flush()
	t.flush()
	t.seek(0)
	smessage = t.read()
	t.close()
	# Put back stderr                                                                                                                
	os.dup2(available_fd,2)
	os.close(available_fd)
	return return_message, smessage.decode('utf-8')
																			  
																			  
######################################################################################################################
##this recreates the batch tuples in the database so the email can retried.  there is a PgAgent Task to retry messages
def reschedule_email(toaddr, retry_time = 300, error_message = '', error_code =1 ):
	global SMTP_CODES_RETRY, pbatch_id
	if error_code in SMTP_CODES_RETRY :
		newid = plpy.execute("select nextval('batch_batch_id_seq') as kid" )
		kid = newid[0]['kid']
		psql= """insert into batch  
			(select %s, 
				batch_action, 
				batch_parameter, 
				batch_user, 
				$emailad$%s$emailad$, --batch_email
				batch_submitted, 
				batch_scheduled +  interval '%s seconds',
				case when batch_started is null then
					clock_timestamp()
				else
					batch_started
				end,
			    null, --batch_completed 
				batch_responsebody, 
				batch_subject, 
				batch_filename,
				case when batch_exitstatus is null  then
			 		'Rescheduled '  || $error_mes$%s$error_mes$ 
			 	else
			 		batch_exitstatus || E'\r\nRescheduled '  || $error_mes$%s$error_mes$ 
			 	end ,
				batch_fromemail,
				batch_reschedinterval, 
				batch_cc, 
				batch_emailhtml,
				batch_replyto, 
				'', 
				batch_recurring_batch_id,
				batch_counts + 1
			from batch 
			where batch_id = %s);
			insert into batchparam (select nextval('batchparam_batchparam_id_seq'),
							%s, --batchparam_batch_id 
							batchparam_order,
							batchparam_name,
							batchparam_value,
							batchparam_type
							from batchparam where batchparam_batch_id = %s)
		""" % ( kid, toaddr, retry_time, error_message, error_message, kid, kid, kid)
		plpy.execute(psql)
																			  
																			  
######################################################################################################################
## this goes to the database loads the files stored in a table and returns that list files.  
## the files are put into MIMEApplication class and encoded int base64.  It assumes that the data is binary and 
## and MimeType is application/octet-stream.  At some point should extend this code to deal with other datatypes
## so the MimeType correctly matches the actual file contents.  Sending everything application/octet-stream should not cause issues

def getAttachments():
	rpg =plpy.execute("""select file_title, file_descrip, file_stream, file_type from 
		file where file_id in  (select baf_file_id from batch_attach_files 
		where baf_batch_id = %s)""" % (pbatch_id))
	if len(rpg) == 0 :
		return []
	attachments =[]
	for files in rpg :
		pa = MIMEApplication(files['file_stream'], 'application/octet-stream; name="%s.%s"' % (files['file_descrip'],files['file_type'] ) )  #, _encoder=encoders.encode_base64,)
		encoders.encode_base64(pa)
		pa.add_header('Content-Description', '%s.%s' % (files['file_descrip'],files['file_type'] ) )
		pa.add_header('Content-Disposition', 'attachment; filename="%s.%s";' % (files['file_descrip'],files['file_type'] ) )
		attachments.append(pa)
	return attachments
					  
					  
######################################################################################################################
## Just a basic message builder function.  Built it this way if the need araises to make even more complex messages
## having built this already as a function should cut down on refactoring the code in the future. 
def buildmessage(fromaddr='mail@magwerks.com', replyto='', toaddr='', subject='From Magwerks', body='no message defined', html=False):
	msg = MIMEMultipart()
	msg['From'] = fromaddr
	msg['Reply-To'] = replyto
	msg['To'] = toaddr
	msg['Subject'] = subject
	msg.add_header('Date', strftime( '%a, %d %b %Y %H:%M:%S +0000', gmtime()))
	msg.add_header('Message-Id', strftime( '<%Y%m%d%H%M%S@db-server.magwerks.com>', gmtime()))
	if html:
		msg.attach(MIMEText(body, 'html'))
	else:
		msg.attach(MIMEText(body, 'plain'))
	return msg

######################################################################################################################
## this opens connection to SMTP server return the SMTP sever object  this does some basic error code and 
## allows trying to switch the connection to TLS if that fails it goes back to unencrypted. 
def openserver(smtpserver, port=25, tls=False, backup_servers = [] ):
	global _status
	return_message = ''
	try :
		server = SMTP(smtpserver, port)
	except SMTPConnectError :
		for bs in backup_servers : ##if we have list of smtp severs to try.
			server, rm = openserver(bs, port, tls)
			if server is not None:
				return server, rm
		return_message += "Failed to connect destination SMTP Server  %s \r\n " % (smtpserver)
		_status = -1
		return None, return_message
	except :  ##this is here to catch general errors and try to test
		for bs in backup_servers : ##if we have list of smtp severs to try.
			server, rm = openserver(bs, port, tls)
			if server is not None:
				return server, rm
		return_message += "Failed to connect destination SMTP Server  %s \r\n " % (smtpserver)
		_status = -1
		return None, return_message
	if tls:
		try: 
			server.starttls()
			server.ehlo()
		except RuntimeError :
			return_message +=  "Failed to start TLS runtime error SSL support error  %s \r\n " % (smtpserver)
			server.quit()
			return openserver(smtpserver, port=25, tls=False )
		except SMTPHeloError :
			return_message +=  "Failed TLS did error by the ehol command failed revert  %s \r\n " % (smtpserver)
			server.quit()
			return openserver(smtpserver, port=25, tls=False )
		except :
			return_message += "Failed to connect SMTP Server using TLS revert to unencrpyted %s \r\n " % (smtpserver)
			server.quit()
			return openserver(smtpserver, port=25, tls=False )
	return server, return_message 
							
######################################################################################################################
#log the communication to the SMTP servers. 
def write_log (batch_id, server_log, email_message ) :  ##writes the emails to a log files 
	psql = """insert into emaillog values (%s, $slog$%s$slog$, $em$%s$em$, clock_timestamp(), default )""" % (batch_id, server_log, email_message)
	plpy.execute(psql)

######################################################################################################################
#Main driving  code that gets the above  functions processing. 
##Step 1 Get some data from the database

qsql= """select batch_action, batch_parameter, 
		batch_user, batch_email, batch_submitted, batch_scheduled,
		batch_completed, batch_responsebody, batch_subject, batch_exitstatus,
		batch_fromemail, batch_reschedinterval, batch_cc, batch_emailhtml,
		batch_replyto, batch_bcc from batch where batch_id = %s
""" % (pbatch_id)
rbatch = plpy.execute(qsql )

if len(rbatch) < 1 :
	return ["""Failed to find the email to send batch_id = %s """ % (pbatch_id), -1]

######################################################################################################################
##Step 2 Go over the emails and find the STMP servers to send mail to...)
emails = (rbatch[0]["batch_email"] + ', ' + rbatch[0]["batch_cc"]).replace(';',',')  ##change the character ; to a , 
smtp_servers, ToAddresses, return_message = getServers_valid_Emails(emails)
emails = (rbatch[0]["batch_bcc"]).replace(';',',')
smtp_serverBCC, BCCAddresses, return_message2 =getServers_valid_Emails(emails)

return_message += return_message2

######################################################################################################################
##step 3 create the Email Message;
msg = buildmessage(rbatch[0]['batch_fromemail'], rbatch[0]['batch_replyto'], ','.join(ToAddresses), 
				   rbatch[0]['batch_subject'], rbatch[0]['batch_responsebody'], rbatch[0]['batch_emailhtml'])
																					   
																					   
######################################################################################################################																			  
#step 4 create the report that this batch is for and create the PDF;
if rbatch[0]['batch_parameter'] != '' :
	psql = """select  report_to_pdf('%s', 
					array_agg(batchparam_name || '=' || replace(batchparam_value, ' ', '_' )), true) as rb 
					from  ( select batchparam_name, batchparam_value, batchparam_batch_id 
							from batchparam 
							where batchparam_batch_id = %s 
							order by batchparam_order
						 ) dd
				group by batchparam_batch_id """ % (rbatch[0]['batch_parameter'], pbatch_id)

	r_binary_report = plpy.execute(psql)

	for rb in r_binary_report :
		if rb['rb'] is not None :
			ra = MIMEApplication(rb['rb'], 'application/pdf; name="%s.pdf"' % (rbatch[0]['batch_parameter']) ) 
			encoders.encode_base64(ra)
			ra.add_header('Content-Description', '%s.pdf' % (rbatch[0]['batch_parameter']) )
			ra.add_header('Content-Disposition', 'attachment; filename="%s.pdf";' % (rbatch[0]['batch_parameter']) )
			msg.attach(ra)
																						
######################################################################################################################																			  
#Step 5 loop over the attchements and attach them 
for f in getAttachments() :
    msg.attach(f)

#convert the message to a string to send via SMTP
msg_as_string =  msg.as_string()

######################################################################################################################
#Step 6 loop over the validated email server and send the messages out

for sservers in smtp_servers :
	smtp, rm = openserver(sservers[0], backup_servers=sservers[2])
	if smtp is not None :
		rm , sm = send_message(smtp, rbatch[0]['batch_fromemail'],  sservers[1], msg_as_string)
		write_log(pbatch_id, sm, '')
	else :
		rm = rm + 'Did not send email(s) to ' + ','.join(sservers[1])
		reschedule_email( ','.join(sservers[1]), error_message = rm, error_code = 1 )						
	return_message += rm

######################################################################################################################
#Step 7 loop over the BCC servers and modify the message to blank out From, CC so end users do not see the BCC list
msg['To'] = 'undisclosed recipient'
msg_as_string =  msg.as_string()
for sservers in smtp_serverBCC :
	smtp, rm = openserver(sservers[0], backup_servers=sservers[2])
	if smtp is not None :
		rm , sm = send_message(smtp, rbatch[0]['batch_fromemail'],  sservers[1], msg_as_string)
		write_log(pbatch_id, sm, '')
	else :
		reschedule_email( ','.join(sservers[1]), error_message=return_message, error_code=1 )	
	return_message += rm

##return [return_message, _status]
return [return_message, 1]
$BODY$;
						    
CREATE OR REPLACE FUNCTION report_to_pdf(
	report_name text,
	rparams text[],
	return_bytea boolean)
    RETURNS bytea
    LANGUAGE 'plpython3u'

    COST 100
    VOLATILE 
AS $BODY$

import tempfile
import uuid
import os, fnmatch, pty, sys 
import subprocess 
import time
import platform

running_on = platform.system()
if "tempdir" not in GD:
	GD["tempdir"] =  tempfile.TemporaryDirectory()
tmppathdir = ''
if  running_on == 'Linux' :
	csql = """select  get_setting_value('Report_Path_UL') as path """ 
	tmppathdir = '/database/scripts'
else :
	csql = "select get_setting_value('Report_cmd_path') as path " 
	tmppathdir = GD["tempdir"].name

csql = csql + """ , get_setting_value('Report_databaseURL') as dburl,
			get_setting_value('Report_username') as usern,
			get_setting_value('Report_password') as pwd ;
			"""
command  = plpy.execute(csql)
##this is where teh report definition is stored can be 
_sql = "select report_source from report where lower(report_name) = lower($$%s$$) order by report_grade desc limit 1 " % (report_name)
_report = plpy.execute(_sql)

if len(_report) == 0:
	return -1
	plpy.error('Failed to Find the report in the database')

if "tempfilename" not in GD :
	GD["tempfilename"] =  tmppathdir+ '/' + uuid.uuid4().hex 
if "bashScript" not in GD :
	GD["bashScript"] =  tmppathdir+ '/' + uuid.uuid4().hex

# write the report out 
thefile = open( GD["tempfilename"] + '.xml', 'wt')
thefile.write(_report[0]["report_source"])
thefile.close()

args = [ command[0]['path'], 
		command[0]['dburl'], 
		'-username='+command[0]['usern'],
		'-passwd='+command[0]['pwd'],
		'-pdf',
		'-outpdf='+ GD["tempfilename"].replace('\\', '/') + '.pdf',
		'-close',
		 GD["tempfilename"].replace('\\', '/')+ '.xml'
		] + ['-param=' + str(x) for x in rparams  ]					  
plpy.notice(' '.join(args))

##getting to this to work on Linux was far harder than one would think
## the report engine require access to X11 enviroment to generate the report
## which Postgresql does not have access to .  instead of trying to figure out to give postgresql X11 acess
## decided to create python script that runs under another user that has the X11 enviroment
## it generates the report, stages it in file to be read later in the script
## put this process to sleep for 5 seconds to give the report to render a quick hack.  can rewrite this
## so it looks at the directory ever X seconds for its file then move on if X time elesape fail.
if  running_on == 'Linux' :	
	thebash = open( GD["bashScript"] + '.sh', 'wt')
	thebash.write('#!/bin/bash \n')
	thebash.write(' '.join(args))
	thebash.write('\nchmod 777 '+GD["tempfilename"] + '.pdf'  )
	thebash.close()
	os.chmod(GD["tempfilename"]+'.xml', 0o777)
	os.chmod(GD["bashScript"]+'.sh', 0o777)
	time.sleep(5)
else :	
	##this works on windows no issues as Postgresql User has
	##access to pretty much everything in Windows enviroment. 		       
	subprocess.run(args, shell=False)

##logic here is the function returns the name of the file or bytea stream 
rvalue = None
if return_bytea :
	returnF = open( GD["tempfilename"].replace('\\', '/') + '.pdf', 'r+b')
	returnF.seek(0)
	rvalue = returnF.read()
	returnF.close()
	os.remove(GD["bashScript"]+'.sh')
	os.remove(GD["tempfilename"]+'.xml')
	os.remove(GD["tempfilename"]+'.pdf')
	GD.pop("tempfilename")
	GD.pop("bashScript")
	GD["tempdir"].cleanup()				  
	GD.pop("tempdir")
	

return rvalue

$BODY$;
			       
CREATE OR REPLACE FUNCTION get_setting_value(
	pName text)
    RETURNS text
    LANGUAGE 'plpgsql'

    COST 100
    STABLE 
AS $BODY$
-- Copyright (c) 1999-2014 by OpenMFG LLC, d/b/a xTuple. 
-- See www.xtuple.com/CPAL for the full text of the software license.
DECLARE
   ALIAS FOR $1;
  _returnVal TEXT;
BEGIN
  SELECT set_value INTO _returnVal
    FROM settings
   WHERE set_name = pName;
  RETURN _returnVal;
END;
$BODY$;

CREATE TABLE batch
(
    batch_id serial primary key,
    batch_action text COLLATE pg_catalog."default",
    batch_parameter text COLLATE pg_catalog."default",
    batch_user text COLLATE pg_catalog."default",
    batch_email text COLLATE pg_catalog."default",
    batch_submitted timestamp without time zone,
    batch_scheduled timestamp without time zone,
    batch_started timestamp without time zone,
    batch_completed timestamp without time zone,
    batch_responsebody text COLLATE pg_catalog."default",
    batch_subject text COLLATE pg_catalog."default",
    batch_filename text COLLATE pg_catalog."default",
    batch_exitstatus text COLLATE pg_catalog."default",
    batch_fromemail text COLLATE pg_catalog."default",
    batch_reschedinterval character(1) COLLATE pg_catalog."default" NOT NULL DEFAULT 'N'::bpchar,
    batch_cc text COLLATE pg_catalog."default",
    batch_emailhtml boolean NOT NULL DEFAULT false,
    batch_replyto text COLLATE pg_catalog."default",
    batch_bcc text COLLATE pg_catalog."default",
    batch_recurring_batch_id integer,
    batch_counts integer DEFAULT 0,
    CONSTRAINT batch_batch_recurring_batch_id_fkey FOREIGN KEY (batch_recurring_batch_id)
        REFERENCES batch (batch_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

CREATE TABLE batchparam
(
    batchparam_id serial primary key,
    batchparam_batch_id integer,
    batchparam_order integer,
    batchparam_name text COLLATE pg_catalog."default",
    batchparam_value text COLLATE pg_catalog."default",
    batchparam_type text COLLATE pg_catalog."default",
    CONSTRAINT batchparam_batchparam_batch_id_fkey FOREIGN KEY (batchparam_batch_id)
        REFERENCES batch (batch_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE batch_attach_files
(
    baf_id serial primary key,
    baf_file_id integer,
    baf_file_order integer,
    baf_batch_id integer
);
CREATE TABLE emaillog
(
    el_batch_id integer,
    el_server_log text COLLATE pg_catalog."default",
    el_email_message text COLLATE pg_catalog."default",
    datatime timestamp without time zone DEFAULT now(),
    el_id integer serial primary key
);

CREATE TABLE public.file
(
    file_id serial primary key,
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
			       
CREATE TABLE public.settings
(
    set_id serial  primary key ,
    set_name text COLLATE pg_catalog."default" NOT NULL,
    set_value text COLLATE pg_catalog."default",
    set_module text COLLATE pg_catalog."default",
    set_meaningofvalue text COLLATE pg_catalog."default",
    
    CONSTRAINT metric_metric_name_key UNIQUE (metric_name)
);
