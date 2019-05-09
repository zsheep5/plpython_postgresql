-- Python Function to figure out the best way to travel air or drive cost wise.
--this worked great until Google killed this API but it still works great to find the 
-- closes airport to your destination 

CREATE OR REPLACE FUNCTION map_distance(
	toaddr text,
	fromaddr text,
	transite_mode text)
    RETURNS TABLE(distance numeric, distance_by_car numeric, time_by_car numeric, 
    travel_time numeric, travel_expense numeric, dest_lat numeric, 
    dest_long numeric, flighttext text) 
    LANGUAGE 'plpython3u'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$

import googlemaps
from datetime import datetime
from datetime import timedelta
import json, re 
import requests
flighttext = ''
flighttime = 0
travel_time = 0
travel_expense = 0.0
trips = None
distance_by_car = 0.0
time_by_car = 0.0
_exit = False

rsettings = plpy.execute("select get_setting_value('GoogleMap_API') as googleapikey, 
    get_setting_value('Googel_Api_Directors_Url') as urldirections, 
    get_setting_value('My_Geo_lati') as lati,  
    get_setting_value('My_Geo_long') as long,  
    get_setting_value('My_Airport_Distance_Block')::float as add_to_airport, 
    get_setting_value('Day_Forward_look_for_AirTickets')::int as daysforward;")

if len(rsettings) == 0:
	plpy.error("failed to find GOOGLE Map API Setting")

gmaps = googlemaps.Client(key=rsettings[0]['googleapikey'])

ggeo = gmaps.geocode(address=toaddr)
if len(ggeo) ==0:
	plpy.error('Failed to Geo Locate the Destination')
dest_lati = ggeo[0]['geometry']['location']['lat']
dest_long = ggeo[0]['geometry']['location']['lng']



##Always load home to destination: 
gdm = gmaps.distance_matrix( origins =fromaddr, destinations =toaddr, 
			mode ='',  
			units = 'imperial', 
			traffic_model= 'pessimistic', 
			departure_time= datetime.now())
plpy.notice(gdm)
if  'ZERO_RESULTS' != gdm['rows'][0]['elements'][0]['status'] :
	#travel time in seconds need to convert to fractions of hours
	minutes, seconds  = divmod(gdm['rows'][0]['elements'][0]['duration']['value'], 60)
	hours, minutes = divmod(minutes, 60) 
	time_by_car  = hours + (minutes/60) # puting the duration into percent of hour
	if hours < 1 :
		travel_time = 1
	else :
		travel_time = hours + (minutes/60) # puting the duration into percent of hour
	dist =  gdm['rows'][0]['elements'][0]['distance']['value'] * 0.000621371
	distance_by_car = dist
	if dist < 10 :
		dist = 10
else :
	plpy.notice('addresses passed did not come back with a match failing')


###this no long works as this API was killed
def getflightdata(origin, destination, pdate):
	json_data ={
			  "request": {
			    "slice": [
			      {
				"origin": origin ,
				"destination": destination,
				"date": pdate.strftime('%Y-%m-%d')
			      }
			    ],
			    "passengers": {
			      "adultCount": 1,
			      "infantInLapCount": 0,
			      "infantInSeatCount": 0,
			      "childCount": 0,
			      "seniorCount": 0
			    },
			    "solutions": 1,
			    "refundable": False
			  }
			}
	plpy.notice(json.dumps(json_data, indent=4, sort_keys=True))
	reps =  requests.post('https://www.googleapis.com/qpxExpress/v1/trips/search?key='+rsettings[0]['googleapikey'], 
		json = json_data,
		verify = False )
	if reps.status_code != requests.codes.ok:
		plpy.error('Error code from trying to talked to Google: ' + str(reps.status_code) + ' Text from Google: ' + reps.text )
	return reps


if transite_mode == 'air' :
	sql_str = """select air_iata_code, air_name, air_type ,
			earth_distance(   ll_to_earth(%s, %s), ll_to_earth(air_lat, air_long ) ) * 0.000621371 as airport_distance, 
			air_lat, 
			air_long,
			earth_distance(   ll_to_earth(%s, %s), ll_to_earth(%s, %s ) )* 0.000621371 as car_distance
		from xmag.airports where air_scheduled_servie 
		and air_iata_code <> '' 
	order by 4 limit 5
	""" % (dest_lati, dest_long, dest_lati, dest_long, rsettings[0]['lati'], rsettings[0]['long'])
	plpy.notice(sql_str)
	close_airport = plpy.execute( sql_str )

	for ca in close_airport :  ##loop over the air ports 
		if ca['car_distance'] < (ca['airport_distance'] + rsettings[0]['add_to_airport']):  ##
			flighttext = 'Travel distance from airport to destination  (%s miles)  = + buffer: %s is greater than from origin '% (ca['airport_distance'], rsettings[0]['add_to_airport'])
			break
		else:
			trips = json.loads( getflightdata( "IND", ca['air_iata_code'], (datetime.now()+ timedelta(days=rsettings[0]['daysforward'])) ).text )
			if 'tripOption' in  trips['trips']:
				_exit =True
				plpy.notice(json.dumps(trips, indent=4, sort_keys=True))
				break 

	if _exit :
		for legs in trips['trips']['tripOption'][0]['slice'][0]['segment'] :
			flighttext += 'Departure on :' + legs['leg'][0]['departureTime'] + "\r\n"
			flighttext += 'Arriving on :' + legs['leg'][0]['arrivalTime'] + "\r\n"
			if 'operatingDisclosure' in legs['leg'][0]:
				flighttext +=  legs['leg'][0]['operatingDisclosure'] + "\r\n"
			flighttime += legs['leg'][0]['duration']  
			if 'connectionDuration' in legs:
				flighttime += legs['connectionDuration']
				
		travel_expense = float(re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", trips['trips']['tripOption'][0]['saleTotal'])[0])

		hours, minutes = divmod(flighttime,60)
		flighttext += 'Flight time include layovers = hours: ' + str(hours) + ' minutes: ' + str(minutes) + "\r\n"
		travel_time =  hours + (minutes/60) # puting the duration into percent of hour
		
		gdm = gmaps.distance_matrix( origins= str(close_airport[0]['air_lat']) +',' + str(close_airport[0]['air_long']) , 
				destinations =toaddr, 
				mode ='',  
				units = 'imperial', 
				traffic_model= 'pessimistic', 
				departure_time= datetime.now())
		plpy.notice(gdm)
		if  'ZERO_RESULTS' not in gdm :
			minutes, seconds  = divmod(gdm['rows'][0]['elements'][0]['duration']['value'], 60)
			hours, minutes = divmod(minutes, 60) 
		
			if hours < 2 :
				travel_time += 2
			else :
				travel_time += hours + (minutes/60) # puting the duration into percent of hour
			dist =  gdm['rows'][0]['elements'][0]['distance']['value'] * 0.000621371
			if dist < 10 :
				dist = 10
		else :
			dist = 0

return   [ {'distance':dist, 
		'distance_by_car' : distance_by_car,
		'time_by_car' : time_by_car,
		'travel_time':travel_time, 
		'travel_expense':travel_expense,
		'dest_lat': dest_lati,
		'dest_long': dest_long,
		'flighttext':flighttext
	} ] 

$BODY$;

CREATE OR REPLACE FUNCTION get_setting_value(
	pName text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    STABLE 
AS $BODY$
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

CREATE TABLE public.settings
(
    set_id serial  primary key ,
    set_name text COLLATE pg_catalog."default" NOT NULL,
    set_value text COLLATE pg_catalog."default",
    set_module text COLLATE pg_catalog."default",
    set_meaningofvalue text COLLATE pg_catalog."default",
    
    CONSTRAINT metric_metric_name_key UNIQUE (metric_name)
);

CREATE TABLE xmag.airports
(
    air_id integer NOT NULL DEFAULT nextval('xmag.airports_air_id_seq'::regclass),
    air_ident text COLLATE pg_catalog."default",
    air_type text COLLATE pg_catalog."default",
    air_name text COLLATE pg_catalog."default",
    air_lat numeric(20,10),
    air_long numeric(20,10),
    air_elevation integer,
    air_iso_country text COLLATE pg_catalog."default",
    air_iso_region text COLLATE pg_catalog."default",
    air_municipality text COLLATE pg_catalog."default",
    air_scheduled_servie boolean,
    air_gps_code text COLLATE pg_catalog."default",
    air_iata_code text COLLATE pg_catalog."default",
    air_local_code text COLLATE pg_catalog."default",
    air_weburl text COLLATE pg_catalog."default",
    CONSTRAINT airports_pkey PRIMARY KEY (air_id)
)
