-- Sample Code how to process a credit using Authorize.Net gateway in plpython enviroment 

CREATE TYPE xmag.cc_returns AS
(
	ccpay_id integer,
	message text,
	status text
);

CREATE OR REPLACE FUNCTION process_creditcard(
	pccardid integer,
	pcvv text,
	pamount numeric,
	psales_hd_id integer)
    RETURNS "cc_returns"
    LANGUAGE 'plpython3u'

    COST 100
    VOLATILE 
AS $BODY$

### CRITICAL NOTE,  with python verison <=3.3 the Authorize.Net had a bug in it
## where it relied on pip featured that was removed in later verions, this resulted in a crash
## if you want to run this code on early version of Python because you are using Postgresql < 10
## which was built against python 3.3 you have to downgrade the pip to a very early verison i do not recall the number
## ran into this when another developer updated PIP and credit card processing started crashing.. 
from authorizenet import apicontractsv1
from authorizenet.apicontrollers import createTransactionController

def avscode( pcode):  #reponse code lookup to human readable message.. 
	if pcode == 'A':
		return """A --The street address matched, but the postal code did not.""" 
	elif pcode == 'B':
		return """B -- No address information was provided. """
	elif pcode == 'E':
		return """E -- The AVS check returned an error."""
	elif pcode == 'G':
		return """G -- The card was issued by a bank outside the U.S. and does not support AVS."""
	elif pcode == 'N':
		return """N -- Neither the street address nor postal code matched."""
	elif pcode == 'P':
		return """P -- AVS is not applicable for this transaction."""
	elif pcode == 'R':
		return """R -- Retry — AVS was unavailable or timed out."""
	elif pcode == 'S':
		return """S -- AVS is not supported by card issuer."""
	elif pcode == 'U':
		return """U -- Address information is unavailable."""
	elif pcode == 'W':
		return """W -- The US ZIP+4 code matches, but the street address does not."""
	elif pcode == 'X':
		return """X -- Both the street address and the US ZIP+4 code matched."""
	elif pcode == 'Y':
		return """Y -- The street address and postal code matched."""
	elif pcode == 'Z':
		return """Z -- The postal code matched, but the street address did not."""
	else :
		return """unknown response from AVS """

def ccvResultCode(pcode): #Additional reponse code lookup to human readable message.. 
	if pcode == 'M':
		return ' CVV matched'
	elif pcode == 'N':
		return 'CVV did not match.'
	elif pcode == 'P':
		return 'CVV was not processed.'
	elif pcode == 'S':
		return ' CVV should have been present but was not indicated.'
	elif pcode == 'U':
		return 'The issuer was unable to process the CVV check.'
	else :
		return """unknown response from CVV Check """

if pamount <= 0 :
	plpy.erorr("Amount passed to function 0 or negative")
  
## the data is encrypted so need a key to decrpy this.  Anybody using this 
## code need to come up with better method than hard coding key in the function..
_key = 'This is the Encryption Key Goes Here '

auth_server = "https://certification.authorize.net/gateway/transact.dll"
auth_trans_key = " Authorize .net key"
auth_loginid = "Authorize.net id "

##testmode keys
#auth_trans_key = "TestMode Key"
#auth_loginid = "TestMode ID "

_sql = """SELECT ccard_active, ccard_type,
      formatbytea(decrypt(setbytea(ccard_number),   setbytea('%s'),'bf')) AS ccard_number,
      formatccnumber(decrypt(setbytea(ccard_number),setbytea('%s'),'bf')) AS ccard_number_x,
      formatbytea(decrypt(setbytea(ccard_name),     setbytea('%s'),'bf')) AS ccard_name,
      formatbytea(decrypt(setbytea(ccard_address1), setbytea('%s'),'bf')) AS ccard_address1,
      formatbytea(decrypt(setbytea(ccard_address2), setbytea('%s'),'bf')) AS ccard_address2,
      formatbytea(decrypt(setbytea(ccard_city),     setbytea('%s'),'bf')) AS ccard_city,
      formatbytea(decrypt(setbytea(ccard_state),    setbytea('%s'),'bf')) AS ccard_state,
      formatbytea(decrypt(setbytea(ccard_zip),      setbytea('%s'),'bf')) AS ccard_zip,
      formatbytea(decrypt(setbytea(ccard_country),  setbytea('%s'),'bf')) AS ccard_country,
      formatbytea(decrypt(setbytea(ccard_month_expired),setbytea('%s'),'bf')) AS ccard_month_expired,
      formatbytea(decrypt(setbytea(ccard_year_expired),setbytea('%s'), 'bf')) AS ccard_year_expired,
      custinfo.*, cntct_phone, cntct_email, cust_number, cust_name, cust_id
      FROM ccard
      JOIN custinfo ON (ccard_cust_id=cust_id)
      LEFT OUTER JOIN cntct ON (cust_cntct_id=cntct_id)
    WHERE (ccard_id=%s)"""% (_key, _key, _key, _key, _key, _key, _key, _key, _key, _key , _key, pccardid)

ccdata = plpy.execute(_sql)

##This is where we get items from the sales order to and item descriptions to start building a CC request 
_sql = """select sales_hd_id, sales_hd_number, part_number, part_descrip1, sales_qtyord,  sales_freight,
	sales_linenumber, sales_price,  sales_note, now()::text as datetime 
	from sales 
	left join parts on part_id = sales_part_id
	left join saleshead on saleshd_id = sales_hd_id 
	where sales_hd_id = %s """ % (psales_hd_id)

colines = plpy.execute(_sql)
ccpay_attempts = 0
attemptscount = plpy.execute("select coalesce(max(ccpay_order_number_seq),0) + 1 as count from ccpay where ccpay_ccard_id = %s and ccpay_order_number = '%s'" % (pccardid, colines[0]['sales_hd_number']))
if len(attemptscount) == 1 :
	ccpay_attempts  = attemptscount[0]['count']
else :
	ccpay_attempts  = 1

if len(ccdata) != 1:
	return ( [-1, 'Failed Could not Look Up the credit card in the database', 'DECLINED' ] ) 

if ccdata[0]['ccard_active'] == False:
	return ( [-1, 'Failed The selected card is not active', 'DECLINED' ] ) 
	
##build the request

# Create a merchantAuthenticationType object with authentication details
# retrieved from the constants file
merchantAuth = apicontractsv1.merchantAuthenticationType()
merchantAuth.name = auth_loginid
merchantAuth.transactionKey = auth_trans_key

# Create the payment data for a credit card
creditCard = apicontractsv1.creditCardType()
creditCard.cardNumber = ccdata[0]['ccard_number']
creditCard.expirationDate = ccdata[0]['ccard_year_expired'] + '-' + ccdata[0]['ccard_month_expired']
if pcvv:
	creditCard.cardCode = pcvv

# Add the payment data to a paymentType object
payment = apicontractsv1.paymentType()
payment.creditCard = creditCard

# Create order information
order = apicontractsv1.orderType()
order.invoiceNumber = "Sales_Order "+ colines[0]['cohead_number']
order.description = "MPI Supplies or Equipement"

# Set the customers Bill To address
customerAddress = apicontractsv1.customerAddressType()
name = ccdata[0]['ccard_name']
name = name.split(" ", 1)
customerAddress.firstName = name[0]
if len(name) == 2:
	customerAddress.lastName = name[1]
	
customerAddress.company = str(ccdata[0]['cust_name'])[0:50]
customerAddress.address = str(ccdata[0]['ccard_address1'])[0:60]
customerAddress.city = str(ccdata[0]['ccard_city'])[0:50]
customerAddress.state = str(ccdata[0]['ccard_state'])[0:60]
customerAddress.zip = str(ccdata[0]['ccard_zip'])[0:20]
customerAddress.country = str(ccdata[0]['ccard_country'])[0:60]

# Set the customers identifying information
customerData = apicontractsv1.customerDataType()
customerData.type = "individual"
customerData.id = ccdata[0]['cust_number']
customerData.email = ccdata[0]['cntct_email']

# Add values for transaction settings
duplicateWindowSetting = apicontractsv1.settingType()
duplicateWindowSetting.settingName = "duplicateWindow"
duplicateWindowSetting.settingValue = "300"
settings = apicontractsv1.ArrayOfSetting()
settings.setting.append(duplicateWindowSetting)

# setup individual line items
line_items = apicontractsv1.ArrayOfLineItem()
count = 1
for line in colines :
	count = count + 1
	line_item = apicontractsv1.lineItemType()
	if count == 30 :
		line_item.description = 'No More Detail Allowed ' + line['item_descrip1']
	else :
		line_item.description = line['part_descrip1']
	line_item = apicontractsv1.lineItemType()
	line_item.itemId = str(line['sales_linenumber'])
	line_item.name = line['part_number']
	line_item.quantity = line['sales_qtyord']
	line_item.unitPrice = line['sales_price']
	line_items.lineItem.append(line_item)
	if count == 30 :  #this is Authorize .Net hard limit.  yes had to process orders with more than 30 items... 
		break

# Create a transactionRequestType object and add the previous objects to it.
transactionrequest = apicontractsv1.transactionRequestType()
transactionrequest.transactionType = "authCaptureTransaction"
transactionrequest.amount = pamount
transactionrequest.payment = payment
transactionrequest.order = order
transactionrequest.billTo = customerAddress
transactionrequest.customer = customerData
transactionrequest.transactionSettings = settings
transactionrequest.lineItems = line_items

# Assemble the complete transaction request
createtransactionrequest = apicontractsv1.createTransactionRequest()
createtransactionrequest.merchantAuthentication = merchantAuth
createtransactionrequest.refId = colines[0]['cohead_number'] +'-'+str(ccpay_attempts)
createtransactionrequest.transactionRequest = transactionrequest
# Create the controller
createtransactioncontroller = createTransactionController(createtransactionrequest)
createtransactioncontroller.setenvironment("https://api2.authorize.net/xml/v1/request.api")
createtransactioncontroller.execute()

response = createtransactioncontroller.getresponse()
ccpay_status = ''
ccpay_type = 'C'
ccpay_auth_charge = 'C'
ccpay_order_number = colines[0]['cohead_number']
ccpay_r_avs = ''
ccpay_r_ordernumb = ''
ccpay_r_error = 'NULL'
ccpay_r_approved = 'DECLINED'
ccpay_r_code = ''
ccpay_message = ''
ccpay_yp_r_time = 'NULL' 
ccpay_r_ref = 'NULL'
ccpay_yp_r_tdate = 'NULL'
ccpay_r_tax = 0.00
ccpay_r_shipping = colines[0]['cohead_freight']
ccpay_yp_r_score = 'NULL'
ccpay_tranaction_datetime= 'NOW()'
ccpay_by_username = 'current_user'
ccpay_curr_id = '2'
ccpay_ccpay_id = 'NULL'
ccpay_source_type = 'A'
ccpay_source_id = str(pcohead_id)
ccpay_card_pan_trunc = 'NULL'
ccpay_card_type = ccdata[0]['ccard_type']
message = ''

if response is not None:
	# Check to see if the API request was successfully received and acted upon
	if response.messages.resultCode == "Ok":
		# Since the API request was successful, look for a transaction response
		# and parse it to display the results of authorizing the card
		if hasattr(response.transactionResponse, 'messages') is True:
			message = 'Successfully created processed with Transaction ID: %s' % (response.transactionResponse.transId) + str('\r\n')
			ccpay_status = 'C'
			message += 'AVS Respons: ' + avscode(response.transactionResponse.avsResultCode) + str('\r\n')
			ccpay_r_avs = avscode(response.transactionResponse.avsResultCode)
			ccpay_r_ordernumb =response.transactionResponse.transId
			ccpay_r_approved = 'APPROVED'
			ccpay_r_code = response.transactionResponse.authCode 
			ccpay_message = str(response.transactionResponse.messages.message[0].code)
			message += 'Message Code: '+ str(response.transactionResponse.messages.message[0].code) + str('\r\n')
			##ccpay_r_ref =  response.transactionResponse.responseCode.refTransId
			message += 'Transaction Response Code: %s' % response.transactionResponse.responseCode + str('\r\n')
			message += 'Description: %s' % response.transactionResponse.messages.message[0].description
		else:
			ccpay_status = 'D'
			cpay_r_approved = 'DECLINED'
			message = 'Failed Transaction.'+ str('\r\n') 
			if hasattr(response.transactionResponse, 'errors') is True:
				message += 'Error Code:  %s' % str(response.transactionResponse.errors.error[0].errorCode) + str('\r\n')
				message += 'Error message: %s' % response.transactionResponse.errors.error[0].errorText
	# Or, print errors if the API request wasnt successful
	else:
		ccpay_status = 'D'
		ccpay_r_approved = 'DECLINED'
		message = 'Failed Transaction.' + str('\r\n')
		if hasattr(response, 'transactionResponse') is True and hasattr(response.transactionResponse, 'errors') is True:
			message += 'Error Code: %s' % str(response.transactionResponse.errors.error[0].errorCode) + str('\r\n') 
			message += 'Error message: %s' % response.transactionResponse.errors.error[0].errorText + str('\r\n')
		else:
			message += 'Error Code: %s' % response.messages.message[0]['code'].text + str('\r\n')
			message += 'Error message: %s' % response.messages.message[0]['text'].text
		
else:
	ccpay_r_approved = 'DECLINED'
	ccpay_status = 'D'
	message = 'Failed no response from Authorize.Net'

## sure going to get yelled at not using params query. 
## not worried about that in this case all the data should have been
## santized long before we got this far.
_sql = "INSERT INTO ccpay values( default, " + str(pccardid) + ", " + str(ccdata[0]['cust_id']) + ", " + str(pamount) + ", "
_sql += "false, $$" + ccpay_status + "$$, $$" + ccpay_type + "$$, $$" + ccpay_auth_charge + "$$, $$" + ccpay_order_number +"$$, " + str(ccpay_attempts)
_sql += ", $$" + ccpay_r_avs + "$$, $$" + str(ccpay_r_ordernumb) + "$$, " + str(ccpay_r_error) + ", $$" + str(ccpay_r_approved) + "$$, $$" +str(ccpay_r_code)+ "$$, "
_sql += "$m$" +message+ "$m$," + ccpay_yp_r_time + ", " + ccpay_r_ref + ", " + ccpay_yp_r_tdate + ", " + str(ccpay_r_tax) + ", " + str(ccpay_r_shipping) + ", " 
_sql += ccpay_yp_r_score + ", " + ccpay_tranaction_datetime + ", " + ccpay_by_username + ", " + str(ccpay_curr_id) + ", null , "
_sql += "$$" + ccpay_source_type + "$$, " + str(ccpay_source_id) + ", " + ccpay_card_pan_trunc + ", $$" + ccpay_card_type + "$$) returning  ccpay_id as key"
					
_ccpay_r = plpy.execute(_sql)
return ( [_ccpay_r[0]['key'], message, ccpay_r_approved ] )

$BODY$;
CREATE TABLE public.ccpay
(
    ccpay_id integer serial primary key ,
    ccpay_ccard_id integer,
    ccpay_cust_id integer,
    ccpay_amount numeric(24,8) NOT NULL DEFAULT 0.00,
    ccpay_auth boolean NOT NULL DEFAULT true,
    ccpay_status character(1) COLLATE pg_catalog."default" NOT NULL,
    ccpay_type character(1) COLLATE pg_catalog."default" NOT NULL,
    ccpay_auth_charge character(1) COLLATE pg_catalog."default" NOT NULL,
    ccpay_order_number text COLLATE pg_catalog."default",
    ccpay_order_number_seq integer,
    ccpay_r_avs text COLLATE pg_catalog."default",
    ccpay_r_ordernum text COLLATE pg_catalog."default",
    ccpay_r_error text COLLATE pg_catalog."default",
    ccpay_r_approved text COLLATE pg_catalog."default",
    ccpay_r_code text COLLATE pg_catalog."default",
    ccpay_r_message text COLLATE pg_catalog."default",
    ccpay_yp_r_time timestamp without time zone,
    ccpay_r_ref text COLLATE pg_catalog."default",
    ccpay_yp_r_tdate text COLLATE pg_catalog."default",
    ccpay_r_tax text COLLATE pg_catalog."default",
    ccpay_r_shipping text COLLATE pg_catalog."default",
    ccpay_yp_r_score integer,
    ccpay_transaction_datetime timestamp without time zone NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone,
    ccpay_by_username text COLLATE pg_catalog."default" NOT NULL DEFAULT geteffectivextuser(),
    ccpay_curr_id integer DEFAULT basecurrid(),
    ccpay_ccpay_id integer,
    ccpay_source_type character(1) COLLATE pg_catalog."default",
    ccpay_source_id integer,
    ccpay_card_pan_trunc text COLLATE pg_catalog."default",
    ccpay_card_type text COLLATE pg_catalog."default"
    );
    
CREATE TABLE public.ccard
(
    ccard_id integer serial primary key,
    ccard_seq integer NOT NULL DEFAULT 10,
    ccard_customer_id integer NOT NULL, --link back to the customer table... 
    ccard_active boolean DEFAULT true,
    ccard_name bytea,
    ccard_address1 bytea,
    ccard_address2 bytea,
    ccard_city bytea,
    ccard_state bytea,
    ccard_zip bytea,
    ccard_country bytea,
    ccard_number bytea,
    ccard_debit boolean DEFAULT false,
    ccard_month_expired bytea,
    ccard_year_expired bytea,
    ccard_type character(1) COLLATE pg_catalog."default" NOT NULL,
    ccard_date_added timestamp without time zone NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone,
    ccard_lastupdated timestamp without time zone NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone,
    ccard_added_by_username text COLLATE pg_catalog."default" NOT NULL DEFAULT geteffectivextuser(),
    ccard_last_updated_by_username text COLLATE pg_catalog."default" NOT NULL DEFAULT geteffectivextuser(),
)
