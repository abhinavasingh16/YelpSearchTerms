import sys
import os
import pandas as pd
import oauth2
import urllib
import urllib2
import json
import time

search_term = ""
location = ""
offset = 0
limit=20

CONSUMER_KEY = 'odNp_rpwywrN5Am1Jq_P6g'
CONSUMER_SECRET = 'OseBpF94SsUV-Wis9gJgQ_JiVMw'
TOKEN = 'UKDrPwJLLPf6wz7znIjiOwLHX57yTHh8'
TOKEN_SECRET = '5KH27TCJpkyVLbspMg0ezrWBla8'

main_columns = ['id','is_claimed','is_closed','name','image_url','url','mobile_url','phone','display_phone','review_count',
	'categories','distance','rating','rating_img_url','rating_img_url_small','rating_img_url_large','snippet_text','snippet_image_url','address','display_address','city',
	'state_code','postal_code','country_code','cross_streets','neighborhoods','geo_accuracy','coordinate',
	'menu_provider','menu_date_updated','reservation_url','eat24_url']

	#[u'city', u'display_address', u'geo_accuracy', u'neighborhoods', u'postal_code', u'country_code', u'address', u'coordinate', u'state_code']

deal_columns = ['biz_id','id','title','url','image_url','currency_code','time_start','time_end',
	'is_popular','what_you_get','important_restrictions','additional_restrictions']

deal_option_columns = ['deals.id','title','purchase_url','price','formatted_price','formatted_price','original_price',
						'formatted_original_price','is_quantity_limited','remaining_count']

gift_certificate_columns = ['biz_id','id','url','image_url','currency_code','unused_balances']

gift_certificate_option_columns = ['gift_certificates.id','price','formatted_price']

def search(std_url,payload):
	'''
	This method handles OAUTH and url signing for the yelp search API v2. 
	It inputs the std url and the arguments. This outputs the json response
	from the API.
	'''
	consumer = oauth2.Consumer(CONSUMER_KEY, CONSUMER_SECRET)
	
	oauth_request = oauth2.Request(method="GET", url=std_url, parameters=payload)
	oauth_request.update(
		{
			'oauth_nonce': oauth2.generate_nonce(),
			'oauth_timestamp': oauth2.generate_timestamp(),
			'oauth_token': TOKEN,
			'oauth_consumer_key': CONSUMER_KEY
		}
	)
	#print(std_url)
	token = oauth2.Token(TOKEN, TOKEN_SECRET)
	oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
	signed_url = oauth_request.to_url()

	conn = urllib2.urlopen(signed_url,None)
	try:
		response = json.loads(conn.read())
	finally:
		conn.close()
	return response


def process_location(json_,df,row):
	'''
	Takes the location dictionary from the json response
	and extracts the data from json to the main dataframe.
	'''
	for key in json_.keys():
		df[key][row] = str(json_[key])
	return df

def process_deals(json_,d_row,o_row,biz_id):
	'''
	Takes the deals dictionary and parses information and 
	puts into an interim deals dataframe as well as a interim options dataframe.
	'''
	deals_interim_df = pd.DataFrame(index=[d_row],columns=deal_columns)
	deals_interim_df['id'][d_row] = biz_id

	options_interim_df = pd.DataFrame(index=[o_row],columns=deal_option_columns)

	had_options = False
	for key in json_.keys():
		if key == "options":
			had_options = True
			#Takes the options dictionary and appends to interim options dataframe row by row
			for option in json_[key]:
				frames = [options_interim_df,process_deal_options(option,len(options_interim_df)+1,json_['id'])]
				options_interim_df = pd.concat(frames)
		else:
			deals_interim_df[key][d_row] = json_[key]

	if had_options == True:
		return deals_interim_df,options_interim_df
	return deals_interim_df,None

def process_deal_options(json_,o_row,deal_id):
	'''
	Takes the options dictionary and generates a 1 row dataframe which is appended to the 
	interim options dataframe in the process_deals function.
	'''
	options_interim_df = pd.DataFrame(index=[o_row],columns=deal_option_columns)
	options_interim_df['deals.id'][o_row] = deal_id
	for key in json_.keys():
		options_interim_df[key][o_row] = json_[key]
	return options_interim_df

def process_gifts(json_,d_row,o_row,biz_id):
	'''
	Takes the gift_certificates dictionary and parses the JSON
	and puts into a interim gifts dataframe as well as a interim 
	options dataframe.
	'''
	gifts_interim_df = pd.DataFrame(index=[d_row],columns=gift_certificate_columns)
	gifts_interim_df['id'][d_row] = biz_id

	options_interim_df = pd.DataFrame(index=[o_row],columns=gift_certificate_option_columns)

	had_options = False
	for key in json_.keys():
		if key == "options":
			#Takes the options dictionary and appends to interm options datafram row by row.
			for option in json_[key]:
				frames = [options_interim_df,process_gift_options(option,o_row,json_['id'])]
				options_interim_df = pd.concat(frames)
		else:
			gifts_interim_df[key][d_row] = str(json_[key])
	if had_options == True:
		return gifts_interim_df,options_interim_df
	return gifts_interim_df,None

def process_gift_options(json_,o_row,gift_id):
	'''
	Takes the options dictionary and generates a 1 row dataframe which is appended 
	to the interm options dataframe in the process_gifts function above.
	'''
	options_interim_df = pd.DataFrame(index=[o_row],columns=gift_certificate_option_columns)
	options_interim_df['gift_certificates.id'][o_row] = gift_id
	for key in json_.keys():
		options_interim_df[key][o_row] = str(json_[key])
	return options_interim_df

def main(cmmd):
	'''
	This is the main method which calls the YELP API and performs an ETL
	step to convert json to a dataframe and merges the dataframes by 
	wrangling the data.
	'''
	#Parse command line
	search_term = cmmd[1]
	location = cmmd[2]
	#print(len(cmmd))
	if len(cmmd) > 3:
		new_file_name = cmmd[3]
		json_dump = open(new_file_name,'w')
	else:
		json_dump = open('output.txt','w+')

	#Create json dump and set up API by getting number of search terms
	offset=0
	payload = {
				'term':search_term.replace(' ','+'),
				'location':location.replace(' ','+'), 
				'offset':offset,
				'limit': 20
			  }
	pre_response = search('http://api.yelp.com/v2/search',payload)
	number_of_terms = pre_response['total']
	json_dump.write("There are {0} {1} in {2}.\n".format(str(number_of_terms),search_term,location))
	json_dump.write(" ")
	
	#Initialize the pandas dataframe
	main_df = pd.DataFrame(index=range(number_of_terms),columns=main_columns)
	
	deals_df = pd.DataFrame(columns=deal_columns)
	deal_options_df = pd.DataFrame(columns=deal_option_columns)

	gifts_df = pd.DataFrame(columns=gift_certificate_columns)
	gift_options_df = pd.DataFrame(columns=gift_certificate_option_columns)

	#Start calling the API, extract data from json response and put into dataframe.
	while (offset < number_of_terms):
		payload['offset'] = offset
		try:
			time.sleep(1)
			response = search('http://api.yelp.com/v2/search',payload)
		except urllib2.HTTPError as error:
			print("Got a bad HTTP error")
			print(main_df)
			sys.exit()
		offset+=20
		main_row = 0;
		for biz_json in response['businesses']:
			json_dump.write(str(json))
			json_dump.write(" ")
			#print(biz_json['location'].keys())
			for key in biz_json.keys():
				if key == 'location':
					main_df = process_location(biz_json[key],main_df,main_row)
				elif key == 'deals':
					for deal in biz_json[key]:
						if (len(deals_df) == 0):
							interim_deals_df,interim_deal_options_df = process_deals(deal,0,0,biz_json["id"])
						else:
							interim_deals_df,interim_deal_options_df = process_deals(deal,len(deals_df)+1,len(deal_options_df)+1,biz_json["id"])
						deal_frames = [deals_df,interim_deals_df]
						if interim_deal_options_df is not None:
							option_frames = [deal_options_df,interim_deal_options_df]
							deal_options_df = pd.concat(option_frames)
						deals_df = pd.concat(deal_frames)
				elif key == 'gift_certificates':
					for gift in biz_json[key]:
						if len(gifts_df) == 0:
							interim_gifts_df,interim_gift_options_df = process_gifts(gift,0,0,biz_json["id"])
						else:
							interim_deals_df,interim_deal_options_df = process_gifts(gift,len(gifts_df)+1,len(gift_options_df)+1,biz_json["id"])
						gift_frames = [gifts_df,interim_gifts_df]
						if interim_gift_options_df is not None:
							option_frames = [gift_options_df,interim_gift_options_df]
							gift_options_df = pd.concat(option_frames)
						gifts_df = pd.concat(gift_frames) 
				else:
					main_df[key][main_row] = biz_json[key]
			main_row+=1
		print("Finished processing 20 json responses we have {0} responses to go.".format(number_of_terms-offset))
	
	print(main_df)
	#JOIN dataframes and convert data frame to csv for future usage. 

if __name__ == "__main__":
	main(sys.argv)




