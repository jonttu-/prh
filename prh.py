import requests, json
from datetime import date, timedelta
from pymongo import MongoClient


def main():
  client = MongoClient()
  companies_table = client.mydb.companies
  last_day = date.today() - timedelta(1)
  parameters = {'companyRegistrationFrom':last_day.isoformat()}
  r = requests.get("http://avoindata.prh.fi/tr/tv", params=parameters)
  for result in r.json()['results']:
    business_id = result['businessId']
    existing = companies_table.find({'business_id':business_id})
    if existing.count() != 0:
    	print "business id exists, skipping: "+business_id
    	continue
    r = requests.get("http://avoindata.prh.fi/tr/v1/"+business_id)
    business_info = r.json()['results'][0]
    address = business_info['addresses'][0]
    contact_info = dict(business_id=business_id, 
    	               name=business_info['name'], 
    	               street=address['street'],
    	               post_code=address['postCode'],
    	               city=address['city'],
    	               website=address['website']
    	               )
    db_id = companies_table.insert(contact_info)
    print 'added company with business id '+business_id

if __name__ == "__main__":
    main()