import requests, json, argparse, csv, pymongo
from datetime import date, timedelta


def persist():
  companies_collection = get_companies_collection()
  last_day = date.today() - timedelta(1)
  register_date = last_day.isoformat()
  parameters = {'companyRegistrationFrom':register_date}
  r = requests.get("http://avoindata.prh.fi/tr/tv", params=parameters)
  for result in r.json()['results']:
    business_id = result['businessId']
    existing = companies_collection.find({'business_id':business_id})
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
    	               website=address['website'],
    	               phone=address['phone'],
    	               register_date=register_date
    	               )
    db_id = companies_collection.insert(contact_info)
    print 'added company with business id '+business_id

def dump():
  companies_collection = get_companies_collection()
  filename = "out-%s.csv"%date.today().isoformat()
  f = csv.writer(open(filename, "wb+"))

  f.writerow(["pvm", "y-tunnus", "nimi", "osoite", "puhelin" , "url"])  
  for doc in companies_collection.find().sort('register_date', pymongo.ASCENDING):
    register_date = doc['register_date'] if doc['register_date'] else ''
    business_id = doc['business_id'] if doc['business_id'] else ''
    name = doc['name'] if doc['name'] else ''
    address = doc['street'] + ' ' +doc['post_code'] + ' ' +doc['city']
    phone = doc['phone'] if doc['phone'] else ''
    url =  doc['website'] if doc['website'] else ''
    f.writerow([
    	register_date.encode('utf-8'), 
    	business_id.encode('utf-8'), 
    	name.encode('utf-8'), 
    	address.encode('utf-8'), 
    	phone.encode('utf-8'), 
    	url.encode('utf-8')])

def get_companies_collection():
  client = pymongo.MongoClient()
  return client.mydb.companies

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Persists company information from PRH. Use option --dump to output information into csv file.')  
  parser.add_argument('-d','--dump', help='Dump company information into out-<date>.csv', action='store_true')

  args = parser.parse_args()
  if  not args.dump:
    persist()
  else :
    dump()