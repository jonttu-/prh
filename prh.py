import requests, json
from datetime import date, timedelta
from pymongo import MongoClient


def main():
  client = MongoClient()
  db = client.mydb
  companies_table = db.companies
  this_day = date.today() - timedelta(1)
  companies_url = "http://avoindata.prh.fi/tr/tv"
  company_base_url = "http://avoindata.prh.fi/tr/v1/"
  parameters = {'companyRegistrationFrom':this_day.isoformat()}
  r = requests.get(companies_url, params=parameters)
  results = r.json()['results']
  for business in results:
    business_id = business['businessId']
    company_url = company_base_url + business_id
    r = requests.get(company_url)
    business_info = r.json()['results'][0]
    address = business_info['addresses'][0]
    contact_info = dict(businessId=business_id, 
    	               name=business_info['name'], 
    	               street=address['street'],
    	               postCode=address['postCode'],
    	               city=address['city'],
    	               website=address['website']
    	               )
    db_id = companies_table.insert(contact_info)

    print db_id

if __name__ == "__main__":
    main()