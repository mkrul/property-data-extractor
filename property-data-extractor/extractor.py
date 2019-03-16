from pymongo import MongoClient
import requests
import json

data = {
  "parcelid": "100016",
  "card": "",
  "year": "",
  "debug[currentURL]": "https://property.spatialest.com/nc/durham/#/property/100016",
  "debug[previousURL]": ""
}

response = requests.post("https://property.spatialest.com/nc/durham/data/propertycard", data=data)
pretty_json = json.loads(response.content)

print(json.dumps(pretty_json, indent=2))

file = open("password.txt", "r")
password = file.read()

db_endpoint = f"mongodb+srv://mkrul:{password}@cluster0-uadok.mongodb.net/test?retryWrites=true"
client = MongoClient("mongodb+srv://mkrul:{password}@cluster0-uadok.mongodb.net/test?retryWrites=true")

