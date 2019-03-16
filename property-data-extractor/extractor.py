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
