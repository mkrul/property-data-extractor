from pymongo import MongoClient
import requests
import json

file = open("password.txt", "r")
password = file.read().strip()
connection_string = f"mongodb+srv://mkrul:{password}@cluster0-uadok.mongodb.net/test?retryWrites=true"

id = 0
param_data = {
    "parcelid": "",
    "debug[currentURL]": "",
}

while id < 200000:
    id += 1
    url = f"https://property.spatialest.com/nc/durham/#/property/{id}"
    file_url = f"https://5c5a99sucl.execute-api.us-east-1.amazonaws.com/Prod/durhamImageLister?filename={id}&size=1000x1000"
    param_data.update({
        "parcelid": id,
        "debug[contentURL]": url,
    })

    print("requesting data for property " + str(id))
    response = requests.post("https://property.spatialest.com/nc/durham/data/propertycard", data=param_data)
    
    response_string = response.content.decode("utf-8")

    if response_string == '{"found":false}':
        continue
    else:
        response_json = response.json()
        land_use = response_json["parcel"]["keyinfo"][5]["value"]
        if "1-FAMILY" in land_use:
            continue
        else:
            print("Multifamily property found!")
            img_url = None
            image_response = requests.post(file_url)
    
            if image_response.status_code == 200:
                img_url = image_response.content.decode("utf-8")[2:-2]

            try:
                location = response_json["parcel"]["header"]["location"]["value"]
            except:
                location = None

            try:
                owner_address = response_json["parcel"]["header"]["mailingaddress"]["value"].replace("<br/>", " ")
                owner_address_str = " ".join(owner_address.split())
            except:
                owner_address_str = None
                
            try:
                sale_price = response_json["parcel"]["tabs"]["Sales"]["result"][0]["row"][1]["value"].replace("$", "")
            except:
                sale_price = None
            
            try:
                last_sale_date = response_json["parcel"]["keyinfo"][10]["value"]
            except:
                last_sale_date = None

            try:
                land_fmv = response_json["parcel"]["assessment"][0]["value"].replace("$", "")
            except:
                land_fmv = None

            try:
                improvement_fmv = response_json["parcel"]["assessment"][1]["value"].replace("$", "")
            except:
                improvement_fmv = None

            try:
                total_fmv = response_json["parcel"]["assessment"][2]["value"].replace("$", "")
            except:
                total_fmv = None
           
            try:
                year_built = response_json["parcel"]["buildings"]["residential"][0]["display"][0]["value"]
            except:
                year_built = None
            
            try:
                style = response_json["parcel"]["buildings"]["residential"][0]["display"][1]["value"]
            except:
                style = None
            
            try:
                current_use = response_json["parcel"]["buildings"]["residential"][0]["display"][2]["value"]
            except:
                current_use = None
            
            try:
                sq_feet = response_json["parcel"]["buildings"]["residential"][0]["display"][4]["value"]
            except:
                sq_feet = None
            
            try:
                beds = response_json["parcel"]["buildings"]["residential"][0]["display"][7]["value"]
            except:
                beds = None
            
            try:
                full_baths = response_json["parcel"]["buildings"]["residential"][0]["display"][5]["value"]
            except:
                full_baths = None
            
            try:
                half_baths = response_json["parcel"]["buildings"]["residential"][0]["display"][6]["value"]
            except:
                half_baths = None
            
            try:
                fireplace = response_json["parcel"]["buildings"]["residential"][0]["display"][8]["value"]
            except:
                fireplace = None

            try:
                basement = response_json["parcel"]["buildings"]["residential"][0]["display"][9]["value"]
            except:
                basement = None
            
            try:
                unfinished_basement = response_json["parcel"]["buildings"]["residential"][0]["display"][10]["value"]
            except:
                unfinished_basement = None
            
            try:
                attached_garage = response_json["parcel"]["buildings"]["residential"][0]["display"][13]["value"]
            except:
                attached_garage = None
            
            try:
                acres = response_json["parcel"]["tabs"]["LandDetails"]["result"][0]["row"][2]["value"]
            except:
                acres = None
            
            try:
                y_coords = response_json["cty"]
            except:
                y_coords = None
            
            try:
                x_coords = response_json["ctx"]
            except:
                x_coords = None
            
            property_data = {
                "property_id": id,
                "img_url": img_url,
                "location": location,
                "owner_address": owner_address_str,
                "sale_price": sale_price,
                "last_sale_date": last_sale_date,
                "land_use": land_use,
                "land_fair_market_value": land_fmv,
                "improvement_fair_market_value": improvement_fmv,
                "total_fair_market_value": total_fmv,
                "year_built": year_built,
                "style": style,
                "current_use": current_use,
                "sq_feet": sq_feet,
                "beds": beds,
                "full_baths": full_baths,
                "half_baths": half_baths,
                "fireplace": fireplace,
                "basement": basement,
                "unfinished_basement": unfinished_basement,
                "attached_garage": attached_garage,
                "acres": acres,
                "y_coords": y_coords,
                "x_coords": x_coords,
            }

            client = MongoClient(connection_string)
            db = client["properties"]
            collection = db["properties"]
            collection.insert_one(property_data)


