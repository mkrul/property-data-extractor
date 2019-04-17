from pymongo import MongoClient
import requests
import json

def main():
    PROPERTY_NOT_FOUND = '{"found":false}'
    PROPERTIES_DATABASE = "property-db"

    connection_string = get_connection_string()
    client = MongoClient(connection_string)
    db = client[PROPERTIES_DATABASE]
    collection = db["properties"]

    parcel_id = 100000
    param_data = {
        "parcelid": "",
        "debug[currentURL]": "",
    }
    
    while parcel_id < 300500:
        parcel_id += 1
        property_url = f"https://property.spatialest.com/nc/durham/#/property/{parcel_id}"
        param_data.update({
            "parcelid": parcel_id,
            "debug[contentURL]": property_url,
        })
    
        print("Requesting data for property " + str(parcel_id))
        response = requests.post("https://property.spatialest.com/nc/durham/data/propertycard", data=param_data)
        response_string = response.content.decode("utf-8")
    
        if response_string == PROPERTY_NOT_FOUND:
            continue
        else:
            print("Property found!")
            add_or_modify_document(collection, response, parcel_id)
 
def get_connection_string():
    file = open("password.txt", "r")
    password = file.read().strip() 
    connection_string = f"mongodb+srv://mkrul:{password}@cluster0-uadok.mongodb.net/test?retryWrites=true"
    return connection_string

def add_or_modify_document(collection, response, parcel_id):
    parcel_id_str = str(parcel_id)
    property = collection.find_one({ "parcel_id": parcel_id_str })
    response_data = response.json()
    property_data = get_property_data(response_data)
    if property:
        print("Updating parcel " + parcel_id_str)
        collection.update({ "_id": property["_id"] }, property_data)
    else:
        print("Adding parcel " + parcel_id_str)
        collection.insert(property_data)

def check_data(data):
    if data == "-":
        raise Exception

def get_property_data(response_data):
    parcel_id = response_data["id"] 
    photo_location = f"https://5c5a99sucl.execute-api.us-east-1.amazonaws.com/Prod/durhamImageLister?filename={parcel_id}&size=1000x1000"
    image_response = requests.post(photo_location)

    if image_response.status_code == 200:
        photo_url = image_response.content.decode("utf-8")[2:-2]
        check_data(photo_url)
    else:
        photo_url = None

    try:
        location = response_data["parcel"]["header"]["location"]["value"]
        check_data(location)
    except:
        location = None

    try:
        owner_address = response_data["parcel"]["header"]["mailingaddress"]["value"].replace("<br/>", " ")
        owner_address_str = " ".join(owner_address.split())
        check_data(owner_address_str)
    except:
        owner_address_str = None

    try:
        sale_price = response_data["parcel"]["tabs"]["Sales"]["result"][0]["row"][1]["value"].replace("$", "")
        check_data(sale_price)
    except:
        sale_price = None

    try:
        last_sale_date = response_data["parcel"]["keyinfo"][10]["value"]
        check_data(last_sale_date)
    except:
        last_sale_date = None

    try:
        land_use = response_data["parcel"]["keyinfo"][5]["value"]
        check_data(land_use)
    except:
        land_use = None

    try:
        land_use_code = response_data["parcel"]["keyinfo"][4]["value"]
        check_data(land_use_code)
    except:
        land_use_code = None
 
    try:
        subdiv_code = response_data["parcel"]["keyinfo"][6]["value"]
        check_data(subdiv_code)
    except:
        subdiv_code = None

    try:
        land_fmv = response_data["parcel"]["assessment"][0]["value"].replace("$", "")
        check_data(land_fmv)
    except:
        land_fmv = None

    try:
        improvement_fmv = response_data["parcel"]["assessment"][1]["value"].replace("$", "")
        check_data(improvement_fmv)
    except:
        improvement_fmv = None

    try:
        total_fmv = response_data["parcel"]["assessment"][2]["value"].replace("$", "")
        check_data(total_fmv)
    except:
        total_fmv = None

    try:
        year_built = response_data["parcel"]["buildings"]["residential"][0]["display"][0]["value"]
        check_data(year_built)
    except:
        year_built = None

    try:
        style = response_data["parcel"]["buildings"]["residential"][0]["display"][1]["value"]
        check_data(style)
    except:
        style = None

    try:
        current_use = response_data["parcel"]["buildings"]["residential"][0]["display"][2]["value"]
        check_data(current_use)
    except:
        current_use = None

    try:
        sq_feet = response_data["parcel"]["buildings"]["residential"][0]["display"][4]["value"]
        check_data(sq_feet)
    except:
        sq_feet = None

    try:
        beds = response_data["parcel"]["buildings"]["residential"][0]["display"][7]["value"]
        check_data(beds)
    except:
        beds = None

    try:
        full_baths = response_data["parcel"]["buildings"]["residential"][0]["display"][5]["value"]
        check_data(full_baths)
    except:
        full_baths = None

    try:
        half_baths = response_data["parcel"]["buildings"]["residential"][0]["display"][6]["value"]
        check_data(half_baths)
    except:
        half_baths = None

    try:
        fireplace = response_data["parcel"]["buildings"]["residential"][0]["display"][8]["value"]
        check_data(fireplace)
    except:
        fireplace = None

    try:
        basement = response_data["parcel"]["buildings"]["residential"][0]["display"][9]["value"]
        check_data(basement)
    except:
        basement = None

    try:
        unfinished_basement = response_data["parcel"]["buildings"]["residential"][0]["display"][10]["value"]
        check_data(unfinished_basement)
    except:
        unfinished_basement = None

    try:
        attached_garage = response_data["parcel"]["buildings"]["residential"][0]["display"][13]["value"]
        check_data(attached_garage)
    except:
        attached_garage = None

    try:
        acres = response_data["parcel"]["tabs"]["LandDetails"]["result"][0]["row"][2]["value"]
        check_data(acres)
    except:
        acres = None

    try:
        y_coords = response_data["cty"]
        check_data(y_coords)
    except: 
        y_coords = None

    try:
        x_coords = response_data["ctx"]
        check_data(x_coords)
    except:
        x_coords = None

    property_data = {
        "parcel_id": parcel_id,
        "img_url": photo_url,
        "location": location,
        "owner_address": owner_address_str,
        "sale_price": sale_price,
        "last_sale_date": last_sale_date,
        "land_use": land_use,
        "land_use_code": land_use_code,
        "subdiv_code": subdiv_code,
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

    return property_data

main()

