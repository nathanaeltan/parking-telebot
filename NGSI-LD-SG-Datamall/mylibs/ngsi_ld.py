import mylibs.constants as constants
from landtransportsg import Traffic
import requests
import urllib.parse

import json
from requests.exceptions import RequestException, HTTPError
from ngsildclient import Client, Entity, SmartDataModels
from datetime import datetime


API_KEY = constants.DATAMALL_API_KEY
ctx = constants.ctx
broker_url = constants.broker_url
broker_port = constants.broker_port  # default, 80
temporal_port = constants.temporal_port  # default 1026
broker_tenant = constants.broker_tenant

# Convert to NGSI-LD
"""
Carpark
- id - Core context
- DevelopmentName (From LTA)
- Region (From LTA)
- Location - Gprop
- Price (Pending Terrence)
- ParkingAvalibility - From SDM
- ParkingChargeType - From SDM (Pending Terrence)
- ParkingMaxAvalibility - From SDM (Info not avaliable)
- DataSource - From SDM
- ParkingSiteOwner (relationship) - From SDM


Example LTA data return:
            "CarParkID": "1",
            "Area": "Marina",
            "Development": "Suntec City",
            "Location": "1.29375 103.85718",
            "AvailableLots": 442,
            "LotType": "C",
            "Agency": "LTA"
"""

#Batch size controls how many entities we upload at a time. Small onbjects can use a larger batch size to increase speed
def create_entities_in_broker(entities, batch_size=100):
    with Client(hostname=broker_url, port=broker_port, tenant=broker_tenant, port_temporal=temporal_port) as client:
        count = 0
        failed = 0
        # Split the entities list into chunks of the given batch size
        for i in range(0, len(entities), batch_size):
            chunk = entities[i:i+batch_size]  # Get a chunk of the specified batch size
            ret = client.upsert(chunk)  # Upsert the chunk

            if ret:
                count += len(ret.success)
                if len(ret.errors)>0:
                    warnings.warn("Some entities have failed to upload")
                    failed += len(ret.errors)
                    #print(ret.errors)

        print("Uploaded: ", count)
        print("Failed: ", failed)
        return (failed>0)


def update_entities_in_broker(entities):
    with Client(
        hostname=broker_url,
        port=broker_port,
        tenant=broker_tenant,
        port_temporal=temporal_port,
    ) as client:
        ret = client.upsert(entities)
    print("Update ", ret)
    return ret


def retrieve_ngsi_type(input_type: str):
    with Client(
        hostname=broker_url,
        port=broker_port,
        tenant=broker_tenant,
        port_temporal=temporal_port,
    ) as client:
        entities = client.query(
            type=input_type, ctx=ctx
        )  # Query for all type of carpark
        print("Number of entities retrieved: ", len(entities))
        for entity in entities:
            print(entity.id)
    return entities

    """
def retrieve_entity_from_json_file(output_file=constants.cache):
    entity_list: list[Entity] = Entity.load(output_file)
    print("\n\n")
    #print(carpark_list)
    print("Number of entities received:  " , len(entity_list))
    return entity_list
    """


def retrieve_entity_from_json_file(output_file=constants.cache):
    try:
        entity_list = Entity.load(output_file)
    except Exception as e:
        print(f"Failed to load entities from {output_file}: {e}")
        return []

    print("\n\n")
    print("Number of entities received:", len(entity_list))
    return entity_list


def geoquery_ngsi_long(
    input_type: str,
    geoquery: str,
    broker_url=broker_url,
    broker_tenant=broker_tenant,
    ctx=ctx,
):

    url = f"http://{broker_url}/api/broker/ngsi-ld/v1/entities/?type={input_type}&{geoquery}"

    payload = {}
    headers = {
        "NGSILD-Tenant": broker_tenant,
        "fiware-servicepath": "/",
        "Link": f'<{ctx}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"',
    }
    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)

    # Save the response to a file as a JSON array
    try:
        data = json.loads(response.text)
        if not isinstance(data, list):
            data = [data]
        with open(output_file, "w") as file:
            json.dump(data, file, indent=2)
        print(f"Response saved to {output_file}")
    except json.JSONDecodeError as e:
        print("Failed to parse JSON response:", e)
        print("Response text:", response.text)

    return retrieve_entity_from_json_file(output_file)


# Documentation
# https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.01.01_60/gs_cim009v010101p.pdf
# 4.10 NGSI-LD Geo-query language
def geoquery_ngsi_point(
    input_type: str,
    maxDistance: int,
    lat: float,
    long: float,
    output_file=constants.cache,
    broker_url=broker_url,
    broker_tenant=broker_tenant,
    ctx=ctx,
):

    geometry = "Point"

    # URL encode the coordinates
    # encoded_coordinates = urllib.parse.quote(f"[{lat},{long}]")
    encoded_coordinates = urllib.parse.quote(f"[{long},{lat}]")

    # Construct the geoquery string
    georel = f"near%3BmaxDistance=={maxDistance}"
    geoquery = f"geometry={geometry}&georel={georel}&coordinates={encoded_coordinates}"

    # Construct the full URL
    url = f"http://{broker_url}/api/broker/ngsi-ld/v1/entities/?type={input_type}&{geoquery}&limit=100"
    payload = {}
    headers = {
        "NGSILD-Tenant": broker_tenant,
        "fiware-servicepath": "/",
        "Link": f'<{ctx}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"',
    }

    print(url)
    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)

    # Save the response to a file as a JSON array
    try:
        data = json.loads(response.text)
        if not isinstance(data, list):
            data = [data]
        with open(output_file, "w") as file:
            json.dump(data, file, indent=2)
        print(f"Response saved to {output_file}")
    except json.JSONDecodeError as e:
        print("Failed to parse JSON response:", e)
        print("Response text:", response.text)

    return retrieve_entity_from_json_file(output_file)

def delete_all_type(type):
    with Client(
        hostname=broker_url,
        port=broker_port,
        tenant=broker_tenant,
        port_temporal=temporal_port,
    ) as client:
        entities = client.query(type=type, ctx=ctx)
        print("Entities retrieved: ", len(entities))

        # Delete by type
        # client.drop("https://schema.org/BusStop")

        # Delete by list
        if len(entities) > 0:
            client.delete(entities)
            print("\n")
        else:
            print("Skipping - no entities to delete\n")


"""
entity_list = get_parking_data()
print ("Num entities to upload" , len(entity_list))
entity_list[1].pprint()
create_entities_in_broker(entity_list)


print("\n\n\n\n\n")
retrieved_carparks = retrieve_carparks()
print ("Num entities retrieved" , len(retrieved_carparks))
retrieved_carparks[0].pprint()


print("\n\n\n\n\n")
#delete_all_type("Carpark")
gq = "geometry=Point&georel=near%3BmaxDistance==800&coordinates=%5B103.83359,1.3071%5D"
#retrieved_carparks = geoquery_ngsi_long(input_type = "Carpark" , geoquery = gq)

geoquery_ngsi_point(input_type = "Carpark", maxDistance=100 , lat = 103.83359, long= 1.3071)

"""
