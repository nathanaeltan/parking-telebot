from mylibs import ngsi_ld
from mylibs import constants
from import_hdb_parking import fetch_hdb_capark_raw_data, generate_carpark_id

from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
ctx = constants.ctx
broker_url = constants.broker_url
broker_port = constants.broker_port  # default, 80
temporal_port = constants.temporal_port  # default 1026
broker_tenant = constants.broker_tenant


def chunk_list(lst, chunk_size):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]


def main():
    print("Fetching HDB Carpark Data")
    hdb_carpark_data = fetch_hdb_capark_raw_data()
    context_carpark_data = ngsi_ld.retrieve_ngsi_type("Carpark")
     # Create a dictionary from the context data with 'id' as the key
    context_carpark_dict = {entity.id: entity for entity in context_carpark_data}

    # Iterate over the API data and update the ParkingAvailability attribute
    for carpark in hdb_carpark_data:
        carpark_id = generate_carpark_id(carpark)
        entity_id = f"urn:ngsi-ld:Carpark:{carpark_id}"
        if entity_id in context_carpark_dict:
            carpark_info = carpark.get("carpark_info", [])
            lot_type_c = next(
                (info for info in carpark_info if info.get("lot_type") == "C"), None
            )
            if lot_type_c:
                lots_available_c = int(lot_type_c.get("lots_available", 0) or 0)
                context_carpark_dict[entity_id]["ParkingAvailability"] = {
                    "type": "Property",
                    "value": lots_available_c
                }

    # Convert the updated dictionary values back to Entity objects
    entities_to_update = list(context_carpark_dict.values())

    # print("Updating HDB carpark availabilities to broker")
    print(entities_to_update[0])
    try:
        chunk_size = 100  # Define your chunk size
        for chunk in chunk_list(entities_to_update, chunk_size):
            ngsi_ld.update_entities_in_broker(chunk)
    except Exception as e:
        print(f"Failed to update entities: {e}")
    print("Update completed")


if __name__ == "__main__":
    INTERVAL_TO_FETCH_DATA = 5 # in minutes
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', minutes=INTERVAL_TO_FETCH_DATA)
    scheduler.start()

