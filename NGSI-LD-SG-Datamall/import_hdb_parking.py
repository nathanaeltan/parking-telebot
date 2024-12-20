import requests
import json
import pandas as pd
from ngsildclient import Entity
import mylibs.SVY21 as SVY21
import mylibs.constants as constants
import re
from mylibs.ngsi_ld import create_entities_in_broker

ctx = constants.ctx
broker_url = constants.broker_url
broker_port = constants.broker_port  # default, 80
temporal_port = constants.temporal_port  # default 1026
broker_tenant = constants.broker_tenant
HDB_PARKING_LIST_API = "https://data.gov.sg/api/action/datastore_search?resource_id=d_23f946fa557947f93a8043bbef41dd09"
HDB_PARKING_AVAILABILITY_API = "https://api.data.gov.sg/v1/transport/carpark-availability"
CENTRAL_CARPARK_IDS = [
    "ACB",
    "BBB",
    "BRB1",
    "CY",
    "DUXM",
    "HLM",
    "KAB",
    "KAM",
    "KAS",
    "PRM",
    "SLS",
    "SR1",
    "SR2",
    "TPM",
    "UCS",
    "WCB",
]

PEAK_HOUR_CARPARK_IDS = [
    "ACB",
    "CY",
    "SE21",
    "SE22",
    "SE24",
    "MP14",
    "MP15",
    "MP16",
    "HG9",
    "HG9T",
    "HG15",
    "HG16"
]


def generate_pricing(carpark_id):
    pricing = {
            "Car": {
                "TimeSlots": [
                    {
                        "WeekdayRate": {
                            "startTime": "0700",
                            "endTime": "0830",
                            "weekdayMin": "30 mins",
                            "weekdayRate": "$0.60" if carpark_id not in CENTRAL_CARPARK_IDS else "$1.20"
                        },
                        "SaturdayRate": {
                            "startTime": "0700",
                            "endTime": "0830",
                            "satdayMin": "30 mins",
                            "satdayRate": "$0.60" if carpark_id not in CENTRAL_CARPARK_IDS else "$1.20"
                        },
                        "SundayPHRate": {
                            "startTime": "0700",
                            "endTime": "0830",
                            "sunPHMin": "30 mins",
                            "sunPHRate": "$0.60" if carpark_id not in CENTRAL_CARPARK_IDS else "$1.20"
                        }
                    },
                    {
                        "WeekdayRate": {
                            "startTime": "0830",
                            "endTime": "1700",
                            "weekdayMin": "30 mins",
                            "weekdayRate": "$0.60" if carpark_id not in CENTRAL_CARPARK_IDS else "$1.20"
                        },
                        "SaturdayRate": {
                            "startTime": "0830",
                            "endTime": "1700",
                            "satdayMin": "30 mins",
                            "satdayRate": "$0.60" if carpark_id not in CENTRAL_CARPARK_IDS else "$1.20"
                        },
                        "SundayPHRate": {
                            "startTime": "0830",
                            "endTime": "1700",
                            "sunPHMin": "30 mins",
                            "sunPHRate": "$0.60" if carpark_id not in CENTRAL_CARPARK_IDS else "$1.20"
                        }
                    },
                    {
                        "WeekdayRate": {
                            "startTime": "1700",
                            "endTime": "2200",
                            "weekdayMin": "30 mins",
                            "weekdayRate": "$0.60"
                        },
                        "SaturdayRate": {
                            "startTime": "1700",
                            "endTime": "2200",
                            "satdayMin": "30 mins",
                            "satdayRate": "$0.60"
                        },
                        "SundayPHRate": {
                            "startTime": "1700",
                            "endTime": "2200",
                            "sunPHMin": "30 mins",
                            "sunPHRate": "$0.60"
                        }
                    },
                    {
                        "WeekdayRate": {
                            "startTime": "2200",
                            "endTime": "0700",
                            "weekdayMin": "30 mins",
                            "weekdayRate": "$0.00"
                        },
                        "SaturdayRate": {
                            "startTime": "2200",
                            "endTime": "0700",
                            "satdayMin": "30 mins",
                            "satdayRate": "$0.00"
                        },
                        "SundayPHRate": {
                            "startTime": "2200",
                            "endTime": "0700",
                            "sunPHMin": "30 mins",
                            "sunPHRate": "$0.00"
                        }
                    }
                ]
            },
            "Motorcycle": {
                "TimeSlots": [
                    {
                        "WeekdayRate": {
                            "startTime": "0700",
                            "endTime": "0830",
                            "weekdayMin": "0 mins",
                            "weekdayRate": "$0.00"
                        },
                        "SaturdayRate": {
                            "startTime": "0700",
                            "endTime": "0830",
                            "satdayMin": "0 mins",
                            "satdayRate": "$0.00"
                        },
                        "SundayPHRate": {
                            "startTime": "0700",
                            "endTime": "0830",
                            "sunPHMin": "0 mins",
                            "sunPHRate": "$0.00"
                        }
                    },
                    {
                        "WeekdayRate": {
                            "startTime": "0830",
                            "endTime": "2200",
                            "weekdayMin": "810 mins",
                            "weekdayRate": "$0.65"
                        },
                        "SaturdayRate": {
                            "startTime": "0830",
                            "endTime": "2200",
                            "satdayMin": "810 mins",
                            "satdayRate": "$0.65"
                        },
                        "SundayPHRate": {
                            "startTime": "0830",
                            "endTime": "2200",
                            "sunPHMin": "810 mins",
                            "sunPHRate": "$0.65"
                        }
                    },
                    {
                        "WeekdayRate": {
                            "startTime": "2200",
                            "endTime": "0700",
                            "weekdayMin": "0 mins",
                            "weekdayRate": "$0.00"
                        },
                        "SaturdayRate": {
                            "startTime": "2200",
                            "endTime": "0700",
                            "satdayMin": "0 mins",
                            "satdayRate": "$0.00"
                        },
                        "SundayPHRate": {
                            "startTime": "2200",
                            "endTime": "0700",
                            "sunPHMin": "0 mins",
                            "sunPHRate": "$0.00"
                        }
                    }
                ]
            },
            "Heavy Vehicle": {
                "TimeSlots": [
                    {
                        "WeekdayRate": {
                            "startTime": "0700",
                            "endTime": "0830",
                            "weekdayMin": "0 mins",
                            "weekdayRate": "$0.00"
                        },
                        "SaturdayRate": {
                            "startTime": "0700",
                            "endTime": "0830",
                            "satdayMin": "0 mins",
                            "satdayRate": "$0.00"
                        },
                        "SundayPHRate": {
                            "startTime": "0700",
                            "endTime": "0830",
                            "sunPHMin": "0 mins",
                            "sunPHRate": "$0.00"
                        }
                    },
                    {
                        "WeekdayRate": {
                            "startTime": "0830",
                            "endTime": "2200",
                            "weekdayMin": "30 mins",
                            "weekdayRate": "$1.20"
                        },
                        "SaturdayRate": {
                            "startTime": "0830",
                            "endTime": "2200",
                            "satdayMin": "30 mins",
                            "satdayRate": "$1.20"
                        },
                        "SundayPHRate": {
                            "startTime": "0830",
                            "endTime": "2200",
                            "sunPHMin": "30 mins",
                            "sunPHRate": "$1.20"
                        }
                    },
                    {
                        "WeekdayRate": {
                            "startTime": "2200",
                            "endTime": "0700",
                            "weekdayMin": "0 mins",
                            "weekdayRate": "$0.00"
                        },
                        "SaturdayRate": {
                            "startTime": "2200",
                            "endTime": "0700",
                            "satdayMin": "0 mins",
                            "satdayRate": "$0.00"
                        },
                        "SundayPHRate": {
                            "startTime": "2200",
                            "endTime": "0700",
                            "sunPHMin": "0 mins",
                            "sunPHRate": "$0.00"
                        }
                    }
                ]
            }
        }


    if carpark_id in PEAK_HOUR_CARPARK_IDS:
        for slot in pricing["Car"]["TimeSlots"]:
            if slot["WeekdayRate"]["weekdayRate"] != "$0.00":
                slot["WeekdayRate"]["weekdayRate"] = "${:.2f}".format(
                    float(slot["WeekdayRate"]["weekdayRate"][1:]) + 0.20
                )
            if slot["SaturdayRate"]["satdayRate"] != "$0.00":
                slot["SaturdayRate"]["satdayRate"] = "${:.2f}".format(
                    float(slot["SaturdayRate"]["satdayRate"][1:]) + 0.20
                )
            if slot["SundayPHRate"]["sunPHRate"] != "$0.00":
                slot["SundayPHRate"]["sunPHRate"] = "${:.2f}".format(
                    float(slot["SundayPHRate"]["sunPHRate"][1:]) + 0.20
                )

    return pricing

def fetch_hdb_carpark_list():
    list_of_hdb_carparks = []
    offset = 0
    while True:
        response = requests.get(
            HDB_PARKING_LIST_API,
            params={"offset": offset},
        )
        if response.status_code == 200:
            hdb_carparks = json.loads(response.content.decode("utf-8"))["result"][
                "records"
            ]
            if len(hdb_carparks) == 0:
                break
            list_of_hdb_carparks.extend(hdb_carparks)
            offset += 100
        else:
            break
    return list_of_hdb_carparks

def fetch_hdb_carpark_availability():
    response = requests.get(HDB_PARKING_AVAILABILITY_API)
    list_of_hdb_carpark_availabilities = []
    if response.status_code == 200:
        list_of_hdb_carpark_availabilities = json.loads(
            response.content.decode("utf-8")
        )["items"][0]["carpark_data"]

    return list_of_hdb_carpark_availabilities


def fetch_hdb_capark_raw_data():
    list_of_hdb_carparks = fetch_hdb_carpark_list()
    list_of_hdb_carpark_availabilities = fetch_hdb_carpark_availability()
    print("Number of HDB carparks: ", len(list_of_hdb_carparks))
    df_cp = pd.DataFrame(list_of_hdb_carparks)
    df_cp = df_cp[df_cp["short_term_parking"] != "NO"]
    df_cpa = pd.DataFrame(list_of_hdb_carpark_availabilities)
    df_merged = pd.merge(
        df_cp,
        df_cpa,
        left_on="car_park_no",
        right_on="carpark_number",
        how="inner",  # Only keep matched records
    )

    final_json = df_merged.to_dict(orient="records")
    return final_json

def generate_carpark_id(carpark):
    id = carpark["address"].replace(" ", "") + str(carpark["car_park_no"])
    pattern = r"[^a-zA-Z0-9]"
    id = re.sub(pattern, "-", id)
    return id

def main():
    entity_list = []
    svy21_converter = SVY21.SVY21()
    print("\nFetching HDB Parking data...")
    raw_carpark_data = fetch_hdb_capark_raw_data()
    for carpark in raw_carpark_data:
        id = generate_carpark_id(carpark)
        entity = Entity("Carpark", id, ctx=ctx)

        entity.prop("CarparkName", carpark["address"])
        svy21_geocoordinates = [carpark["x_coord"], carpark["y_coord"]]
        latlon_geocoordinates = svy21_converter.computeLatLon(
            float(svy21_geocoordinates[1]), float(svy21_geocoordinates[0])
        )
        if len(latlon_geocoordinates) > 1:
            entity.gprop(
                "location",
                (float(latlon_geocoordinates[0]), float(latlon_geocoordinates[1])),
            )
        carpark_info = carpark.get("carpark_info", [])
        lot_type_c = next(
            (info for info in carpark_info if info.get("lot_type") == "C"), None
        )
        if lot_type_c:
            total_lots_c = int(lot_type_c.get("total_lots", 0) or 0)
            lots_available_c = int(lot_type_c.get("lots_available", 0) or 0)
            entity.prop("ParkingCapacity", total_lots_c)
            entity.prop("ParkingAvailability", lots_available_c)

        entity.prop(
            "Sheltered",
            False if carpark.get("car_park_type") == "SURFACE CAR PARK" else True,
        )
        capark_pricing = generate_pricing(carpark["car_park_no"])
        entity.prop("Pricing", capark_pricing)
        entity_list.append(entity)

    print("\nPushing to HDB Parking to broker...")
    create_entities_in_broker(entity_list)




if __name__ == "__main__":
    main()