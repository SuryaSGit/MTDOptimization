import pandas as pd
import requests
import time

#helper functions to fetch the data

def fetch_bus_stops(
    api_key: str,
    version: str = "v2.2",
    format: str = "json",
    method: str = "getstops",   
) -> dict:
    ''' returns a stop_id to stop_name dict'''
    response = requests.get(f"https://developer.mtd.org/api/{version}/{format}/{method}?key={api_key}")
    return {stop["stop_id"]: stop["stop_name"] for stop in response.json()["stops"]}

def fetch_current_bus_data(
        api_key: str,
        version: str = "v2.2",
        format: str = "json",
        method: str = "getvehicles"
        ) ->  dict[list[dict]]:
    """
    returns bus data in json format for all the busses running in urbana champaign at the time of function
    """
    url = f"https://developer.mtd.org/api/{version}/{format}/{method}?key={api_key}"
    response = requests.get(url)
    return response.json()

# does not change the input data variable itself
def format_bus_data(data: dict[list[dict]]) -> pd.DataFrame:
    """
    return a well formatted, human readable table from data
    """
    stop_id_to_stop_dict = fetch_bus_stops(api_key = "f29aa2f34d4d45c9814fbc90612219c9")
    out = []
    for vehicle in data["vehicles"]:

        vehicle["previous_stop"] = stop_id_to_stop_dict.get(str(vehicle["previous_stop_id"])[:-2],"Unknown")
        vehicle["next_stop"] = stop_id_to_stop_dict.get(str(vehicle["next_stop_id"])[:-2],"Unknown")
        vehicle["origin_stop"] = stop_id_to_stop_dict.get(str(vehicle["origin_stop_id"])[:-2],"Unknown")
        vehicle["destination_stop"] = stop_id_to_stop_dict.get(str(vehicle["destination_stop_id"])[:-2],"Unknown")

        if ("location" in vehicle) and (vehicle["location"]):
            vehicle["lat"] = vehicle["location"]["lat"]
            vehicle["lon"] = vehicle["location"]["lon"]
        else: 
            vehicle["lat"] = "Not available"
            vehicle["lon"] = "Not available"

        if ("trip" in vehicle) and (vehicle["trip"]["route_id"]):
            vehicle["route"] = vehicle["trip"]["route_id"] + " " + vehicle["trip"]["direction"]
        else:
            vehicle["route"] = "Not available"

        del vehicle["location"]
        del vehicle["trip"]
        del vehicle["previous_stop_id"]
        del vehicle["next_stop_id"]
        del vehicle["origin_stop_id"]
        del vehicle["destination_stop_id"]

        out += [vehicle]
    
    return pd.DataFrame(out)

def fetch_bus_data(
        api_key: str,
        version: str = "v2.2",
        format: str = "json",
        method: str = "getvehicles",
        times: int = 60, 
        fetch_function: function = fetch_current_bus_data, 
        format_function: function = format_bus_data
        ) -> pd.DataFrame:
    """
    fetches bus data 'times' times (once per minute) and returns the result as one concatinated dataframe 
    """
    t = time.time()
    res = []
    count = 0
    while(True): 
        if (time.time() - t) > 60:
            t = time.time()
            data = fetch_function(api_key=api_key,version=version,format=format,method=method)
            print(data)
            res.append(format_function(data))
            count += 1
        if count >= times:
            break
    return pd.concat(res).reset_index(drop=True)