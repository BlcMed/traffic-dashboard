import os
from dotenv import load_dotenv
import requests
import json


class TomtomClient:
    icons_dict = {
        0: "Unknown",
        1: "Accident",
        2: "Fog",
        3: "Dangerous Conditions",
        4: "Rain",
        5: "Ice",
        6: "Jam",
        7: "Lane Closed",
        8: "Road Closed",
        9: "Road Works",
        10: "Wind",
        11: "Flooding",
        14: "Broken Down Vehicle",
    }
    # fields we're interested in
    fields = {
        "fields": "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,startTime,endTime,from,to,length,roadNumbers}}}"
    }

    dotenv_path = "config/.env"
    load_dotenv(dotenv_path)

    def __init__(self):
        self.base_url = os.getenv("base_url")
        self.key = os.getenv("key")

    def make_incident_request(self, method="GET", json=None, params={}, headers=None):
        try:
            key_dict = {"key": self.key}
            base_params = {**TomtomClient.fields, **key_dict}
            response = requests.request(
                method=method,
                url=self.base_url + "/incidentDetails",
                json=json,
                params={**params, **base_params},
            )
            response.raise_for_status()  # Check for HTTP errors
            return response
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error making incident request: {e}")

    def make_request_bounding_box(
        self, bbox_coor, language="en-GB", timeValidityFilter="present", extra_params={}
    ):
        try:
            minLon, minLat, maxLon, maxLat = bbox_coor
            bbox = {"bbox": f"{minLon},{minLat},{maxLon},{maxLat}"}
            function_params = {**bbox, **{"language": language}}
            params = {**function_params, **extra_params}
            return self.make_incident_request(method="GET", params=params)
        except Exception as e:
            raise Exception(f"Error making bounding box request: {e}")

    def make_request_ids(
        self, ids, language="en-GB", timeValidityFilter="present", extra_params={}
    ):
        try:
            ids = {"ids": f"{ids}"}
            function_params = {
                **ids,
                **{"language": language, "timeValidityFilter": timeValidityFilter},
            }
            params = {**function_params, **extra_params}
            return self.make_incident_request(method="GET", params=params)
        except Exception as e:
            raise Exception(f"Error making IDs request: {e}")

    def make_request_area(
        self,
        area="New York",
        categoryFilter=1,
        language="en-GB",
        timeValidityFilter="present",
        extra_params={},
    ):
        pass


client = TomtomClient()
with open("data/coordinates.json", "r") as json_file:
    coordinates_dict = json.load(json_file)
    json_file.close()

bbox = coordinates_dict["New York"]["bbox"]
response = client.make_request_bounding_box(bbox)
# would be changed later to
# response=client.make_request_area('New York')

with open("data/raw/temp6.txt", "wb") as file:
    file.write(response.content)

incidents = response.json()["incidents"]
print(f"we got {len(incidents)} incident.")


# for incident in incidents:
#     print(
#         f'time : {incident["properties"]["startTime"]}, category:{icons_dict[incident["properties"]["iconCategory"]] } .'
#     )
