import os
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
        "fields": "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,startTime,endTime,from,to,length,delay,roadNumbers}}}"
    }


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
        language="en-GB",
        timeValidityFilter="present",
        extra_params={},
    ):
        try:
            with open("data/coordinates.json", "r") as json_file:
                coordinates_dict = json.load(json_file)
                bbox = coordinates_dict[area]["bbox"]
                response = self.make_request_bounding_box(bbox)
                return response
        except FileNotFoundError as e:
            raise Exception("Error opening coordinates.json file: File not found")
        except KeyError as e:
            raise Exception("Error parsing coordinates.json file: Invalid format")
        except Exception as e:
            raise Exception(f"Error loading coordinates.json file: {e}")

    def save_response(self, response, filename):
        with open(filename, "wb") as file:
            file.write(response.content)

