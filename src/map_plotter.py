import folium
from folium import GeoJson


class MapPlotter:
    def __init__(self, incidents, center_lat, center_lon):
        self.incidents = incidents
        self.map = folium.Map(location=[center_lon, center_lat], zoom_start=12)

    def save_map(self, map_path="data/processed/incidents_map.html"):
        self.map.save(map_path)

    def plot_map(self, bbox):
        minLon, minLat, maxLon, maxLat = bbox
        # geojson dict that is a polygone of bbox
        wrapper = {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [minLon, maxLat],
                        [maxLon, maxLat],
                        [maxLon, minLat],
                        [minLon, minLat],
                        [minLon, maxLat],
                    ]
                ],
            },
        }
        GeoJson(wrapper, style_function=lambda x: {"fillOpacity": 0.3, "fillColor": "green"}).add_to(self.map)

        incidents = self.incidents
        for incident in incidents:
            # Create a GeoJson object and add it to the map
            geojson_polygon = {
                "type": incident["type"],
                "geometry": incident["geometry"],
                "properties": incident["properties"],
            }
            GeoJson(geojson_polygon).add_to(self.map)

    def plot_scatter_map(self, df, category="Jam", color="blue"):
        criteria = df["category"] == category
        coordinates = df[criteria]["coordinates"]
        # Plot each incident on the map
        for lat, lon in coordinates:
            folium.Circle(
                location=[lon, lat],
                radius=50,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.4,
                popup=category,
            ).add_to(self.map)


import json

file_path = "data/raw/temp1.json"
with open(file_path, "r") as json_file:
    incidents = json.load(json_file)["incidents"]
    json_file.close()

center_lat, center_lon = -73.9787155, 40.6974875
bbox = [-74.257159, 40.477398, -73.700272, 40.917577]
mp = MapPlotter(incidents, center_lat, center_lon)
mp.plot_map(bbox)
mp.save_map()


# import pandas as pd
# import ast

# def tuple_converter(value):
#     try:
#         return ast.literal_eval(value)
#     except (SyntaxError, ValueError):
#         return value
# df_file_path = 'data_eng/data_storage/temp_data/output.csv'
# df = pd.read_csv(df_file_path, converters={'coordinates': tuple_converter})

# md=map_displayer(incidents)
# md.plot_map()
# md.plot_scatter_map(df)
# md.save_map()
