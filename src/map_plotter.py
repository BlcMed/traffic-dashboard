import folium
from folium import GeoJson


class MapPlotter:
    def __init__(self, incidents, center_lat, center_lon):
        self.incidents = incidents
        self.map = folium.Map(location=[center_lon, center_lat], zoom_start=12)

    def save_map(self, map_path="data/processed/incidents_map.html"):
        self.map.save(map_path)

    def plot_map(self):
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

file_path = "data/raw/temp6.txt"
with open(file_path, "r") as file:
    incidents = json.loads(file.read())["incidents"]
    file.close()

center_lat, center_lon = -73.9787155, 40.6974875
mp = MapPlotter(incidents, center_lat, center_lon)
mp.plot_map()
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
