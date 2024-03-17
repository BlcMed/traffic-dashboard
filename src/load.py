import json
import pandas as pd
from matplotlib import pyplot as plt


def load_incidents(incidents):
    customized_incidents = []
    for inc in incidents:
        properties = inc["properties"]
        customized_incidents.append(
            {
                "id": properties["id"],
                "geometry_type": inc["geometry"]["type"],
                "geometry_coordinates": inc["geometry"]["coordinates"],
                "from": properties["from"],
                "to": properties["to"],
                "startTime": properties["startTime"],
                "endTime": properties["endTime"],
                "category": properties["iconCategory"],
                "roadNumbers": properties["roadNumbers"],
                "length": properties["length"],
                "delay": properties["delay"],
            }
        )

    df = pd.DataFrame(customized_incidents)

    # Convert time type to datetime
    df["startTime"] = pd.to_datetime(df["startTime"])
    df["endTime"] = pd.to_datetime(df["endTime"])

    # Convert df["geometry_coordinates"] list of lists to tuple of tuples
    df["geometry_coordinates"] = df["geometry_coordinates"].apply(
        lambda x: tuple(tuple(inner) for inner in x)
    )

    # Convert df["road_numbers"] list to tuple
    df["roadNumbers"] = df["roadNumbers"].apply(lambda x: tuple(x))

    df.drop_duplicates(subset=["id"], inplace=True)
    df.sort_values(by="startTime", inplace=True, ascending=False)
    return df


with open("data/raw/temp1.json", "r") as json_file:
    incidents = json.load(json_file)["incidents"]
df = load_incidents(incidents)
df.to_csv("data/processed/output.csv", index=False)
