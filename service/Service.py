import re
from bs4 import BeautifulSoup
import requests

import json


class Service:
    OVERPASS_ENDPOINT = "http://overpass-api.de/api"
    with open("./data/source/map_features.json", "r") as fp:
        MAP_FEATURES = json.load(fp)
    fp.close()

    def __init__(self):
        pass

    def xml_to_json(self, node) -> dict:
        data = dict(
            id=node['id'],
            lat=float(node['lat']),
            lon=float(node['lon'])
        )

        tags = node.find_all("tag")
        for tag in tags:
            data.update({
                tag["k"]: tag["v"]
            })

        return data

    def invoke(self, feature_type, feature_value, lat, lon, radius):
        response = {
            "feature_type": feature_type,
            "feature_value": feature_value,
            "lat": lat,
            "lon": lon,
            "radius": radius
        }

        output = []

        # Inputs Validation
        if not all([feature_type, feature_value, lat, lon, radius]):
            return [400, {
                "success": False,
                "message": "Missing parameter"
            }]

        if feature_type not in self.MAP_FEATURES:
            response.update({
                "success": False,
                "message": f"feature_type '{feature_type}' does not exist. Please, check for any typos."
            })
            return (422, response)
        else:
            if feature_value not in self.MAP_FEATURES.get(feature_type):
                response.update({
                    "success": False,
                    "message": f"feature_value '{feature_value}' does not exist. Please, check for any typos."
                })
                return (422, response)

        query = f"""
        <query into="_" type="node">
            <has-kv k="{feature_type}" modv="" v="{feature_value}"/>
            <around radius="{radius}" lat="{lat}" lon="{lon}"/> 
        </query>
        <print/>"""

        data = requests.get(
            f"http://overpass-api.de/api/interpreter?data={query}")

        try:
            data.raise_for_status()
        except:
            response.update({
                "success": False,
                "message": "Something went wrong. Please try again later."
            })
            return (500, response)

        data = data.text

        data = BeautifulSoup(data, features="xml")
        nodes = data.find_all("node")
        for node in nodes:
            output.append(self.xml_to_json(node))

        hits = len(output)

        response.update({
            "hits": hits,
            "data": output
        })
        return (200, response)

    def get_map_features(self, feature: str = None) -> list:
        
        if isinstance(feature, str):
            if feature == "all" or feature == "":
                feature = "all"
                data = self.MAP_FEATURES
                res = {
                    "feature": feature,
                    "success": True,
                    "data": sorted(list(data.keys()))
                }
                return (200, res)

            elif feature in self.MAP_FEATURES:
                data = {feature: self.MAP_FEATURES.get(feature)}
                res = {
                    "feature": feature,
                    "success": True,
                    "data": sorted(data[feature])
                }
                return (200, res)

            else:
                res = {
                    "feature": feature,
                    "success": False,
                    "message": f"feature '{feature}' does not exist. Please, check for any typos."
                }
                return (422, res)
