from functools import lru_cache
import os
import pickle
import re
from pathlib import Path
import json

import pandas as pd
from fusionbase.DataService import DataService
from fusionbase.DataStream import DataStream


class Service:

    ZIP_CODE_PATTERN = re.compile(r'(D-)?(\d{5})')
    DE_GEO_LOOKUP = None
    CRIME_DATA_DF = None
    REFERENCE_YEAR = "2020"
    STATISTIC_COLUMNS = ["number_of_cases", "frequency_count", "number_of_attempted_cases", "threatened_with_firearm", "shot_with_firearm", "male_suspects", "female_suspects",  "number_of_non_german_suspects",
                        "number_of_suspects",
                        "cleared_cases"]
    CRIME_DATA = dict()

    def __init__(self):
        if Path("./data/lookup/de_geo_lookup.json").exists():
            with open("./data/lookup/de_geo_lookup.json", "r") as fp:
                self.DE_GEO_LOOKUP = json.load(fp)
                fp.close()

        if Path("./data/lookup/population_lookup.json").exists():
            with open("./data/lookup/population_lookup.json", "r") as fp:
                self.POP_LOOKUP = json.load(fp)
                fp.close()

        # Load crime data directly on instantiation
        self.CRIME_DATA_DF = self._get_data()
        self.CRIME_DATA_DF.sort_values(
            by='reference_year', ascending=False, inplace=True)
        self.CRIME_DATA_DF = self.CRIME_DATA_DF.to_dict(orient='records')

        for d in self.CRIME_DATA_DF:
            if d["administrative_district_key"] not in self.CRIME_DATA:
                self.CRIME_DATA[d["administrative_district_key"]] = list()
            self.CRIME_DATA[d["administrative_district_key"]].append(d)

        self.REFERENCE_DATA = dict()
        self.CRIME_DATA_DF = pd.DataFrame(self.CRIME_DATA_DF)

        if not Path("./data/lookup/reference_data_lookup.json").exists():
            df = self.CRIME_DATA_DF[self.CRIME_DATA_DF["reference_year"].astype(
                str) == self.REFERENCE_YEAR]

            
            sum_df = df.groupby(by=["source_key"]).sum().reset_index()
            for c in self.STATISTIC_COLUMNS:                
                sum_df[f'{c}'] = sum_df[c].apply(lambda x: self.get_rel_to_pop(x, 83121363)) #  83121363 hardcoded German population as of 2021
            
            for d in sum_df.to_dict(orient="records"):
                
                # Doesn't make sense to normalize this per 100k
                for _remove in ["number_of_attempted_cases_in_percent", "clearance_rate", "non_german_suspects_in_percent"]:
                    del d[_remove]

                if "DEU" not in self.REFERENCE_DATA:
                    self.REFERENCE_DATA["DEU"] = dict()
                if "NORMALIZED_PER_100K" not in self.REFERENCE_DATA["DEU"]:
                    self.REFERENCE_DATA["DEU"]["NORMALIZED_PER_100K"] = dict()
                # Hardcoded year for now
                if self.REFERENCE_YEAR not in self.REFERENCE_DATA["DEU"]["NORMALIZED_PER_100K"]:
                    self.REFERENCE_DATA["DEU"]["NORMALIZED_PER_100K"][self.REFERENCE_YEAR] = dict(
                    )

                d["reference_year"] = int(d["reference_year"])
                self.REFERENCE_DATA["DEU"]["NORMALIZED_PER_100K"][self.REFERENCE_YEAR][d["source_key"]] = d
                self.REFERENCE_DATA["DEU"]["NORMALIZED_PER_100K"][self.REFERENCE_YEAR][d["source_key"]]["reference_year"] = int(self.REFERENCE_YEAR) # Otherwise reference year is summed up


            mean_df = df.groupby(by=["source_key"]).mean().round(2).reset_index()
            for d in mean_df.to_dict(orient="records"):
                if "DEU" not in self.REFERENCE_DATA:
                    self.REFERENCE_DATA["DEU"] = dict()
                if "MEAN" not in self.REFERENCE_DATA["DEU"]:
                    self.REFERENCE_DATA["DEU"]["MEAN"] = dict()
                # Hardcoded year for now
                if self.REFERENCE_YEAR not in self.REFERENCE_DATA["DEU"]["MEAN"]:
                    self.REFERENCE_DATA["DEU"]["MEAN"][self.REFERENCE_YEAR] = dict(
                    )

                d["reference_year"] = int(d["reference_year"])
                self.REFERENCE_DATA["DEU"]["MEAN"][self.REFERENCE_YEAR][d["source_key"]] = d

            with open("./data/lookup/reference_data_lookup.json", "w") as fp:
                json.dump(self.REFERENCE_DATA, fp)
                fp.close()
        else:
            with open("./data/lookup/reference_data_lookup.json", "r") as fp:
                self.REFERENCE_DATA = json.load(fp)
                fp.close()

        pass

    @classmethod
    def _get_location_geocoding(cls, address):
        # For this service we only need the postal code
        # Check if address string directly contains postal code
        # If yes, avoid calling geocoding service
        zip_code = cls.ZIP_CODE_PATTERN.search(address)
        if zip_code is not None:
            try:
                return {"data": [{"postcode": zip_code.group(2).strip()}]}
            except Exception as e:
                pass

        # Invoke request if fast pattern matching failed
        data_service = DataService(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                                   connection={"base_uri": "https://api.fusionbase.com/v1"})
        location_geocoding_converter_id = 40425233
        payload = {'name': 'address', 'value': address}
        result = data_service.invoke(
            key=location_geocoding_converter_id, parameters=payload)
        return result['place']

    def _get_ags_from_zip_code(self, zip_code):

        # First, try to get from local geolookup
        if self.DE_GEO_LOOKUP is not None:
            try:
                return str(self.DE_GEO_LOOKUP[zip_code])
            except KeyError as e:
                pass

        # Fallback, directly call the service
        data_service = DataService(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                                   connection={"base_uri": "https://api.fusionbase.com/v1"})
        zip_code_converter_id = 33387026
        payload = [{'name': 'zip_code', 'value': zip_code}]
        result = data_service.invoke(
            key=zip_code_converter_id, parameters=payload)
        result = result['data']
        if len(result) < 1:
            return None
        else:
            ags = result[0].get('administrative_district_key')
            return str(ags)

    @classmethod
    def _get_data(cls) -> pd.DataFrame:
        print("LOAD HERE")
        data_stream = DataStream(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                                 connection={"base_uri": "https://api.fusionbase.com/api/v1"}, log=True)
        crime_data_key = 2246748

        if not os.path.exists('./data/source/crime_data.parquet'):
            last_update = data_stream.get_meta_data(
                crime_data_key).get('data_updated_at')
            df = data_stream.get_dataframe(key=crime_data_key)
            df.to_parquet('./data/source/crime_data.parquet')
            with open('./data/source/local_last_update.pickle', 'wb') as f:
                pickle.dump(last_update, f)

        else:
            df = pd.read_parquet('./data/source/crime_data.parquet')

        return df
    
    def get_rel_to_pop(self, value, population):
        value_per100k = (int(value) / int(population)) * 100000
        return round(value_per100k, 2)
        
        
        
    def invoke(self, address_string, reference_year, criminal_offense_keys=[]):
        import time
        start = time.time()

        try:
            geo_data = self._get_location_geocoding(address_string)['data'][0]
            zip_code = geo_data.get('postcode')
            ags = self._get_ags_from_zip_code(zip_code)
            crime_data_json = list()

            if isinstance(criminal_offense_keys, list) and len(criminal_offense_keys) > 0:
                for key, co_stats in self.CRIME_DATA.items():

                    if key != ags:
                        continue

                    for co_stat in co_stats:
                        if co_stat["source_key"] not in criminal_offense_keys:
                            continue

                        if reference_year == "ANY":
                            crime_data_json.append(co_stat)
                        else:
                            if str(co_stat["reference_year"]) == reference_year:
                                crime_data_json.append(co_stat)
            else:
                crime_data_json = self.CRIME_DATA[ags]

            reference_data_points = {
                "MEAN": list(),
                "NORMALIZED_PER_100K": list()
            }
            for co_key in criminal_offense_keys:
                reference_data_points["MEAN"].append(
                    self.REFERENCE_DATA["DEU"]["MEAN"][str(self.REFERENCE_YEAR)][co_key])
                reference_data_points["NORMALIZED_PER_100K"].append(
                    self.REFERENCE_DATA["DEU"]["NORMALIZED_PER_100K"][str(self.REFERENCE_YEAR)][co_key])

            population = self.POP_LOOKUP.get(str(ags), None)
            population = population['population']
            print(f'POPULATION FOR AGS {ags} IS: {population}')
            
                
            if population is None:
                crime_data_to_pop = None
            else:
                crime_data_to_pop = pd.DataFrame(crime_data_json)
                integer_columns = ["number_of_cases", "frequency_count", "number_of_attempted_cases", "number_of_attempted_cases_in_percent", "threatened_with_firearm", "shot_with_firearm", "male_suspects", "female_suspects",      "number_of_non_german_suspects",
                                   "non_german_suspects_in_percent",
                                   "number_of_suspects",
                                   "cleared_cases"]
                
                for c in self.STATISTIC_COLUMNS:
                    crime_data_to_pop[f'{c}'] = crime_data_to_pop[c].apply(lambda x: self.get_rel_to_pop(x, population))
                
                #crime_data_to_pop.rename(columns={c:f'{c}_per100k' for c in self.STATISTIC_COLUMNS}, inplace=True)
                
                    
                crime_data_to_pop = crime_data_to_pop.to_dict(orient='records')

            result = {
                'input': address_string,
                'parsing_successful': True,
                'zip_code': zip_code,
                'city': geo_data.get('city'),
                'state': geo_data.get('state'),
                'administrative_district_key': ags,
                'crime_data': crime_data_json,
                'crime_data_per_100k': crime_data_to_pop,
                'ags_population': population,
                'references': [
                {
                    "area": "DEU",
                    "type": "NORMALIZED_PER_100K",
                    "data": reference_data_points["NORMALIZED_PER_100K"]
                },                
                {
                    "area": "DEU",
                    "type": "MEAN",
                    "data": reference_data_points["MEAN"]
                }]
            }

        except Exception as e:

            print(e)

            result = {
                'input': address_string,
                'parsing_successful': False
            }

        end = time.time() - start
        print(end)

        return result


# if __name__ == '__main__':

#     zip_code_match = ZIP_CODE_PATTERN.search("Agnes-Pockels-Bogen 1, 80992")
#     if zip_code_match is not None:
#         print(zip_code_match.group(2))
#     print(zip_code_match)
