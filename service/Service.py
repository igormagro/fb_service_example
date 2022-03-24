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
    CRIME_DATA = dict()

    def __init__(self):
        if Path("./data/lookup/de_geo_lookup.json").exists():
            with open("./data/lookup/de_geo_lookup.json", "r") as fp:
                self.DE_GEO_LOOKUP = json.load(fp)
                fp.close()


        # Load crime data directly on instantiation
        self.CRIME_DATA_DF = self._get_data()
        self.CRIME_DATA_DF.sort_values(by='reference_year', ascending=False, inplace=True)
        self.CRIME_DATA_DF = self.CRIME_DATA_DF.to_dict(orient='records')

        for d in self.CRIME_DATA_DF:
            if d["administrative_district_key"] not in self.CRIME_DATA:
                self.CRIME_DATA[d["administrative_district_key"]] = list()
            self.CRIME_DATA[d["administrative_district_key"]].append(d)            

        pass

    @classmethod
    def _get_location_geocoding(cls, address):
        # For this service we only need the postal code
        # Check if address string directly contains postal code
        # If yes, avoid calling geocoding service
        zip_code = cls.ZIP_CODE_PATTERN.search(address)
        if zip_code is not None:
            try:
                return {"data" : [{"postcode": zip_code.group(2).strip()}]}
            except Exception as e:
                pass

        # Invoke request if fast pattern matching failed
        data_service = DataService(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                                   connection={"base_uri": "https://api.fusionbase.com/v1"})
        location_geocoding_converter_id = 40425233
        payload = {'name': 'address', 'value': address}
        result = data_service.invoke(key=location_geocoding_converter_id, parameters=payload)
        return result

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
        result = data_service.invoke(key=zip_code_converter_id, parameters=payload)
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
            last_update = data_stream.get_meta_data(crime_data_key).get('data_updated_at')
            df = data_stream.get_dataframe(key=crime_data_key)
            df.to_parquet('./data/source/crime_data.parquet')
            with open('./data/source/local_last_update.pickle', 'wb') as f:
                pickle.dump(last_update, f)

        else:
            df = pd.read_parquet('./data/source/crime_data.parquet')

        return df


    def invoke(self, address_string):
        import time
        start = time.time()

        try:
            geo_data = self._get_location_geocoding(address_string)['data'][0]   
            zip_code = geo_data.get('postcode')
            ags = self._get_ags_from_zip_code(zip_code)
            crime_data_json = self.CRIME_DATA[ags]

            result = {
                'input': address_string,
                'parsing_successful': True,
                'zip_code': zip_code,
                'city': geo_data.get('city'),
                'state': geo_data.get('state'),
                'administrative_district_key': ags,
                'crime_data': crime_data_json
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