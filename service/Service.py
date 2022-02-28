import os
import pickle

import pandas as pd
from fusionbase.DataService import DataService
from fusionbase.DataStream import DataStream


class Service:
    def __init__(self):
        pass

    @staticmethod
    def _get_location_geocoding(address):
        data_service = DataService(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                                   connection={"base_uri": "https://api.fusionbase.com/v1"})
        location_geocoding_converter_id = 40425233
        payload = {'name': 'address', 'value': address}
        result = data_service.invoke(key=location_geocoding_converter_id, parameters=payload)
        return result

    @staticmethod
    def _get_ags_from_zip_code(zip_code):
        data_service = DataService(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                                   connection={"base_uri": "https://api.fusionbase.com/v1"})
        zip_code_converter_id = 33387026
        payload = [{'name': 'zip_code', 'value': zip_code}]
        result = data_service.invoke(key=zip_code_converter_id, parameters=payload)
        result = result['data']
        if len(result) < 1:
            return
        else:
            ags = result[0].get('administrative_district_key')
            return str(ags)

    @staticmethod
    def _get_data() -> pd.DataFrame:
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
        try:
            geo_data = self._get_location_geocoding(address_string)['data'][0]
            zip_code = geo_data.get('postcode')
            ags = self._get_ags_from_zip_code(zip_code)
            df = self._get_data()
            df = df.loc[:, ~df.columns.isin(['fb_id', 'source_key', 'fb_datetime', 'fb_data_version'])]
            crime_for_ags = df.loc[df['administrative_district_key'] == ags]
            crime_for_ags = crime_for_ags.sort_values(by='reference_year', ascending=False)
            crime_data_json = crime_for_ags.to_dict(orient='records')

            result = {
                'input': address_string,
                'parsing_successful': True,
                'zip_code': zip_code,
                'city': geo_data.get('city'),
                'state': geo_data.get('state'),
                'administrative_district_key': ags,
                'crime_data': crime_data_json
            }

        except:

            result = {
                'input': address_string,
                'parsing_successful': False
            }

        return result
