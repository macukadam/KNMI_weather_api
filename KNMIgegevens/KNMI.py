__author__ = "Ugurcan Akpulat"
__copyright__ = "Copyright 2021, Eleena Software"
__credits__ = [""]
__license__ = "MIT"
__version__ = "1.0.0"
__maintainer__ = "Ugurcan Akpulat"
__email__ = "ugurcan.akpulat@gmail.com"
__status__ = "Production"

import pandas as pd
import os
import knmi
import asyncio
import sys
from .KNMI_data_fetch import KNMIDataLoader
from geopy.distance import geodesic
from geopy.geocoders import Nominatim


class KNMI():
    """
    Class responsible for loading csv files into memory
    """

    def __init__(self, data_type='hourly', download=False):
        dir_name = 'station_uur_temp'
        if data_type == 'hourly':
            loader = KNMIDataLoader(1, 1000, 'hourly', dir_name)
        elif data_type == 'daily':
            dir_name = 'station_dag_temp'
            loader = KNMIDataLoader(1, 1000, 'daily', dir_name)
        else:
            raise ValueError('Wrong parameter for data_type. Use hourly or daily')

        if download:
            asyncio.run(loader.start(data_type))

        self.__path = os.path.join(os.getcwd(), dir_name)
        self.__stations = {}
        self.__downloaded_station_numbers = [int(f.split('.')[0]) for f in
                                             os.listdir(self.__path)
                                             if f.endswith('.csv')]

        for nmbr in self.__downloaded_station_numbers:
            file_path = os.path.join(self.__path, str(nmbr) + '.csv')
            sys.stdout.write(f'\rLoading station: {nmbr}')
            dtypes = self.__dtypes(file_path)
            self.__stations[nmbr] = pd.read_csv(os.path.join(
                                                self.__path, file_path),
                                                dtype=dtypes)

        sys.stdout.write('\n')

    def __len__(self):
        return len(self.__stations)

    def __getitem__(self, position):
        return self.__stations[position]

    def __iter__(self):
        self.n = 0
        return self

    def __next__(self):
        if self.n <= len(self):
            result = self[self.__downloaded_station_numbers[self.n]]
            self.n += 1
            return result
        else:
            raise StopIteration

    def __dtypes(self, path: str) -> dict:
        headers = pd.read_csv(path, index_col=0, nrows=0).columns.tolist()
        dtypes = {headers: float for headers in headers}
        dtypes['YYYYMMDD'] = str
        return dtypes

    def __get_coordinates_from_postcode_NL(self, postalcode: str, cnt='NL'):
        geolocator = Nominatim(user_agent="knmi-app")
        location = geolocator.geocode(query={'postalcode': postalcode,
                                             'country': cnt})
        return location

    def __find_closest_st(self, lat: int, lng: int) -> knmi.metadata.Station:
        stations = [knmi.stations.get(station)
                    for station in self.__downloaded_station_numbers]
        minDistance = geodesic((lat, lng), (stations[0].latitude,
                                            stations[0].longitude)).meters
        for station in stations:
            tempDistance = geodesic((lat, lng), (station.latitude,
                                                 station.longitude)).meters
            if tempDistance < minDistance:
                minDistance = tempDistance
                closestStation = station
        return closestStation

    def find_df(self, postcode: str) -> pd.DataFrame:
        """This method gets the df of the closest station"""
        cord = self.__get_coordinates_from_postcode_NL(postcode)
        station = self.__find_closest_st(cord.latitude, cord.longitude)
        return self.__stations[station.number]
