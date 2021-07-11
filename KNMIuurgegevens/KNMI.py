__author__ = "Ugurcan Akpulat"
__copyright__ = "Copyright 2021, Eleena Software"
__credits__ = [""]
__license__ = "MIT"
__version__ = "1.0.0"
__maintainer__ = "Ugurcan Akpulat"
__email__ = "ugurcan.akpulat@gmail.com"
__status__ = "Production"



from attr import s
import pandas as pd, os, knmi, asyncio
from .KNMI_data_fetch import KNMIDataLoader
from geopy.distance import geodesic
from geopy.geocoders import Nominatim

class KNMI():
    """
    Class responsible for loading csv files into memory. If the output directory name of the KNMI_fetch 
    is different than "stations_tmp", initialize this object with relevant directory name.
    
    Headers:
    YYYYMMDD  = Datum (YYYY=jaar MM=maand DD=dag) / Date (YYYY=year MM=month DD=day)
    DDVEC     = Vectorgemiddelde windrichting in graden (360=noord, 90=oost, 180=zuid, 270=west, 0=windstil/variabel). Zie http://www.knmi.nl/kennis-en-datacentrum/achtergrond/klimatologische-brochures-en-boeken / Vector mean wind direction in degrees (360=north, 90=east, 180=south, 270=west, 0=calm/variable)
    FHVEC     = Vectorgemiddelde windsnelheid (in 0.1 m/s). Zie http://www.knmi.nl/kennis-en-datacentrum/achtergrond/klimatologische-brochures-en-boeken / Vector mean windspeed (in 0.1 m/s)
    FG        = Etmaalgemiddelde windsnelheid (in 0.1 m/s) / Daily mean windspeed (in 0.1 m/s) 
    FHX       = Hoogste uurgemiddelde windsnelheid (in 0.1 m/s) / Maximum hourly mean windspeed (in 0.1 m/s)
    FHXH      = Uurvak waarin FHX is gemeten / Hourly division in which FHX was measured
    FHN       = Laagste uurgemiddelde windsnelheid (in 0.1 m/s) / Minimum hourly mean windspeed (in 0.1 m/s)
    FHNH      = Uurvak waarin FHN is gemeten / Hourly division in which FHN was measured
    FXX       = Hoogste windstoot (in 0.1 m/s) / Maximum wind gust (in 0.1 m/s)
    FXXH      = Uurvak waarin FXX is gemeten / Hourly division in which FXX was measured
    TG        = Etmaalgemiddelde temperatuur (in 0.1 graden Celsius) / Daily mean temperature in (0.1 degrees Celsius)
    TN        = Minimum temperatuur (in 0.1 graden Celsius) / Minimum temperature (in 0.1 degrees Celsius)
    TNH       = Uurvak waarin TN is gemeten / Hourly division in which TN was measured
    TX        = Maximum temperatuur (in 0.1 graden Celsius) / Maximum temperature (in 0.1 degrees Celsius)
    TXH       = Uurvak waarin TX is gemeten / Hourly division in which TX was measured
    T10N      = Minimum temperatuur op 10 cm hoogte (in 0.1 graden Celsius) / Minimum temperature at 10 cm above surface (in 0.1 degrees Celsius)
    T10NH     = 6-uurs tijdvak waarin T10N is gemeten / 6-hourly division in which T10N was measured; 6=0-6 UT, 12=6-12 UT, 18=12-18 UT, 24=18-24 UT 
    SQ        = Zonneschijnduur (in 0.1 uur) berekend uit de globale straling (-1 voor <0.05 uur) / Sunshine duration (in 0.1 hour) calculated from global radiation (-1 for <0.05 hour)
    SP        = Percentage van de langst mogelijke zonneschijnduur / Percentage of maximum potential sunshine duration
    Q         = Globale straling (in J/cm2) / Global radiation (in J/cm2)
    DR        = Duur van de neerslag (in 0.1 uur) / Precipitation duration (in 0.1 hour)
    RH        = Etmaalsom van de neerslag (in 0.1 mm) (-1 voor <0.05 mm) / Daily precipitation amount (in 0.1 mm) (-1 for <0.05 mm)
    RHX       = Hoogste uursom van de neerslag (in 0.1 mm) (-1 voor <0.05 mm) / Maximum hourly precipitation amount (in 0.1 mm) (-1 for <0.05 mm)
    RHXH      = Uurvak waarin RHX is gemeten / Hourly division in which RHX was measured
    PG        = Etmaalgemiddelde luchtdruk herleid tot zeeniveau (in 0.1 hPa) berekend uit 24 uurwaarden / Daily mean sea level pressure (in 0.1 hPa) calculated from 24 hourly values
    PX        = Hoogste uurwaarde van de luchtdruk herleid tot zeeniveau (in 0.1 hPa) / Maximum hourly sea level pressure (in 0.1 hPa)
    PXH       = Uurvak waarin PX is gemeten / Hourly division in which PX was measured
    PN        = Laagste uurwaarde van de luchtdruk herleid tot zeeniveau (in 0.1 hPa) / Minimum hourly sea level pressure (in 0.1 hPa)
    PNH       = Uurvak waarin PN is gemeten / Hourly division in which PN was measured
    VVN       = Minimum opgetreden zicht / Minimum visibility; 0: <100 m, 1:100-200 m, 2:200-300 m,..., 49:4900-5000 m, 50:5-6 km, 56:6-7 km, 57:7-8 km,..., 79:29-30 km, 80:30-35 km, 81:35-40 km,..., 89: >70 km)
    VVNH      = Uurvak waarin VVN is gemeten / Hourly division in which VVN was measured
    VVX       = Maximum opgetreden zicht / Maximum visibility; 0: <100 m, 1:100-200 m, 2:200-300 m,..., 49:4900-5000 m, 50:5-6 km, 56:6-7 km, 57:7-8 km,..., 79:29-30 km, 80:30-35 km, 81:35-40 km,..., 89: >70 km)
    VVXH      = Uurvak waarin VVX is gemeten / Hourly division in which VVX was measured
    NG        = Etmaalgemiddelde bewolking (bedekkingsgraad van de bovenlucht in achtsten, 9=bovenlucht onzichtbaar) / Mean daily cloud cover (in octants, 9=sky invisible)
    UG        = Etmaalgemiddelde relatieve vochtigheid (in procenten) / Daily mean relative atmospheric humidity (in percents)
    UX        = Maximale relatieve vochtigheid (in procenten) / Maximum relative atmospheric humidity (in percents)
    UXH       = Uurvak waarin UX is gemeten / Hourly division in which UX was measured
    UN        = Minimale relatieve vochtigheid (in procenten) / Minimum relative atmospheric humidity (in percents)
    UNH       = Uurvak waarin UN is gemeten / Hourly division in which UN was measured
    EV24      = Referentiegewasverdamping (Makkink) (in 0.1 mm) / Potential evapotranspiration (Makkink) (in 0.1 mm)
    """

    def __init__(self, download= False, dir_name='stations_tmp'):

        if download:
            loader = KNMIDataLoader(1, 1000)
            asyncio.run(loader.start())

        self.__path = os.path.join(os.getcwd(), dir_name)
        self.__stations = {}
        self.__downloaded_station_numbers = [int(f.split('.')[0]) for f in os.listdir(self.__path) if f.endswith('.csv')]
        
        for nmbr in self.__downloaded_station_numbers:
            file_path = os.path.join(self.__path, str(nmbr) + '.csv')
            print('Loading station: %s' % nmbr)
            dtypes = self.__dtypes(file_path)
            self.__stations[nmbr] = pd.read_csv(os.path.join(self.__path, file_path), dtype=dtypes)

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
            self.n +=1
            return result
        else:
            raise StopIteration

    def __dtypes(self, path:str) -> dict:
        headers = pd.read_csv(path, index_col=0, nrows=0).columns.tolist()
        dtypes = {headers:float for headers in headers}
        dtypes['YYYYMMDD'] = str
        return dtypes

    def __get_coordinates_from_postcode_NL(self, postalcode:str, cnt='NL'):
        geolocator = Nominatim(user_agent="knmi-app")
        location = geolocator.geocode(query={'postalcode':postalcode, 'country':cnt})
        return location

    def __find_closest_station(self, lat:int, lng:int) -> knmi.metadata.Station:
        stations = [knmi.stations.get(station) for station in self.__downloaded_station_numbers]
        minDistance = geodesic((lat, lng), (stations[0].latitude, stations[0].longitude)).meters
        for station in stations:
            tempDistance = geodesic((lat, lng), (station.latitude, station.longitude)).meters
            if tempDistance < minDistance:
                minDistance = tempDistance
                closestStation = station
        return closestStation
    
    def find_df(self, postcode:str) -> pd.DataFrame:
        """This method gets the df of the closest station"""
        cord= self.__get_coordinates_from_postcode_NL(postcode)
        station = self.__find_closest_station(cord.latitude, cord.longitude)
        return self.__stations[station.number]

    def for_eleena(self, postcode:str) -> pd.DataFrame:
        df = self.find_df(postcode)
        eleena_df = df[['T', 'SQ', 'DR', 'N', 'Q']] / 10
        eleena_df['Q'] = eleena_df['Q'] / 36
        eleena_df.insert(0, 'date', pd.to_datetime(df['YYYYMMDD']) + pd.to_timedelta(df['HH'],'h'))
        return eleena_df

