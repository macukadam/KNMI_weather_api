#!/usr/bin/env python3

__author__ = "Ugurcan Akpulat"
__copyright__ = "Copyright 2021, Eleena Software"
__credits__ = [""]
__license__ = "MIT"
__version__ = "1.0.0"
__maintainer__ = "Ugurcan Akpulat"
__email__ = "ugurcan.akpulat@gmail.com"
__status__ = "Production"

import aiohttp
import asyncio
import time
import os
import shutil
import traceback
from io import BytesIO
from zipfile import ZipFile
from .loading_screen import LoadingScreen


class KNMIDataLoader():
    """This class downloads the hourly weather data
        from KNMI klimatologie page and saves it to csv files at
        given path. Those files are updated daily.
    """
    def __init__(self, sbegin: int, send: int, data_type: str, dir_name=None):
        self.__type = data_type
        self.__sbegin = sbegin
        self.__send = send
 
        if not dir_name:
            if self.__type == 'hourly':
                dir_name = 'station_uur_temp'
            else:
                dir_name = 'station_daily_temp'

        _path = os.path.join(os.getcwd(), dir_name)
        self.__output_path = _path

        if os.path.exists(_path):
            shutil.rmtree(_path)
        os.makedirs(_path)

    def manupulate_text(self, zip: ZipFile, contained_file: str,
                        file_path: str) -> str:
        text = zip.open(contained_file).read().decode()
        text = text[text.index('STN,'):].replace(" ", "")
        text = text.replace("\n,", ",", 1)
        if os.path.exists(file_path):
            text = text[text.find('\n')+3:]
        return text

    def unpack_zip(self, content: aiohttp.ClientResponse, i: int) -> bool:
        with ZipFile(BytesIO(content)) as zip:
            file = f'{i}'
            for contained_file in zip.namelist():
                file_path = (os.path.join(self.__output_path, file + ".csv"))
                text = self.manupulate_text(zip, contained_file, file_path)
                with open(file_path, "a") as output:
                    output.write(text)
            return True

    def generate_tasks(self, s):
        tasks = []
        for i in range(self.__sbegin, self.__send):
            if self.__type == 'hourly':
                for j in ['2001-2010', '2011-2020', '2021-2030']:
                    url = f'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/uurgegevens/uurgeg_{i}_{j}.zip'
                    task = asyncio.ensure_future(self.save_to_file(s, url, i))
                    tasks.append(task)
            else:
                url = f'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/daggegevens/etmgeg_{i}.zip'
                task = asyncio.ensure_future(self.save_to_file(s, url, i))
                tasks.append(task)

        return tasks

    async def save_to_file(self, session: aiohttp.ClientSession, url: str,
                           i: int) -> bool:
        async with session.get(url) as resp:
            try:
                content = await resp.read()
                try:
                    if 'Error' in content.decode("utf-8"):
                        pass
                except UnicodeDecodeError:
                    if len(content) > 0:
                        return self.unpack_zip(content, i)
            except Exception:
                traceback.print_exc()

    async def fetch(self) -> None:
        async with aiohttp.ClientSession() as session:
            tasks = self.generate_tasks(session)
            tasks = await asyncio.gather(*tasks)
            for task_result in tasks:
                pass

    async def start(self, data_type) -> None:
        try:
            ls = LoadingScreen()
            ls.start(data_type)
            start_time = time.time()
            await self.fetch()
            ls.stop()
            print("--- %s seconds ---" % (time.time() - start_time))
        except Exception:
            traceback.print_exc()
