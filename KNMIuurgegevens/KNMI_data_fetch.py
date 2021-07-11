#!/usr/bin/env python3

__author__ = "Ugurcan Akpulat"
__copyright__ = "Copyright 2021, Eleena Software"
__credits__ = [""]
__license__ = "MIT"
__version__ = "1.0.0"
__maintainer__ = "Ugurcan Akpulat"
__email__ = "ugurcan.akpulat@gmail.com"
__status__ = "Production"

import aiohttp, asyncio, time, os, shutil
from io import BytesIO
from zipfile import ZipFile

class KNMIDataLoader():
    """This class downloads the hourly weather data 
        from KNMI klimatologie page and saves it to csv files at
        given path. Those files are updated daily.
    """
    
    
    def __init__(self, sbegin:int, send:int, dir_name='stations_tmp'):
        _path = os.path.join(os.getcwd(), dir_name)

        if os.path.exists(_path):
            shutil.rmtree(_path)
        
        os.makedirs(_path)

        self.__sbegin = sbegin
        self.__send = send
        self.__output_path = _path


    def manupulate_text(self, zip:ZipFile, contained_file:str, file_path:str) -> str:
        text = zip.open(contained_file).read().decode()
        text = text[text.index('STN,'):].replace(" ", "")
        text = text.replace("\n,",",",1)
        if os.path.exists(file_path):
            text = text[text.find('\n')+3:]
        return text


    def unpack_zip(self, content:aiohttp.ClientResponse, i:int) -> bool:
        with ZipFile(BytesIO(content)) as zip:
            file = f'{i}'
            for contained_file in zip.namelist():
                file_path = (os.path.join(self.__output_path, file + ".csv"))
                text = self.manupulate_text(zip, contained_file, file_path)
                with open(file_path, "a") as output:
                    output.write(text)
            return True


    async def save_to_file(self, session:aiohttp.ClientSession, url:str, i:int) -> bool:
        async with session.get(url) as resp:
            try:
                content = await resp.read()
                try:
                    if 'Error' in content.decode("utf-8"):
                        print('Not found: ' + url)
                except UnicodeDecodeError as e:
                    if len(content) > 0:
                        return self.unpack_zip(content, i)

            except Exception as e:
                print(str(e))


    async def fetch(self) -> None:
        async with aiohttp.ClientSession() as session:

            tasks = []
            for i in range(self.__sbegin, self.__send):
                for j in ['2001-2010', '2011-2020', '2021-2030']:
                    file = f'uurgeg_{i}_{j}.zip'
                    url = f'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/uurgegevens/{file}'
                    tasks.append(asyncio.ensure_future(self.save_to_file(session, url, i)))

            tasks = await asyncio.gather(*tasks)
            for task_result in tasks:
                print(task_result)

    async def start(self) -> None:
        start_time = time.time()
        await self.fetch()
        print("--- %s seconds ---" % (time.time() - start_time))

