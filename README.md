# KNMI hourly weather data API

This project is a simple API over the historical hourly weather data information updated daily by KNMI ranging from 2001 to 2021 (https://www.knmi.nl/nederland-nu/klimatologie/uurgegevens).


## Installation

Use the package manager [pip](https://pypi.org/project/KNMIuurgegevens/) to install.

```bash
pip install KNMIuurgegevens
```

## Usage

```python
from KNMIuurgegevens.KNMI import KNMI

# When download set to true it will remove and download files.
knmi = KNMI(download=True)

# If hourly data files are already downloaded
knmi = KNMI()

# returns station data by station id
df = knmi[277]

# returns closest station data by postcode
df = knmi.find_df('1092AX')
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)