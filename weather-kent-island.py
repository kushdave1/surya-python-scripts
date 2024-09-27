import requests
from datetime import date, timedelta

# Replace 'YOUR_API_KEY' with your actual NOAA API key
API_KEY = 'ltngevkpljTSgoluYYOVULznpWvrBsrw'

# Kent Island coordinates (latitude and longitude)
LAT = '38.9784'
LON = '-76.3144'

# Initialize counters
days_above_30mph = 0
days_above_40mph = 0

# Iterate over the last 20 Septembers
for i in range(20):
    # Calculate the year for the current iteration
    year = date.today().year - i

    # Loop through each day in September
    for day in range(1, 31):
        date_str = f'{year}-09-{day:02}'  # Format the date
        url = f'https://api.weather.com/v3/wx/conditions/historical.json?geocode={LAT},{LON}&date={date_str}&format=json&units=e&apiKey={API_KEY}'

        # Make the API request
        response = requests.get(url)
        data = response.json()

        if 'error' not in data:
            # Check max sustained wind speed and max wind gust
            max_wind_speed = data['imperial']['wspd']['max']
            max_wind_gust = data['imperial']['gust']['max']

            if max_wind_speed > 30:
                days_above_30mph += 1

            if max_wind_gust > 40:
                days_above_40mph += 1

# Print the results
print(f'Days with max sustained wind speeds above 30 mph: {days_above_30mph}')
print(f'Days with max wind gusts above 40 mph: {days_above_40mph}')