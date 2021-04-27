import click
import requests
import pprint
import json

SAMPLE_API_KEY = '562f6213ad0815575de94f7b40671638'


def current_weather(location, api_key=SAMPLE_API_KEY):
    url = 'https://api.openweathermap.org/data/2.5/weather'

    query_params = {
        'q': location,
        'appid': api_key,
    }
    print(url, query_params)
    response = requests.get(url, params=query_params)

    return response.json()['weather'][0]['description']


@click.command()
@click.argument('location')
@click.option(
    '--api-key', '-a',
    envvar="API_KEY",
    help='your API key for the OpenWeatherMap API',
)
def main(location, api_key):
    """
    A little weather tool that shows you the current weather in a LOCATION of
    your choice. Provide the city name and optionally a two-digit country code.
    Here are two examples:
    1. London,UK
    2. Canmore
    You need a valid API key from OpenWeatherMap for the tool to work. You can
    sign up for a free account at https://openweathermap.org/appid.
    """
    weather = current_weather(location, api_key)
    print(f"The weather in {location} right now: {weather}.")


if __name__ == "__main__":
    main()