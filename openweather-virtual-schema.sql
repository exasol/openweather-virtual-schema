----Cleaning up 
DROP FORCE VIRTUAL SCHEMA openweather CASCADE;
DROP SCHEMA openweather_vs_scripts CASCADE;
CREATE SCHEMA openweather_vs_scripts;

--/
CREATE OR REPLACE PYTHON3 ADAPTER SCRIPT openweather_vs_scripts.openweather_adapter AS 
import json 
import string 
import logging.handlers 
import itertools


# -- Overwrite logger class for remote logging
class PlainTextTcpHandler(logging.handlers.SocketHandler):
    """ Sends plain text log message over TCP channel """

    def makePickle(self, record):
        message = self.formatter.format(record) + "\r\n"
        return message.encode()


def initialize_logger(ip, port, level):
    root_logger = logging.getLogger('')
    
    try:
        if level in ('INFO', 'WARNING'):
            root_logger.setLevel(level)
        else:
            raise TypeError()
    except TypeError as e:
        e.message = "E-VS-OWFS-4 Chosen LOG_LEVEL not supported. Please choose 'INFO' or 'WARNING'"
        raise
    
    socket_handler = PlainTextTcpHandler(ip, port)
    socket_handler.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
    root_logger.addHandler(socket_handler)
    return root_logger

def adapter_call(request):
    request_json_object = json.loads(request)
    request_type = request_json_object["type"]
    
    API_URL = 'https://api.openweathermap.org/data/2.5/'
    API_KEY = request_json_object['schemaMetadataInfo']['properties']['API_KEY']
    LOG_LISTENER = request_json_object['schemaMetadataInfo']['properties']['LOG_LISTENER']
    LOG_LISTENER_PORT = int(request_json_object['schemaMetadataInfo']['properties']['LOG_LISTENER_PORT'])
    LOG_LEVEL = request_json_object['schemaMetadataInfo']['properties']['LOG_LEVEL']
    
    logger = initialize_logger(LOG_LISTENER, LOG_LISTENER_PORT, LOG_LEVEL)
    
    if request_type == "createVirtualSchema":
        return handle_create_virtual_schema(logger)
    elif request_type == "dropVirtualSchema":
        return json.dumps({"type": "dropVirtualSchema"})
    elif request_type == "refresh":
        return json.dumps({"type": "refresh"})
    elif request_type == "setProperties":
        return json.dumps({"type": "setProperties"})
    if request_type == "getCapabilities":
        return json.dumps({"type": "getCapabilities",
                           "capabilities": ["FILTER_EXPRESSIONS", "LITERAL_STRING", "LITERAL_DOUBLE",
                                            "LITERAL_EXACTNUMERIC", "FN_PRED_OR", "FN_PRED_AND",
                                            "FN_PRED_EQUAL", "FN_PRED_IN_CONSTLIST"]})
    elif request_type == "pushdown":
        return handle_pushdown(request_json_object, API_URL, API_KEY, logger)
    else:
        raise ValueError('F-VS-OWFS-1 Unsupported adapter callback')

def generate_current_weather_sql(api_host, api_key, api_method, api_parameters, log_ip, log_port, log_level):
    sql = f'SELECT openweather_vs_scripts.api_handler(\'{api_host}\', \
                                                    \'{api_method}\', \
                                                    \'{api_parameters}\', \
                                                    \'{api_key}\', \
                                                    \'{log_ip}\', \
                                                    \'{log_port}\', \
                                                    \'{log_level}\') \
                                                    EMITS (country_code VARCHAR(200), \
                                                            city_name VARCHAR(200), \
                                                            city_id INT, \
                                                            data_collection_time TIMESTAMP, \
                                                            longitude DOUBLE, \
                                                            latitude DOUBLE, \
                                                            weather_id INT, \
                                                            weather_group VARCHAR(200), \
                                                            weather_description VARCHAR(2000), \
                                                            weather_icon_id VARCHAR(20), \
                                                            temperature DOUBLE, \
                                                            felt_temperature DOUBLE, \
                                                            min_temperature DOUBLE, \
                                                            max_temperature DOUBLE, \
                                                            atmospheric_pressure DOUBLE, \
                                                            relative_humidity INT, \
                                                            atmospheric_pressure_sea_level DOUBLE, \
                                                            atmospheric_pressure_ground_level DOUBLE, \
                                                            wind_speed DOUBLE, \
                                                            wind_direction INT, \
                                                            wind_gust DOUBLE, \
                                                            cloudiness INT, \
                                                            rain_1h DOUBLE, \
                                                            rain_3h DOUBLE, \
                                                            snow_1h DOUBLE, \
                                                            snow_3h DOUBLE, \
                                                            visibility INT, \
                                                            sunrise TIMESTAMP, \
                                                            sunset TIMESTAMP, \
                                                            timezone_shift INT, \
                                                            zip VARCHAR(200))'
    return sql

def generate_forecast_sql(api_host, api_key, api_method, api_parameters, log_ip, log_port, log_level):
    sql = f'SELECT openweather_vs_scripts.api_handler(\'{api_host}\', \
                                                    \'{api_method}\', \
                                                    \'{api_parameters}\', \
                                                    \'{api_key}\', \
                                                    \'{log_ip}\', \
                                                    \'{log_port}\', \
                                                     \'{log_level}\') \
                                                    EMITS (country_code VARCHAR(200), \
                                                            city_name VARCHAR(200), \
                                                            city_id INT, \
                                                            forecast_time TIMESTAMP, \
                                                            longitude DOUBLE, \
                                                            latitude DOUBLE, \
                                                            weather_id INT, \
                                                            weather_group VARCHAR(200), \
                                                            weather_description VARCHAR(2000), \
                                                            weather_icon_id VARCHAR(20), \
                                                            temperature DOUBLE, \
                                                            felt_temperature DOUBLE, \
                                                            min_temperature DOUBLE, \
                                                            max_temperature DOUBLE, \
                                                            atmospheric_pressure DOUBLE, \
                                                            relative_humidity INT, \
                                                            atmospheric_pressure_sea_level DOUBLE, \
                                                            atmospheric_pressure_ground_level DOUBLE, \
                                                            wind_speed DOUBLE, \
                                                            wind_direction INT, \
                                                            percipitation_probability DOUBLE, \
                                                            cloudiness INT, \
                                                            rain_3h DOUBLE, \
                                                            snow_3h DOUBLE, \
                                                            visibility INT, \
                                                            sunrise TIMESTAMP, \
                                                            sunset TIMESTAMP, \
                                                            timezone_shift INT, \
                                                            zip VARCHAR(200))'
    return sql

def handle_predicate_equal(filter_json, logger):
    # -- Check if expressions are reversed
    if not filter_json.get('right').get('value'):
        filter_value = filter_json['left']['value']
        filter_name = filter_json['right']['name']
    else:
        filter_value = filter_json['right']['value']
        filter_name = filter_json['left']['name']

    logger.info(f'Filter name: {filter_name} || Filter value: {filter_value}')
    
    api_parameter_key_mapping = {'CITY_NAME':'q=',
                                'LONGITUDE':'lon=',
                                'LATITUDE':'lat=',
                                'CITY_ID':'id=',
                                'ZIP':'zip=',
                                'COUNTRY_CODE':','
                                }
    
    if filter_name in ('CITY_NAME', 'COUNTRY_CODE'):
        try:
            float(filter_value)
            raise TypeError()
        except ValueError:
            return f"{api_parameter_key_mapping[filter_name]}{filter_value}"
        except TypeError as e:
            e.message = f'E-VS-OWFS-2 {filter_name} column filter does not accept numbers. Found <{filter_value}>.'
            raise
    elif filter_name in ('LONGITUDE', 'LATITUDE'):
        try:
            float(filter_value)
            return f"{api_parameter_key_mapping[filter_name]}{filter_value}"
        except ValueError as e:
            e.message = f'E-VS-OWFS-3 {filter_name} column filter only accepts numbers. Found <{filter_value}>.'
            raise
    elif filter_name in ('CITY_ID','ZIP'):
        try:
            int(filter_value)
            return f"{api_parameter_key_mapping[filter_name]}{filter_value}"
        except ValueError as e:
            e.message = f'E-VS-OWFS-5 {filter_name} column filter only accepts whole numbers. Found <{filter_value}>.'
            raise
    else:
        raise KeyError(
            f'E-VS-OWFS-1 Filtering not supported on column {filter_name} in PREDICATE_EQUAL expression.')

def parse_filters(filters, logger):
    buffer = []

    logger.info('>>>>>FILTER<<<<<')
    logger.info(f'{filters}')

    # -- If true filter is 'IN_CONSTLIST'
    if filters.get('arguments'):
        for argument in filters.get('arguments'):
            e = {'left': {'name': filters.get('expression').get('name')},
                 'right': {'value': argument.get('value')}, 'type': 'predicate_equal'}
            buffer.append(parse_filters(e, logger))
    # -- Is filters a single filter or a list of filters?
    elif filters.get('expressions'):
        for f in filters.get('expressions'):
            buffer.append(parse_filters(f, logger))
    # -- Leaf element has to be of type 'predicate_equal' because this is the only predicate on leaf level that is supported
    else:
        try:
            return handle_predicate_equal(filters, logger)
        except (ValueError, KeyError) as e:
            logger.warning(e.message)
            return None
    return buffer

def parse_api_method_from_name(name):
    if name == 'CURRENT_WEATHER':
        return 'weather'
    elif name == 'FORECAST':
        return 'forecast'

def build_sql(api_url, api_key, request_json_object, logger):
    api_method = parse_api_method_from_name(request_json_object['pushdownRequest']['from']['name'])
    filters = parse_filters(request_json_object['pushdownRequest']['filter'], logger)
    
    log_ip = logger.handlers[0].host
    log_port = logger.handlers[0].port
    log_level = logger.level
    
    logger.info(f'\n\n\nAPI FILTERS {filters}')
    
    if api_method == 'weather':
        return generate_current_weather_sql(api_url, api_key, api_method, json.dumps(filters), log_ip, log_port, log_level)
    elif api_method == 'forecast':
        return generate_forecast_sql(api_url, api_key, api_method, json.dumps(filters), log_ip, log_port, log_level)

def handle_create_virtual_schema(logger):
    result = {"type": "createVirtualSchema",
           "schemaMetadata": {"tables": []}
           }

    current_weather = {"name": "CURRENT_WEATHER",
                       "columns":
                           [
                               {"name": "COUNTRY_CODE",
                                "dataType": {"type": "VARCHAR", "size": 200},
                                "comment": "Countrycode of the country"},
                               {"name": "CITY_NAME",
                                "dataType": {"type": "VARCHAR", "size": 200},
                                "comment": "The name of the city"},
                               {"name": "CITY_ID",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "The ID of the city. Reference: https://openweathermap.org/find?q="},
                               {"name": "DATA_COLLECTION_TIME",
                                "dataType": {"type": "TIMESTAMP"},
                                "comment": "The timestamp when the weather data was collected in UTC."},
                               {"name": "LONGITUDE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 4},
                                "comment": "The longitude of the city"},
                               {"name": "LATITUDE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 4},
                                "comment": "The latitude of the city"},
                               {"name": "WEATHER_ID",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "The ID of the current weather condition. Reference: https://openweathermap.org/weather-conditions#Weather-Condition-Codes-2"},
                               {"name": "WEATHER_GROUP",
                                "dataType": {"type": "VARCHAR", "size": 200},
                                "comment": "The group name of the overall weather situation"},
                               {"name": "WEATHER_DESCRIPTION",
                                "dataType": {"type": "VARCHAR", "size": 2000},
                                "comment": "The specific sub-group of weather conditions"},
                               {"name": "WEATHER_ICON_ID",
                                "dataType": {"type": "VARCHAR", "size": 20},
                                "comment": "The weather icon ID. Reference: https://openweathermap.org/weather-conditions"},
                               {"name": "TEMPERATURE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Temperature in degrees centigrade"},
                               {"name": "FELT_TEMPERATURE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Temperature as humans perceive it in degrees centigrade"},
                               {"name": "MIN_TEMPERATURE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Lowest currently recorded temperature in degrees centigrade"},
                               {"name": "MAX_TEMPERATURE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Highest currently recorded temperature in degrees centigrade"},
                               {"name": "ATMOSPHERIC_PRESSURE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Atmospheric pressure (on the sea level, if there is no sea_level or grnd_level data) in hPa"},
                               {"name": "RELATIVE_HUMIDITY",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "Relative humidity in %"},
                               {"name": "ATMOSPHERIC_PRESSURE_SEA_LEVEL",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Atmospheric pressure on the sea level in hPa"},
                               {"name": "ATMOSPHERIC_PRESSURE_GROUND_LEVEL",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Atmospheric pressure on the ground level in hPa"},
                               {"name": "WIND_SPEED",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Wind speed in m/s"},
                               {"name": "WIND_DIRECTION",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "Wind direction in meterological degrees"},
                               {"name": "WIND_GUST",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Wind gust in m/s"},
                               {"name": "CLOUDINESS",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "Cloudiness in % sky coverage"},
                               {"name": "RAIN_1H",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Rain volume for the last 1 hour in mm"},
                               {"name": "RAIN_3H",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Rain volume for the last 3 hours in mm"},
                               {"name": "SNOW_1H",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Snow volume for the last 1 hour in mm"},
                               {"name": "SNOW_3H",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Snow volume for the last 3 hours in mm"},
                               {"name": "VISIBILITY",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "Visibility in m"},
                               {"name": "SUNRISE",
                                "dataType": {"type": "TIMESTAMP"},
                                "comment": "Sunrise time in UTC"},
                               {"name": "SUNSET",
                                "dataType": {"type": "TIMESTAMP"},
                                "comment": "Sunset time in UTC"},
                               {"name": "TIMEZONE_SHIFT",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "Hour shift from UTC to timezone of the specific city. E.g. 2 means UTC+2"},
                               {"name": "ZIP",
                                "dataType": {"type": "VARCHAR", "size": 200},
                                "comment": "Dummy column for filtering API call by ZIP code."}
                           ]}

    weather_forecast = {"name": "FORECAST",
                       "columns":
                           [
                               {"name": "COUNTRY_CODE",
                                "dataType": {"type": "VARCHAR", "size": 200},
                                "comment": "Countrycode of the country"},
                               {"name": "CITY_NAME",
                                "dataType": {"type": "VARCHAR", "size": 200},
                                "comment": "The name of the city"},
                               {"name": "CITY_ID",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "The ID of the city. Reference: https://openweathermap.org/find?q="},
                               {"name": "FORECAST_TIME",
                                "dataType": {"type": "TIMESTAMP"},
                                "comment": "The timestamp of the forecast."},
                               {"name": "LONGITUDE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 4},
                                "comment": "The longitude of the city"},
                               {"name": "LATITUDE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 4},
                                "comment": "The latitude of the city"},
                               {"name": "WEATHER_ID",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "The ID of the forecasted weather condition. Reference: https://openweathermap.org/weather-conditions#Weather-Condition-Codes-2"},
                               {"name": "WEATHER_GROUP",
                                "dataType": {"type": "VARCHAR", "size": 200},
                                "comment": "The group name of the forecasted weather situation"},
                               {"name": "WEATHER_DESCRIPTION",
                                "dataType": {"type": "VARCHAR", "size": 2000},
                                "comment": "The specific sub-group of weather conditions"},
                               {"name": "WEATHER_ICON_ID",
                                "dataType": {"type": "VARCHAR", "size": 20},
                                "comment": "The weather icon ID. Reference: https://openweathermap.org/weather-conditions"},
                               {"name": "TEMPERATURE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Temperature in degrees centigrade"},
                               {"name": "FELT_TEMPERATURE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Temperature as humans perceive it in degrees centigrade"},
                               {"name": "MIN_TEMPERATURE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Lowest currently recorded temperature in degrees centigrade"},
                               {"name": "MAX_TEMPERATURE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Highest currently recorded temperature in degrees centigrade"},
                               {"name": "ATMOSPHERIC_PRESSURE",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Atmospheric pressure (on the sea level, if there is no sea_level or grnd_level data) in hPa"},
                               {"name": "RELATIVE_HUMIDITY",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "Relative humidity in %"},
                               {"name": "ATMOSPHERIC_PRESSURE_SEA_LEVEL",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Atmospheric pressure on the sea level in hPa"},
                               {"name": "ATMOSPHERIC_PRESSURE_GROUND_LEVEL",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Atmospheric pressure on the ground level in hPa"},
                               {"name": "WIND_SPEED",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Wind speed in m/s"},
                               {"name": "WIND_DIRECTION",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "Wind direction in meterological degrees"},
                               {"name": "PERCIPITATION_PROBABILITY",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Probability of percipitation"},
                               {"name": "CLOUDINESS",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "Cloudiness in % sky coverage"},
                               {"name": "RAIN_3H",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Rain volume for the last 3 hours in mm"},
                               {"name": "SNOW_3H",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2},
                                "comment": "Snow volume for the last 3 hours in mm"},
                               {"name": "VISIBILITY",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "Visibility in m"},
                               {"name": "SUNRISE",
                                "dataType": {"type": "TIMESTAMP"},
                                "comment": "Sunrise time in UTC"},
                               {"name": "SUNSET",
                                "dataType": {"type": "TIMESTAMP"},
                                "comment": "Sunset time in UTC"},
                               {"name": "TIMEZONE_SHIFT",
                                "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0},
                                "comment": "Hour shift from UTC to timezone of the specific city. E.g. 2 means UTC+2"},
                               {"name": "ZIP",
                                "dataType": {"type": "VARCHAR", "size": 200},
                                "comment": "Dummy column for filtering API call by ZIP code."}
                           ]}

    result["schemaMetadata"]["tables"].append(current_weather)
    result["schemaMetadata"]["tables"].append(weather_forecast)
    return json.dumps(result)



def handle_pushdown(request_json_object, api_url, api_key, logger):
    logger.info('>>>>PUSHDOWN<<<<')
    logger.info(f'{json.dumps(request_json_object)}\n\n\n')

    sql = build_sql(api_url, api_key, request_json_object, logger)
    result = {
        "type": "pushdown",
        "sql": sql
    }

    logger.info('>>>>ADAPTER SQL<<<<<')
    logger.info(f'{json.dumps(sql)}\n\n\n>')
    return json.dumps(result)
/

--/
--Adapter uses this UDF to request the API
CREATE OR REPLACE PYTHON3 SET SCRIPT openweather_vs_scripts.api_handler(api_host varchar(100),
                                                                        api_method varchar(100),
                                                                        api_parameters varchar(2000),
                                                                        api_key varchar(50),
                                                                        logger_ip varchar(20),
                                                                        logger_port varchar(10),
                                                                        logger_level varchar(10))
EMITS(...) AS
import logging.handlers
import json
import datetime
import re
import requests

# -- Overwrite logger class for remote logging
class PlainTextTcpHandler(logging.handlers.SocketHandler):
    """ Sends plain text log message over TCP channel """

    def makePickle(self, record):
        message = self.formatter.format(record) + "\r\n"
        return message.encode()


def api_request(host, method, param, api_key, logger):
    request_string = f"{host}{method}?{param}&units=metric&appid={api_key}"
    logger.info(f'REQUEST STRING: {request_string}\n\n\n')
    return requests.get(request_string)


def emit_current_weather(ctx, json_dict):
    coord_group = json_dict.get('coord') if json_dict.get('coord') else {}
    main_group = json_dict.get('main') if json_dict.get('main') else {}
    weather_group = json_dict.get('weather') if json_dict.get('weather') else {}
    wind_group = json_dict.get('wind') if json_dict.get('wind') else {}
    clouds_group = json_dict.get('clouds') if json_dict.get('clouds') else {}
    rain_group = json_dict.get('rain') if json_dict.get('rain') else {}
    snow_group = json_dict.get('snow') if json_dict.get('snow') else {}
    sys_group = json_dict.get('sys') if json_dict.get('sys') else {}

    ctx.emit(sys_group.get('country'),  # --country code
             json_dict.get('name'),  # --city name
             json_dict.get('id'),  # --location id
             datetime.datetime.fromtimestamp(int(json_dict.get('dt').__str__())), # --time of data collection in UNIX format -> converted to DB compatible datetime-string
             coord_group.get('lon'),  # --longitude
             coord_group.get('lat'),  # --latitude
             weather_group[0].get('id'),  # --weather id
             weather_group[0].get('main'),  # --weather group
             weather_group[0].get('description'),  # --weather condition in the group
             weather_group[0].get('icon'),  # --weather icon id
             main_group.get('temp'),  # --temperature in degrees centigrade
             main_group.get('feels_like'),  # --felt temperature in degrees centigrade
             main_group.get('temp_min'),  # --min temperature in degrees centigrade
             main_group.get('temp_max'),  # --max temperature in degrees centigradelsius
             main_group.get('pressure'),  # --atmospheric pressure in hPa
             main_group.get('humidity'),  # --relative humidity in %
             main_group.get('sea_level'),  # --atmospheric pressure on the sea level
             main_group.get('grnd_level'),  # --atmospheric pressure on the ground level
             wind_group.get('speed'),  # --wind speed m/s
             wind_group.get('deg'),  # --wind direction
             wind_group.get('gust'),  # --wind gust
             clouds_group.get('all'),  # --cloudiness in %
             rain_group.get('1h'),  # --rain volume in the last hour in mm
             rain_group.get('3h'),  # --rain volume in the last 3 hours in mm
             snow_group.get('1h'),  # --snow volume in the last hour in mm
             snow_group.get('3h'),  # --snow volume in the last 3 hours in mm
             json_dict.get('visibility'),  # --visibility in meters
             datetime.datetime.fromtimestamp(int(sys_group.get('sunrise').__str__())),             # --sunrise time in UNIX format conversion like data collection time
             datetime.datetime.fromtimestamp(int(sys_group.get('sunset').__str__())),             # --sunrise time in UNIX format conversion like data collection time
             json_dict.get('timezone') / 3600,  # --shift in seconds from UTC -> converted to hours shift
             None)


def emit_forecast(ctx, json_dict):
    list_group = json_dict.get('list') if json_dict.get('list') else {}
    city_group = json_dict.get('city') if json_dict.get('city') else {}
    coord_group = city_group.get('coord') if city_group else {}

    for record in list_group:
        main_group = record.get('main') if record.get('main') else {}
        weather_group = record.get('weather') if record.get('weather') else {}
        wind_group = record.get('wind') if record.get('wind') else {}
        clouds_group = record.get('clouds') if record.get('clouds') else {}
        rain_group = record.get('rain') if record.get('rain') else {}
        snow_group = record.get('snow') if record.get('snow') else {}
        sys_group = record.get('sys') if record.get('sys') else {}

        ctx.emit(city_group.get('country'),  # --country code
                 city_group.get('name'),  # --city name
                 city_group.get('id'),  # --location id
                 datetime.datetime.strptime(record.get('dt_txt'), '%Y-%m-%d %H:%M:%S'),  # --forecast timestamp
                 coord_group.get('lon'),  # --longitude
                 coord_group.get('lat'),  # --latitude
                 weather_group[0].get('id'),  # --weather id
                 weather_group[0].get('main'),  # --weather group
                 weather_group[0].get('description'),  # --weather condition in the group
                 weather_group[0].get('icon'),  # --weather icon id
                 main_group.get('temp'),  # --temperature in degrees centigrade
                 main_group.get('feels_like'),  # --felt temperature in degrees centigrade
                 main_group.get('temp_min'),  # --min temperature in degrees centigrade
                 main_group.get('temp_max'),  # --max temperature in degrees centigradelsius
                 main_group.get('pressure'),  # --atmospheric pressure in hPa
                 main_group.get('humidity'),  # --relative humidity in %
                 main_group.get('sea_level'),  # --atmospheric pressure on the sea level
                 main_group.get('grnd_level'),  # --atmospheric pressure on the ground level
                 wind_group.get('speed'),  # --wind speed m/s
                 wind_group.get('deg'),  # --wind direction
                 record.get('pop'),  # --Probability of precipitation
                 clouds_group.get('all'),  # --cloudiness in %
                 rain_group.get('3h'),  # --rain volume in the last 3 hours in mm
                 snow_group.get('3h'),  # --snow volume in the last 3 hours in mm
                 record.get('visibility'),  # --visibility in meters
                 datetime.datetime.fromtimestamp(int(city_group.get('sunrise').__str__())),  # --sunrise time in UNIX format conversion like data collection time
                 datetime.datetime.fromtimestamp(int(city_group.get('sunset').__str__())),  # --sunrise time in UNIX format conversion like data collection time
                 city_group.get('timezone') / 3600,  # --shift in seconds from UTC -> converted to hours shift
                 None)


def initialize_logger(ip, port, level):
    root_logger = logging.getLogger('')
    
    try:
        if level in ('INFO', '20', 'WARNING', '30'):
            try:
                level = int(level)
            except TypeError():
                pass
            root_logger.setLevel(level)
        else:
            raise TypeError()
    except TypeError as e:
        e.message = "E-VS-OWFS-4 Chosen LOG_LEVEL not supported. Please choose 'INFO' or 'WARNING'"
        raise
    
    socket_handler = PlainTextTcpHandler(ip, port)
    socket_handler.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
    root_logger.addHandler(socket_handler)
    return root_logger


def request_api_and_emit(ctx, param, logger):
    host = ctx['api_host']
    method = ctx['api_method']
    key = ctx['api_key']

    logger.info(f'REQUESTNG API WITH: {param}')
    try:
        response = api_request(host, method, param, key, logger)
        json_response_object = json.loads(response.text)
    except requests.Timeout as e:
        e.message = f'E-VW-OWFS-8 API request with parameter <{param}> timed out.'

    if response.status_code == 200:
        if method == 'weather':
            emit_current_weather(ctx, json_response_object)
        elif method == 'forecast':
            emit_forecast(ctx, json_response_object)
    else:
        logger.error('')


def api_calls(parameter_expressions, ctx, logger):
    if type(parameter_expressions) == list:
        for expression in parameter_expressions:
            # -- Handle Null values sent from the adapter
            if (type(expression) == list and any(not element for element in expression)) or not expression:
                continue
            # -- Handle const_list expressions
            elif type(expression) == list and (all(element.startswith('id') for element in expression) or all(element.startswith('q') for element in expression)):
                for literal in expression:
                    request_api_and_emit(ctx, literal, logger)
            # -- Handle zip code requests
            elif type(expression) == list and any(element.startswith('zip') for element in expression):
                reg = re.compile('zip')
                zip_index = expression.index(list(filter(reg.match, expression))[0])  # --Reverse ZIP, Country Code if reversed
                request_api_and_emit(ctx,
                                     f'{expression[zip_index]}{expression[abs(zip_index - 1)]}',
                                     logger)
            # -- Handle geo lookups
            elif type(expression) == list:
                parameter = '&'.join(expression)
                request_api_and_emit(ctx, parameter, logger)
            # -- Handle multiple single expressions
            else:
                request_api_and_emit(ctx, expression, logger)
    # -- Handle single expression
    else:
        request_api_and_emit(ctx, parameter_expressions, logger)


def run(ctx):
    logger = initialize_logger(ctx.logger_ip, int(ctx.logger_port), ctx.logger_level)

    logger.info('>>>>API CALL<<<<')
    logger.info(f'URL PARAMETER SET \n{ctx.api_parameters}\n')
    try:
        parameter_expressions = json.loads(ctx.api_parameters)
    except json.decoder.JSONDecodeError:
        parameter_expressions = ctx.api_parameters

    api_calls(parameter_expressions, ctx, logger)
/

--/
CREATE VIRTUAL SCHEMA openweather
USING openweather_vs_scripts.openweather_adapter
WITH API_KEY = 'd5ea350b0a22f5ba4e4b8f8570bd7c73'
     LOG_LISTENER = '192.168.177.83'
     LOG_LISTENER_PORT = '3333'
     LOG_LEVEL = 'INFO'
/

-- Test Current_Weather
select * from OPENWEATHER.CURRENT_WEATHER
where  city_name = 'München' or --  # 1 standard name lookup       ASSERT: Bremen
        latitude = 41.89 and longitude = 12.48 or --  # 2 standard geo lookup        ASSERT: Rome
        'Los Angeles' = city_name or --  # 3 reversed name lookup       ASSERT: Los Angeles
        latitude = 'm' and longitude = 8.05 or --  # 4 reversed geo lookup        ASSERT: Error - handled
        city_id = 'Bremen' or --  # 5 standard city_id lookup    ASSERT: Error - handled
        3060972 = city_id or --  # 6 reversed city_id lookup    ASSERT: Bratislava
        zip = 96050 and country_code = 'DE' or --  # 7 zip code lookup            ASSERT: Bamberg
        country_code = 'US' and zip = 10301 or --  # 8 rever zip code lookup      ASSERT: Staten Island
        city_id in (
        2759794, 3247449, 2957773) or --  # 9,10,11 in_constlist lookup with city_id     ASSERT: Amsterdam, Aachen, Altenburg
        city_name in ('Memphis', 'Zirndorf', 'Kassel'); --  # 12,13,14 ib_ constlist lookup with city_name ASSERT: Minusio, Zirndorf, Kassel'

---- Test forecast
--select * from OPENWEATHER.FORECAST
--where   city_name = 'Wiesbaden' or --  # 1 standard name lookup       ASSERT: Bremen
--        latitude = 41.89 and longitude = 12.48 or --  # 2 standard geo lookup        ASSERT: Rome
--        'Berlin' = city_name or --  # 3 reversed name lookup       ASSERT: Berlin
--        latitude = 52.27 and longitude = 8.05 or --  # 4 reversed geo lookup        ASSERT: Osnabrück
--        city_id = 2874225 or --  # 5 standard city_id lookup    ASSERT: Mainz
--        3060972 = city_id or --  # 6 reversed city_id lookup    ASSERT: Bratislava
--        zip = 96050 and country_code = 'DE' or --  # 7 zip code lookup            ASSERT: Bamberg
--        country_code = 'US' and zip = 10301 or --  # 8 rever zip code lookup      ASSERT: Staten Island
--        city_id in (
--        2759794, 3247449, 2957773) or --  # 9,10,11 in_constlist lookup with city_id     ASSERT: Amsterdam, Aachen, Altenburg
--        city_name in ('Minusio', 'Zirndorf', 'Kassel'); --  # 12,13,14 ib_ constlist lookup with city_name ASSERT: Minusio, Zirndorf, Kassel'
--                                                        -- Expect 560 rows