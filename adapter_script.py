import json
import logging.handlers

class AdapterCallHandler:
    API_URL = 'https://api.openweathermap.org/data/2.5/'

    def __init__(self, request):
        self.request_json_object: dict = json.loads(request)

        self.api_key: str = self.request_json_object['schemaMetadataInfo']['properties']['API_KEY']
        self.log_listener: str = self.request_json_object['schemaMetadataInfo']['properties']['LOG_LISTENER']
        self.log_listener_port = int(self.request_json_object['schemaMetadataInfo']['properties']['LOG_LISTENER_PORT'])
        self.log_level: str = self.request_json_object['schemaMetadataInfo']['properties']['LOG_LEVEL']

        self.logger = PlainTextTcpHandler.initialize_logger(self.log_listener, self.log_listener_port, self.log_level)

    def controll_request_processing(self) -> str:
        """Takes the parsed JSON request and decides based on the request type how to handle the request.
        :returns a JSON string that will be interpreted by the database."""
        request_type: str = self.request_json_object["type"]
        if request_type == "createVirtualSchema":
            return self.__handle_create_virtual_schema()
        elif request_type == "dropVirtualSchema":
            return json.dumps({"type": "dropVirtualSchema"})
        elif request_type == "refresh":
            return json.dumps({"type": "refresh"})
        elif request_type == "setProperties":
            return json.dumps({"type": "setProperties"})
        elif request_type == "getCapabilities":
            return json.dumps({"type": "getCapabilities",
                               "capabilities": ["FILTER_EXPRESSIONS", "LITERAL_STRING", "LITERAL_DOUBLE",
                                                "LITERAL_EXACTNUMERIC", "FN_PRED_OR", "FN_PRED_AND",
                                                "FN_PRED_EQUAL", "FN_PRED_IN_CONSTLIST"]})
        elif request_type == "pushdown":
            return self.__handle_pushdown()
        else:
            raise ValueError('F-VS-OWFS-1 Unsupported adapter callback')

    def __handle_create_virtual_schema(self) -> str:
        result = {"type": "createVirtualSchema",
                  "schemaMetadata": {"tables": []}
                  }

        current_weather: dict = self.__get_current_weather_table_json()
        weather_forecast: dict = self.__get_forecast_table_json()

        result["schemaMetadata"]["tables"].append(current_weather)
        result["schemaMetadata"]["tables"].append(weather_forecast)
        return json.dumps(result)

    def __get_current_weather_table_json(self) -> dict:
        return {"name": "CURRENT_WEATHER",
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

    def __get_forecast_table_json(self) -> dict:
        return {"name": "FORECAST",
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

    def __handle_pushdown(self) -> str:
        self.logger.info('>>>>PUSHDOWN<<<<')
        self.logger.info(f'{json.dumps(self.request_json_object)}\n\n\n')

        sql: str = self.__build_sql()
        result: dict = {
            "type": "pushdown",
            "sql": sql
        }

        self.logger.info('>>>>ADAPTER SQL<<<<<')
        self.logger.info(f'{json.dumps(sql)}\n\n\n>')
        return json.dumps(result)

    def __build_sql(self):
        api_method: str = self.__parse_api_method_from_name(self.request_json_object['pushdownRequest']['from']['name'])
        filters = self.parse_filters(self.request_json_object['pushdownRequest']['filter'])

        log_ip: str = self.logger.handlers[0].host
        log_port: int = self.logger.handlers[0].port
        log_level: int = self.logger.level

        self.logger.info(f'\n\n\nAPI FILTERS {filters}')

        if api_method == 'weather':
            return self.__generate_current_weather_sql(api_method, json.dumps(filters), log_ip, log_port, log_level)
        elif api_method == 'forecast':
            return self.__generate_forecast_sql(api_method, json.dumps(filters), log_ip, log_port, log_level)

    def __parse_api_method_from_name(self, name) -> str:
        if name == 'CURRENT_WEATHER':
            return 'weather'
        elif name == 'FORECAST':
            return 'forecast'

    def parse_filters(self, filters):
        buffer = []

        self.logger.info('>>>>>FILTER<<<<<')
        self.logger.info(f'{filters}')

        # -- If true filter is 'IN_CONSTLIST'
        if filters.get('arguments'):
            for argument in filters.get('arguments'):
                e: dict = {'left': {'name': filters.get('expression').get('name')},
                           'right': {'value': argument.get('value')}, 'type': 'predicate_equal'}
                buffer.append(self.parse_filters(e))
        # -- Is filters a single filter or a list of filters?
        elif filters.get('expressions'):
            for f in filters.get('expressions'):
                buffer.append(self.parse_filters(f))
        # -- Leaf element has to be of type 'predicate_equal' because this is the only predicate on leaf level that is supported
        else:
            try:
                return self.__handle_predicate_equal(filters)
            except (ValueError, KeyError) as err:
                self.logger.warning(err.message)
                return None
        return buffer

    def __handle_predicate_equal(self, filter_json) -> str:
        """Check if expressions are reversed"""

        if filter_json.get('right').get('value'):
            filter_value: str = filter_json['right']['value']
            filter_name: str = filter_json['left']['name']
        else:
            filter_value: str = filter_json['left']['value']
            filter_name: str = filter_json['right']['name']

        self.logger.info(f'Filter name: {filter_name} || Filter value: {filter_value}')

        api_parameter_key_mapping: dict = {'CITY_NAME': 'q=',
                                           'LONGITUDE': 'lon=',
                                           'LATITUDE': 'lat=',
                                           'CITY_ID': 'id=',
                                           'ZIP': 'zip=',
                                           'COUNTRY_CODE': ','
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
        elif filter_name in ('CITY_ID', 'ZIP'):
            try:
                int(filter_value)
                return f"{api_parameter_key_mapping[filter_name]}{filter_value}"
            except ValueError as e:
                e.message = f'E-VS-OWFS-5 {filter_name} column filter only accepts whole numbers. Found <{filter_value}>.'
                raise
        else:
            raise KeyError(
                f'E-VS-OWFS-1 Filtering not supported on column {filter_name} in PREDICATE_EQUAL expression.')

    def __generate_current_weather_sql(self, api_method, filters, log_ip, log_port, log_level) -> str:
        sql: str = f'SELECT openweather_vs_scripts.api_handler(\'{self.API_URL}\', \
                                                        \'{api_method}\', \
                                                        \'{filters}\', \
                                                        \'{self.api_key}\', \
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

    def __generate_forecast_sql(self, api_method, filters, log_ip, log_port, log_level) -> str:
        sql: str = f'SELECT openweather_vs_scripts.api_handler(\'{self.API_URL}\', \
                                                        \'{api_method}\', \
                                                        \'{filters}\', \
                                                        \'{self.api_key}\', \
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