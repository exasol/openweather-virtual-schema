----Cleaning up 
DROP FORCE VIRTUAL SCHEMA openweather CASCADE;
DROP SCHEMA openweather_vs_scripts CASCADE;
CREATE SCHEMA openweather_vs_scripts;


--/
CREATE OR REPLACE PYTHON3 ADAPTER SCRIPT openweather_vs_scripts.openweather_adapter AS 

def adapter_call(request) -> str:
    """Public entry point to any adapter script on Exasol"""
    call_handler = AdapterCallHandler(request)
    return call_handler.controll_request_processing()
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


class PlainTextTcpHandler(logging.handlers.SocketHandler):
    """ Sends plain text log message over TCP channel """

    def makePickle(self, record):
        message = self.formatter.format(record) + "\r\n"
        return message.encode()

    @staticmethod
    def initialize_logger(ip, port, level):

        root_logger = logging.getLogger('')

        try:
            root_logger.setLevel(level)
            if level in ('INFO', 'WARNING', 20, 30):
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


class ApiHandler:
    def __init__(self, ctx):
        self.ctx = ctx
        self.api_host: str = ctx.api_host
        self.api_method: str = ctx.api_method
        self.api_key: str = ctx.api_key

        try:
            self.parameter_expressions = json.loads(ctx.api_parameters)
        except json.decoder.JSONDecodeError:
            self.parameter_expressions = ctx.api_parameters

        self.logger = PlainTextTcpHandler.initialize_logger(ctx.logger_ip, int(ctx.logger_port), int(ctx.logger_level))

    def api_calls(self) -> None:
        """Takes the API parameter expression(s) the UDF was called with and unpacks them if they are a list. After
        unpacking the values the class proceeds with calling the API with the respective parameters and emitting the
        results."""
        if type(self.parameter_expressions) == list:
            self.__unpack_parameter_expression_list()
        else:
            self.__request_api_and_emit(self.parameter_expressions)

    def __unpack_parameter_expression_list(self) -> None:
        for expression in self.parameter_expressions:
            # -- Handle Null values sent from the adapter
            if (type(expression) == list and any(not element for element in expression)) or not expression:
                continue
            elif type(expression) == list and (all(element.startswith('id') for element in expression) or all(
                    element.startswith('q') for element in expression)):
                self.__unpack_const_list_expression(expression)
            elif type(expression) == list and any(element.startswith('zip') for element in expression):
                self.__handle_zip_code_expression(expression)
            elif type(expression) == list:
                self.__handle_geo_lookup_expression(expression)
            else:
                self.__request_api_and_emit(expression)

    def __unpack_const_list_expression(self, expression: list) -> None:
        for literal in expression:
            self.__request_api_and_emit(literal)

    def __handle_zip_code_expression(self, expression: list) -> None:
        reg = re.compile('zip')
        zip_index: int = expression.index(list(filter(reg.match, expression))[0])  # --Reverse ZIP, Country Code if reversed
        self.__request_api_and_emit(f'{expression[zip_index]}{expression[abs(zip_index - 1)]}')

    def __handle_geo_lookup_expression(self, expression: str) -> None:
        parameter: str = '&'.join(expression)
        self.__request_api_and_emit(parameter)

    def __request_api_and_emit(self, param: str) -> None:
        self.logger.info(f'REQUESTNG API WITH: {param}')

        try:
            response: requests.Response = self.__api_request(param)
            json_response_object: dict = json.loads(response.text)
        except requests.Timeout as e:
            e.message: str = f'E-VW-OWFS-8 API request with parameter <{param}> timed out.'

        if response and response.status_code == 200:
            if self.api_method == 'weather':
                self.__emit_current_weather(json_response_object)
            elif self.api_method == 'forecast':
                self.__emit_forecast(json_response_object)
        else:
            self.logger.error('')

    def __api_request(self, param: str) -> requests.Response:
        request: str = f"{self.api_host}{self.api_method}?{param}&units=metric&appid={self.api_key}"
        self.logger.info(f'REQUEST STRING: {request}\n\n\n')
        return requests.get(request)

    def __emit_current_weather(self, json_dict: dict) -> None:
        coord_group = json_dict.get('coord') if json_dict.get('coord') else {}
        main_group = json_dict.get('main') if json_dict.get('main') else {}
        weather_group = json_dict.get('weather') if json_dict.get('weather') else {}
        wind_group = json_dict.get('wind') if json_dict.get('wind') else {}
        clouds_group = json_dict.get('clouds') if json_dict.get('clouds') else {}
        rain_group = json_dict.get('rain') if json_dict.get('rain') else {}
        snow_group = json_dict.get('snow') if json_dict.get('snow') else {}
        sys_group = json_dict.get('sys') if json_dict.get('sys') else {}

        self.ctx.emit(sys_group.get('country'),  # --country code
                      json_dict.get('name'),  # --city name
                      json_dict.get('id'),  # --location id
                      datetime.datetime.fromtimestamp(int(json_dict.get('dt').__str__())),
                      # --time of data collection in UNIX format -> converted to DB compatible datetime-string
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
                      datetime.datetime.fromtimestamp(int(sys_group.get('sunrise').__str__())),
                      # --sunrise time in UNIX format conversion like data collection time
                      datetime.datetime.fromtimestamp(int(sys_group.get('sunset').__str__())),
                      # --sunrise time in UNIX format conversion like data collection time
                      json_dict.get('timezone') / 3600,  # --shift in seconds from UTC -> converted to hours shift
                      None)

    def __emit_forecast(self, json_dict: dict) -> None:
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

            self.ctx.emit(city_group.get('country'),  # --country code
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
                          datetime.datetime.fromtimestamp(int(city_group.get('sunrise').__str__())),
                          # --sunrise time in UNIX format conversion like data collection time
                          datetime.datetime.fromtimestamp(int(city_group.get('sunset').__str__())),
                          # --sunrise time in UNIX format conversion like data collection time
                          city_group.get('timezone') / 3600,  # --shift in seconds from UTC -> converted to hours shift
                          None)


def run(ctx) -> None:
    """Public run method as entry point to any Python UDF on Exasol"""
    api_handler = ApiHandler(ctx)

    api_handler.logger.info('>>>>API CALL<<<<')
    api_handler.logger.info(f'URL PARAMETER SET \n{ctx.api_parameters}\n')

    api_handler.api_calls()
/

--/
CREATE VIRTUAL SCHEMA openweather
USING openweather_vs_scripts.openweather_adapter
WITH API_KEY = 'd5ea350b0a22f5ba4e4b8f8570bd7c73'
     LOG_LISTENER = '192.168.177.83'   --IP Address
     LOG_LISTENER_PORT = '3333'         --Port
     LOG_LEVEL = 'INFO'                 --INFO or WARNING
/

-- Test Current_Weather
SELECT * FROM OPENWEATHER.CURRENT_WEATHER
WHERE  city_name = 'München' OR
        latitude = 41.89 AND longitude = 12.48 OR 
        'Los Angeles' = city_name OR
        latitude = 'm' AND longitude = 8.05 OR
        city_id = 'Bremen' OR
        3060972 = city_id OR
        zip = 96050 AND country_code = 'DE' OR
        country_code = 'US' AND zip = 10301 OR
        city_id IN (2759794, 3247449, 2957773) OR
        city_name IN ('Memphis', 'Zirndorf', 'Kassel');

---- Test forecast
SELECT * FROM OPENWEATHER.FORECAST
WHERE   city_name = 'Los Angeles' OR
        latitude = 41.89 AND longitude = 12.48 OR
        'Berlin' = city_name OR
        latitude = 52.27 AND longitude = 8.05 OR
        city_id = 2874225 OR
        3060972 = city_id OR
        zip = 96050 AND country_code = 'DE' OR
        country_code = 'US' AND zip = 10301 OR
        city_id IN (
        2759794, 3247449, 2957773) OR
        city_name IN ('Minusio', 'Zirndorf', 'Kassel');