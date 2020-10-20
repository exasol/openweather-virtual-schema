--/
CREATE OR REPLACE PYTHON3 ADAPTER SCRIPT openweather_vs_scripts.openweather_adapter AS 
import requests
import sys
from pathlib import Path


def download_python_files():
    """Downloads the adapter script and logger code from github"""
    python_code_github_links = ["https://raw.githubusercontent.com/exasol/openweather-virtual-schema/master/openweather_adapter.py",
                                "https://raw.githubusercontent.com/exasol/openweather-virtual-schema/master/plain_text_tcp_handler.py"]

    file_names = ['openweather_adapter.py', 'plain_text_tcp_handler.py']
    for ind, link in enumerate(python_code_github_links):
        file_path = f"tmp/{file_names[ind]}"
        Path("tmp/").mkdir(parents=True, exist_ok=True)
        r = requests.get(link, allow_redirects=True)

        with open(file_path, 'wb') as f:
            f.write(r.content)
            

def adapter_call(request) -> str:
    """Public entry point to any adapter script on Exasol"""
    download_python_files()
    sys.path.append('tmp/')
    from openweather_adapter import AdapterCallHandler
    
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
import requests
import sys
from pathlib import Path


def download_python_files():
    """Downloads the UDF and TCP logger code from github"""
    python_code_github_links = ["https://raw.githubusercontent.com/exasol/openweather-virtual-schema/master/api_handler.py",
                                "https://raw.githubusercontent.com/exasol/openweather-virtual-schema/master/plain_text_tcp_handler.py"]

    file_names = ['api_handler.py', 'plain_text_tcp_handler.py']
    for ind, link in enumerate(python_code_github_links):
        file_path = f"tmp/{file_names[ind]}"
        Path("tmp/").mkdir(parents=True, exist_ok=True)
        r = requests.get(link, allow_redirects=True)

        with open(file_path, 'wb') as f:
            f.write(r.content)

def run(ctx) -> None:
    """Public run method as entry point to any Python UDF on Exasol"""
    download_python_files()
    sys.path.append('tmp/')
    from api_handler import ApiHandler
    
    api_handler = ApiHandler(ctx)

    api_handler.logger.info('>>>>API CALL<<<<')
    api_handler.logger.info(f'URL PARAMETER SET \n{ctx.api_parameters}\n')

    api_handler.api_calls()
/

--/
CREATE VIRTUAL SCHEMA openweather
USING openweather_vs_scripts.openweather_adapter
WITH API_KEY = '...'
     LOG_LISTENER = '0.0.0.0'   --IP Address
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
