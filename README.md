# Openweather Virtual Schema

##### Please note that this is an open source project which is not officially supported by EXASOL. We will try to help you as much as possible, but can't guarantee anything since this is not an official EXASOL product.

## Overview
Exasol Virtual Schemas allow you to acces arbitrary data sources from inside your Exasol database. 
This functionality is mainly used to integrate relational database management systems but is not limited to it.
This project shows that even REST APIs can be connected to Exasol using Virtual Schema.

## Getting Started
In order to use the virtual schema you need an account with [Openweather](https://openweathermap.org/). A free account works for this example. With an account you will get an API key which you need to access the API. The key looks like this: `d5ea350b1a22f5ba4e4b8a8570bd5c73`.

After you have created your account and accquired your key you need to create the Virtual Schema. To do so copy the contents of [openweather-virtual-schema.sql](https://github.com/exasol/openweather-virtual-schema/blob/master/openweather-virtual-schema.sql) into your SQL editor and run the first two `CREATE OR REPLACE` statements

After the scripts are created you need to fill in the placeholders for the Virtual Schema creation.

### DISCLAIMER
In a production environment you should never put your `API_KEY` in plain text here. Instead create a [named connection](https://docs.exasol.com/7.0/sql/create_connection.htm). Otherwise the `API_KEY` will show up in your database logging.

```sql
CREATE VIRTUAL SCHEMA <NAME>
USING openweather_vs_scripts.openweather_adapter
WITH API_KEY = 'your key'
     LOG_LISTENER = 'your log listener IP'
     LOG_LISTENER_PORT = 'your log listener port'
     LOG_LEVEL = 'INFO or WARNING'
/
``` 

After the Virtual Schema is creates succesfully you can run SQL queries from your database against the API. Please refer to the example SQL statements at the bottom of [openweather-virtual-schema.sql](https://github.com/exasol/openweather-virtual-schema/blob/master/openweather-virtual-schema.sql).

## Behind the scenes

A more detailed explanation on how this Virtual Schema works can be found in the Exasol Community. Have a look [here](https://community.exasol.com/t5/database-features/using-virtual-schema-on-a-rest-api/ta-p/2298)

## Suported Features
All expressions work in both directions:
`[city_name = 'Stuttgart'] == ['Stuttgart' = city_name]`

### Filter by city
```sql
SELECT * FROM OPENWEATHER.CURRENT_WEATHER
WHERE  city_name = 'Stuttgart';
```

### Filter by city-list
```sql
SELECT * FROM OPENWEATHER.CURRENT_WEATHER
WHERE  city_name IN ('Stuttgart', 'New York', 'Memphis');
```

### Filter by latitude and longitude
```sql
SELECT * FROM OPENWEATHER.CURRENT_WEATHER
WHERE  latitude = 41.89 AND longitude = 12.48;
```

### Filter by cityID
```sql
SELECT * FROM OPENWEATHER.CURRENT_WEATHER
WHERE city_id = 3060972
```


### Filter by cityID-list
```sql
SELECT * FROM OPENWEATHER.CURRENT_WEATHER
WHERE  city_name IN (2759794, 3247449, 2957773);
```

### Filter by zip code
```sql
SELECT * FROM OPENWEATHER.CURRENT_WEATHER
WHERE zip = 96050 AND country_code = 'DE'
```

## Deleting the schema

In order to delete the Virtual Schema and it's schema  run:

```sql
DROP FORCE VIRTUAL SCHEMA openweather CASCADE;
DROP SCHEMA openweather_vs_scripts CASCADE;
```
