# Openweather Virtual Schema

## Overview
Exasol Virtual Schemas allow you to acces arbitrary data sources from inside your Exasol database. 
This functionality is mainly used to integrate relational database management systems but is not limited to it.
This project shows that even REST APIs can be connected to Exasol using Virtual Schema.

## Getting Started
In order to use the virtual schema you need an account with [Openweather](https://openweathermap.org/). A free account works for this example. With an account you will get an API key which you need to access the API. The key looks like this: `d5ea350b1a22f5ba4e4b8a8570bd5c73`.

After you have created your account and accquired your key you need to create the Virtual Schema. To do so copy the contents of [openweather-virtual-schema.sql](https://github.com/exasol/openweather-virtual-schema/blob/develop/openweather-virtual-schema.sql) into your SQL editor and run the first two `CREATE OR REPLACE` statements

After the scripts are created you need to fill in the placeholders for the Virtual Schema creation.

```sql
CREATE VIRTUAL SCHEMA <NAME>
USING openweather_vs_scripts.openweather_adapter
WITH API_KEY = 'your key'
     LOG_LISTENER = 'your log listener IP'
     LOG_LISTENER_PORT = 'your log listener port'
     LOG_LEVEL = 'INFO or WARNING'
/
``` 

After the Virtual Schema is creates succesfully you can run SQL queries from your database against the API. Please refer to the example SQL statements at the bottom of [openweather-virtual-schema.sql](https://github.com/exasol/openweather-virtual-schema/blob/develop/openweather-virtual-schema.sql).

## Behind the scenes

A more detailed explanation on how this Virtual Schema works can be found in the Exasol Community. Have a look [here](https://community.exasol.com/t5/tkb/articleeditorpage/tkb-id/tkb/message-uid/2298)

## Deleting the schema

In order to delete the Virtual Schema and it's schema  run:

´´´sql
DROP FORCE VIRTUAL SCHEMA openweather CASCADE;
DROP SCHEMA openweather_vs_scripts CASCADE;
´´´
