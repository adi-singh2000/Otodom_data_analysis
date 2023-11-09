-- To Scrape data we have use "Bright data" to scrape the Otodom website.

-- To Load data, we are loading our data in Snowflake cloud enviroment. 
-- Data is in JSON format, so to load data in Snowflake we will use this code

-- Create the destination table.
CREATE or replace TABLE OTODOM
(
    json_data  text
);


-- Create the file format object
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  type = csv
  field_delimiter = ','
  field_optionally_enclosed_by='"';
  

-- Create the internal Stage Object.
CREATE OR REPLACE STAGE MY_CSV_STAGE_SHORT
  file_format=csv_format;

-- Load data from Stage to table
COPY INTO OTODOM
FROM @MY_CSV_STAGE_SHORT;


SELECT COUNT(1) FROM OTODOM; -- It gives 62816 records.
SELECT PARSE_JSON(json_data):price FROM OTODOM limit 5;


-- To FLATTEN the data 

Create or replace table flatten_otodom as 
select row_number() over(order by title) as rn,
x.*
from (
select replace(parse_json(json_data):advertiser_type,'"')::string as advertiser_type
, replace(parse_json(json_data):balcony_garden_terrace,'"')::string as balcony_garden_terrace
, regexp_replace(replace(parse_json(json_data):description,'"'), '<[^>]+>')::string as description
, replace(parse_json(json_data):heating,'"')::string as heating
, replace(parse_json(json_data):is_for_sale,'"')::string as is_for_sale
, replace(parse_json(json_data):lighting,'"')::string as lighting
, replace(parse_json(json_data):location,'"')::string as location
, replace(parse_json(json_data):price,'"')::string as price
, replace(parse_json(json_data):remote_support,'"')::string as remote_support
, replace(parse_json(json_data):rent_sale,'"')::string as rent_sale
, replace(parse_json(json_data):surface,'"')::string as surface
, replace(parse_json(json_data):timestamp,'"')::date as timestamp
, replace(parse_json(json_data):title,'"')::string as title
, replace(parse_json(json_data):url,'"')::string as url
, replace(parse_json(json_data):form_of_property,'"')::string as form_of_property
, replace(parse_json(json_data):no_of_rooms,'"')::string as no_of_rooms
, replace(parse_json(json_data):parking_space,'"')::string as parking_space
from OTODOM
) x;
