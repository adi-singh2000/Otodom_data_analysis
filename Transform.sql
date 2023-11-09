-- We have a column address in table which have LONGITUTE & LATITUDE value, to get proper location from that value we have to use python, but we already got the data so we will add it in our new table.
create or replace table flatten_otodom_address
(
    rn int,
    location text,
    address text
);

-- This website is from poland, because of that some column are in Polish language. So we will use python to Translate Polish Language to English. But we already got the data so we will add it in our new table.
create or replace table flatten_otodom_translate
(
    rn int,
    title text,
    title_eng text
);

-- Now we got 3 tables
select * from flatten_otodom_translate; -- 62802

select * from flatten_otodom_address; -- 62802

select * from flatten_otodom; -- 62816

-- Now we join our tables to get one table and we also do some changes in our data like removing "PLN" from Price column.

CREATE OR REPLACE TABLE OTODOM_DATA_TRANSFORMED
as
with cte as 
    (select ot.*
    , case when price like 'PLN%' then try_to_number(replace(price,'PLN ',''),'999,999,999.99')
           when price like '€%' then try_to_number(replace(price,'€',''),'999,999,999.99') * 4.43
      end as price_new
    , try_to_double(replace(replace(replace(replace(surface,'m²',''),'м²',''),' ',''),',','.'),'9999.99') as surface_new
    , replace(parse_json(addr.address):suburb,'"', '') as suburb
    , replace(parse_json(addr.address):city,'"', '') as city
    , replace(parse_json(addr.address):country,'"', '') as country
    , trans.title_eng as title_eng
    from flatten_otodom ot 
    left join flatten_otodom_address addr on ot.rn=addr.rn 
    left join flatten_otodom_translate trans on ot.rn=trans.rn)
select *,
  case when lower(title_eng) like '%commercial%' or lower(title_eng) like '%office%' or lower(title_eng) like '%shop%' then 'non apartment'
       when is_for_sale = 'false' and surface_new <=330 and price_new <=55000 then 'apartment'
       when is_for_sale = 'false' then 'non apartment'
       when is_for_sale = 'true'  and surface_new <=600 and price_new <=20000000 then 'apartment'
       when is_for_sale = 'true'  then 'non apartment'
  end as apartment_flag
from cte;
 

select * from OTODOM_DATA_TRANSFORMED; -- 62816
