--create a seperate database to store hotel data
create database hotel_db;

--go to the created db
use database hotel_db;

--create a schema for raw data load
create schema bronze;

--go to the created bronze schema
use schema bronze;

--create a fileformat to use your csv table as source
create or replace file format ff_csv
type = 'CSV'
field_optionally_enclosed_by = '"'
field_delimiter = ','
skip_header = 1
null_if = ('NULL','null','')

--create stage as this is necessary to read data from a file
create or replace stage stg_hotel_bookings
file_format = ff_csv

--create bronze layer table
create or replace table bronze_hotel_booking
cluster by (check_in_date) (
booking_id string,
hotel_id   string,
hotel_city string,
customer_id   string,
customer_name string,
customer_email string,
check_in_date  string,
check_out_date string,
room_type      string,
num_guests     string,
total_amount   string,
currency       string,
booking_status string
);

--copying the file data into bronze table
copy into bronze_hotel_booking
from @stg_hotel_bookings
file_format = (format_name = ff_csv)
on_error = 'continue';

--view data in the bronze table
select * from bronze.bronze_hotel_booking;

--you can see data in the bronze table which means the load is successful and our bronze layer data movement is finished.
''' now our next load is to clean the data, and load the cleaned/usable data into silver layer. '''


--for silver works, lets maintain seperate schema altogether
create schema silver;

--now create the silver table
create or replace table silver_hotel_booking
cluster by (check_in_date) (
booking_id varchar2(100),
hotel_id   varchar2(100),
hotel_city varchar2(50),
customer_id   varchar2(100),
customer_name varchar2(80),
customer_email varchar2(100),
check_in_date  DATE,
check_out_date DATE,
room_type      varchar2(50),
num_guests     INTEGER,
total_amount   float4,
currency       varchar2(10),
booking_status varchar2(20)
);

--ANALYSIS OF BRONZE TABLE
SELECT * FROM BRONZE.BRONZE_HOTEL_BOOKING
WHERE CUSTOMER_EMAIL NOT LIKE '%@%'; --THERE ARE 400 invalid email values

SELECT distinct customer_email FROM BRONZE.BRONZE_HOTEL_BOOKING
WHERE CUSTOMER_EMAIL NOT LIKE '%@%';

SELECT * FROM BRONZE.BRONZE_HOTEL_BOOKING
WHERE CUSTOMER_EMAIL is null;

--now lets check total amount. amount cannot be less than 0
select total_amount from bronze.bronze_hotel_booking where try_to_number(total_amount) <0 --176 invalid data

--check if checkin date is after checkout date
select check_in_date, check_out_date from bronze.bronze_hotel_booking where 
try_to_date(check_in_date)>=try_to_date(check_out_date) --50 rows

--lets investigate booking status column
select booking_status, count(*) as total_count from bronze.bronze_hotel_booking
group by booking_status;--Confirmeeed	227 in appropiate, need to spell right


'''now we are done with analysis. lets implement the query to data processing into silver layer.
list of items taken care of:
1) Data type correction
2) invalid values in the columns, can rectify
3)invalid values in the columns, need to drop
4) space removal'''


--data processing query into silver layer
insert into silver.silver_hotel_booking 
select booking_id,
       hotel_id,
       initcap(trim(hotel_city)) as hotel_city,
       customer_id,
       initcap(trim(customer_name)) as customer_name,
       case when customer_email like '%@%.%' then lower(trim(customer_email))
       else null end as customer_email,
       try_to_date(nullif(check_in_date,'')) as check_in_date,
       try_to_date(nullif(check_out_date,'')) as check_out_date,
       room_type,
       try_to_number(num_guests) as num_guests,
       abs(try_to_number(total_amount)) as total_amount,
       currency,
       case when lower(booking_status)  in ('confirmeeed','confirmd','confirmeed') then 'Confirmed'
       else initcap(booking_status) end as booking_status
       from bronze.bronze_hotel_booking
       where check_in_date is not null and check_out_date is not null and try_to_date(check_in_date) <= try_to_date(check_out_date)


--data processing to silver layer is successful with 1729 records
select * from silver.silver_hotel_booking;


''' now we have clean and useful data. Now lets prepare the gold layer and extract all the useful KPI, and 
analytics data in gold area'''

---create a new schema for gold
create schema gold;

use schema gold;

-- Create Gold Tables
CREATE TABLE gold.GOLD_AGG_DAILY_BOOKING AS
SELECT
    check_in_date AS date,
    COUNT(*) AS total_booking,
    SUM(total_amount) AS total_revenue
FROM silver.silver_hotel_booking
GROUP BY check_in_date
ORDER BY check_in_date;

CREATE TABLE gold.GOLD_AGG_HOTEL_CITY_SALES AS
SELECT
    hotel_city,
    SUM(total_amount) AS total_revenue
FROM silver.silver_hotel_booking
GROUP BY hotel_city
ORDER BY total_revenue DESC;

CREATE TABLE gold.GOLD_BOOKING_CLEAN AS
SELECT
    booking_id,
    hotel_id,
    hotel_city,
    customer_id,
    customer_name,
    customer_email,
    check_in_date,
    check_out_date,
    room_type,
    num_guests,
    total_amount,
    currency,
    booking_status
FROM silver.silver_hotel_booking;


--lets view the tables
SELECT * FROM GOLD_AGG_DAILY_BOOKING LIMIT 30;

SELECT * FROM GOLD_AGG_HOTEL_CITY_SALES LIMIT 30;