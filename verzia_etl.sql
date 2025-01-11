CREATE DATABASE IF NOT EXISTS CHINOOK_TAPIR;
USE DATABASE CHINOOK_TAPIR;

CREATE OR REPLACE STAGE TAPER_STAGE FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

create or replace TABLE stage_customer (
    customerid NUMBER(38,0),
    firstname VARCHAR(40),
    lastname VARCHAR(20),
    company VARCHAR(80),
    address VARCHAR(70),
    city VARCHAR(40),
    state VARCHAR(40),
    country VARCHAR(40),
    postalcode VARCHAR(10),
    phone VARCHAR(24),
    fax VARCHAR(24),
    email VARCHAR(60),
    supportrepid NUMBER(38,0)
);

COPY INTO stage_customer
FROM @TAPER_STAGE/customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


create or replace TABLE stage_artist (
    artistid NUMBER(38,0),
    name VARCHAR(120)
);

COPY INTO stage_artist
FROM @TAPER_STAGE/artist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


create or replace TABLE stage_album (
    albumid NUMBER(38,0),
    title VARCHAR(160),
    artist NUMBER(38,0)
);

COPY INTO stage_album
FROM @TAPER_STAGE/album.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


create or replace TABLE stage_genre (
    genreid NUMBER(38,0),
    name VARCHAR(120)
);

COPY INTO stage_genre
FROM @TAPER_STAGE/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


create or replace TABLE stage_employee (
    employeeid NUMBER(38,0),
    lastname VARCHAR(20),
    firstname VARCHAR(20),
    title VARCHAR(30),
    reportsto NUMBER(38,0),
    birthdate DATE,
    hiredate DATE,
    address VARCHAR(70),
    city VARCHAR(40),
    state VARCHAR(40),
    country VARCHAR(40),
    postalcode VARCHAR(10),
    phone VARCHAR(24),
    fax VARCHAR(24),
    email VARCHAR(60)
);

COPY INTO stage_employee
FROM @TAPER_STAGE/employee.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


create or replace TABLE stage_mediatype (
    mediatypeid NUMBER(38,0),
    name VARCHAR(120)
);

COPY INTO stage_mediatype
FROM @TAPER_STAGE/mediatype.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


create or replace TABLE stage_track (
    trackid NUMBER(38,0),
    name VARCHAR(200),
    albumid NUMBER(38,0),
    mediatypeid NUMBER(38,0),
    genreid NUMBER(38,0),
    composer VARCHAR(220),
    milliseconds NUMBER(38,0),
    bytes NUMBER(38,0),
    unitprice NUMBER(10,2)
);

COPY INTO stage_track
FROM @TAPER_STAGE/track.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


create or replace TABLE stage_invoice (
    invoiceid NUMBER(38,0),
    customerid NUMBER(38,0),
    invoicedate TIMESTAMP,
    billingaddress VARCHAR(70),
    billingcity VARCHAR(40),
    billingstate VARCHAR(40),
    billingcountry VARCHAR(40),
    billingpostalcode VARCHAR(10),
    total NUMBER(10,2)
);


COPY INTO stage_invoice
FROM @TAPER_STAGE/invoice.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


create or replace TABLE stage_invoiceline (
    invoicelineid NUMBER(38,0),
    invoiceid NUMBER(38,0),
    trackid NUMBER(38,0),
    unitprice NUMBER(10,2),
    quantity NUMBER(38,0)
);

COPY INTO stage_invoiceline
FROM @TAPER_STAGE/invoiceline.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


create or replace TABLE stage_playlist (
    playlist_id NUMBER(38,0),
    name VARCHAR(120)
);

COPY INTO stage_playlist
FROM @TAPER_STAGE/playlist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


create or replace TABLE stage_playlisttrack (
    playlist_id NUMBER(38,0),
    track_id NUMBER(38,0)
);

COPY INTO stage_playlisttrack
FROM @TAPER_STAGE/playlisttrack.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


SELECT * FROM stage_customer;
SELECT * FROM stage_album;
SELECT * FROM stage_artist;
SELECT * FROM stage_genre;
SELECT * FROM stage_employee;
SELECT * FROM stage_mediatype;
SELECT * FROM stage_track;
SELECT * FROM stage_invoice;
SELECT * FROM stage_invoiceline;
SELECT * FROM stage_playlist; 
SELECT * FROM stage_playlisttrack;


CREATE OR REPLACE TABLE dim_address AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY postalcode) AS addressid,
    city,
    state,
    country,
    postalcode AS postal_code
FROM stage_customer
WHERE postalcode IS NOT NULL;

select * from dim_address;

CREATE OR REPLACE TABLE dim_customer AS
SELECT DISTINCT
    customerid,
    firstname,
    lastname,
    company,
    null as gender 
FROM stage_customer;

select * from dim_customer;


CREATE OR REPLACE TABLE dim_employee AS
SELECT DISTINCT
    employeeid,
    firstname,
    lastname,
    birthdate as age,
    null as gender 
FROM stage_employee;

select * from dim_employee;


CREATE OR REPLACE TABLE dim_track AS
SELECT DISTINCT
    trackid,
    name,
    composer,
    albumid,
    genreid,
    bytes
FROM stage_track;

select * from dim_track;



CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY invoicedate) AS dateid,
    EXTRACT(YEAR FROM invoicedate) AS year,
    EXTRACT(MONTH FROM invoicedate) AS month,
    EXTRACT(DAY FROM invoicedate) AS day
FROM stage_invoice
WHERE invoicedate IS NOT NULL;

CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY invoicedate) AS timeid,
    EXTRACT(HOUR FROM invoicedate) AS hour,
    EXTRACT(MINUTE FROM invoicedate) AS minute,
    EXTRACT(SECOND FROM invoicedate) AS second
FROM stage_invoice
WHERE invoicedate IS NOT NULL;

select * from dim_date;

CREATE OR REPLACE TABLE fact_sales AS
SELECT 
    il.invoicelineid AS fact_salesid,
    i.customerid AS dim_customer_customerid,
    e.employeeid AS dim_employee_employeeid, 
    d.dateid AS dim_date_dateid,           
    t.timeid AS dim_time_timeid,            
    il.trackid AS dim_track_trackid,
    il.unitprice,
    il.quantity,
    il.unitprice * il.quantity AS total,
    a.addressid AS dim_address_addressid   
FROM stage_invoiceline il
JOIN stage_invoice i ON il.invoiceid = i.invoiceid
LEFT JOIN dim_customer c ON i.customerid = c.customerid
LEFT JOIN dim_employee e ON e.employeeid = il.invoicelineid
LEFT JOIN dim_date d ON DATE(i.invoicedate) = DATE(d.year || '-' || d.month || '-' || d.day)
LEFT JOIN dim_time t ON EXTRACT(HOUR FROM i.invoicedate) = t.hour 
                     AND EXTRACT(MINUTE FROM i.invoicedate) = t.minute
LEFT JOIN dim_address a ON i.billingpostalcode = a.postal_code;

select * from fact_sales;



DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS album;
DROP TABLE IF EXISTS artist;
DROP TABLE IF EXISTS genre;
DROP TABLE IF EXISTS employee;
DROP TABLE IF EXISTS mediatype;
DROP TABLE IF EXISTS track;
DROP TABLE IF EXISTS invoiceline;
DROP TABLE IF EXISTS invoice;
DROP TABLE IF EXISTS playlisttrack;

-----------------------------------------------------------------------

CREATE OR REPLACE VIEW dayly_sales_analysis AS
SELECT
    dd.day AS day,
    SUM(fs.total) AS total_sales
FROM 
    fact_sales fs
JOIN 
    dim_date dd ON fs.dim_date_dateid = dd.dateid
GROUP BY 
    dd.day
ORDER BY 
    dd.day;

-----------------------------------------------------------------------

CREATE OR REPLACE VIEW top_10_country_sales AS
SELECT 
    da.country,
    SUM(fs.quantity) AS total_orders
FROM fact_sales fs
JOIN dim_address da ON fs.dim_address_addressid = da.addressid
JOIN stage_invoice si ON fs.dim_customer_customerid = si.customerid
WHERE EXTRACT(YEAR FROM si.invoicedate) = 2021  
GROUP BY da.country
ORDER BY total_orders DESC
LIMIT 10;

-----------------------------------------------------------------------

CREATE OR REPLACE VIEW top_10_albums_orders_and_sales AS
SELECT 
    sa.title AS album_name,
    COUNT(fs.fact_salesid) AS total_orders,
    SUM(fs.unitprice * fs.quantity) AS total_album_sales
FROM fact_sales fs
JOIN dim_track dt ON fs.dim_track_trackid = dt.trackid
JOIN stage_album sa ON dt.albumid = sa.albumid
GROUP BY sa.title
ORDER BY total_orders DESC, total_album_sales DESC
LIMIT 10;

-----------------------------------------------------------------------

CREATE OR REPLACE VIEW monthly_revenue_2022 AS
SELECT 
    d.year, 
    d.month, 
    SUM(fs.total) AS total_revenue
FROM 
    fact_sales fs
JOIN 
    dim_date d ON fs.dim_date_dateid = d.dateid
WHERE 
    d.year = 2022
GROUP BY 
    d.year, 
    d.month
ORDER BY 
    d.year, 
    d.month;

----------------------------------------------------------------------

CREATE OR REPLACE VIEW popular_genres AS
SELECT 
    g.name AS genre_name,
    COUNT(fs.fact_salesid) AS total_sales_count
FROM 
    fact_sales fs
JOIN 
    dim_track t ON fs.dim_track_trackid = t.trackid
JOIN 
    stage_genre g ON t.genreid = g.genreid
GROUP BY 
    g.name
ORDER BY 
    total_sales_count DESC
LIMIT 10;

----------------------------------------------------------------------

CREATE OR REPLACE VIEW top_10_albums_by_listens AS
SELECT 
    sa.title AS album_name,
    COUNT(fs.fact_salesid) AS total_listens
FROM fact_sales fs
JOIN dim_track dt ON fs.dim_track_trackid = dt.trackid
JOIN stage_album sa ON dt.albumid = sa.albumid
GROUP BY sa.title
ORDER BY total_listens DESC
LIMIT 10;

----------------------------------------------------------------------

CREATE OR REPLACE VIEW loyal_customer AS
SELECT 
    c.customerid,
    CONCAT(c.firstname, ' ', c.lastname) AS name,  
    COUNT(fs.fact_salesid) AS total_sales
FROM 
    fact_sales fs
JOIN 
    dim_customer c ON fs.dim_customer_customerid = c.customerid
GROUP BY 
    c.customerid, c.firstname, c.lastname
ORDER BY 
    total_sales DESC
LIMIT 10;

-----------------------------------------------------------------------

select * from dayly_sales_analysis;
select * from top_10_country_sales;
select * from top_10_albums_orders_and_sales;
select * from monthly_revenue_2022;
select * from popular_genres;
select * from top_10_albums_by_listens;
select * from loyal_customer;


