# dt_projekt_ch

Tento repozitár obsahuje implementáciu ETL procesu, ktorý slúži na analýzu dát z databázy Chinook. Proces zahŕňa extrakciu, transformáciu a načítanie dát do Snowflake dimenzionálneho modelu. Tento model umožňuje efektívnu vizualizáciu a analýzu informácií o hudobných albumoch, skladbách, zákazníkoch a predajoch.

## **1. Úvod a popis zdrojových dát**

Cieľom semestrálneho projektu je vykonať analýzu dát v databáze Chinook, pričom sa zameriame na používateľov, ich preferencie a nákupy skladieb. Táto analýza nám umožní identifikovať trendy v záujmoch používateľov, najobľúbenejšie položky (ako skladby alebo albumy) a správanie používateľov. Dataset obsahuje nasledujúce tabuľky:

`album.csv`: Údaje o hudobných albumoch.
`artist.csv`: Informácie o interpretoch.
`customer.csv`: Dáta o zákazníkoch, vrátane kontaktných údajov.
`employee.csv`: Informácie o zamestnancoch, ktorí spravujú objednávky.
`genre.csv`: Kategórie hudby (žánre).
`invoice.csv`: Faktúry a údaje o predajoch.
`invoiceline.csv`: Detaily o položkách na jednotlivých faktúrach.
`mediatype.csv`: Rôzne typy médií, ako napríklad MP3 alebo AAC.
`playlist.csv`: Zoznamy skladieb vytvorené používateľmi.
`playlisttrack.csv`: Väzby medzi playlistami a skladbami.
`track.csv`: Údaje o skladbách, vrátane názvov, žánrov a dĺžky.

### **1.1 Dátová architektúra**

Dáta sú organizované v relačnom modeli, ktorý je vizualizovaný prostredníctvom entitno-relačného diagramu (ERD):

<p align="center">
  <img src="https://github.com/dqkme/dt_projekt_ch/blob/main/Chinook_ERD.png" alt="ERD Schema">
  <br>
  <em>Entitno-relačná schéma Chinook</em>
</p>

## **2. Dimenzionálny model**


Bol navrhnutý hviezdicový model (star schema), ktorý umožňuje efektívnu analýzu dát. Centrálny bod modelu tvorí faktová tabuľka `fact_sales`, ktorá je prepojená s týmito dimenziami:

`dim_address`: Táto tabuľka obsahuje informácie o geografických lokalitách zákazníkov.
`dim_customer`: Zahrňuje informácie o zákazníkoch, ktorí uskutočnili nákupy.
`dim_date`: Poskytuje podrobné údaje o dátumoch a časových aspektoch pre analýzu.
`dim_employee`: Obsahuje dáta o zamestnancoch, ktorí sa podieľali na realizácii transakcií.
`dim_time`: Obsahuje podrobné časové údaje, ako sú hodina, minúta a sekunda, pre presnejšiu analýzu transakcií.
`dim_track`: Obsahuje údaje o skladbách, albumoch, interpretoch a žánroch.

## **3. ETL proces v Snowflake**

ETL proces v Snowflake zahŕňal tri hlavné fázy: extrahovanie (Extract), transformácia (Transform) a načítanie (Load). Tento proces bol navrhnutý na spracovanie zdrojových dát zo staging vrstvy a ich transformáciu do dimenzionálneho modelu, ktorý je optimalizovaný pre analýzu a vizualizáciu.

### **3.1 Extract (Extrahovanie dát)**

Dáta zo zdrojových súborov vo formáte .csv boli nahrané do Snowflake do dočasného úložiska s názvom **TEMP_STAGE**. Pred samotným nahraním dát bola vytvorená a inicializovaná databáza, dátový sklad a schéma. Ďalšie kroky zahŕňali import údajov do staging tabuliek. Proces bol spustený pomocou nasledujúcich príkazov:

```sql
CREATE DATABASE IF NOT EXISTS CHINOOK_TAPIR;
USE DATABASE CHINOOK_TAPIR;
```
Kroky extrakcie dát:

Vytvorenie staging tabuliek pre všetky zdrojové údaje (napr. zamestnanci, zákazníci, faktúry, skladby, žánre, atď.).  Použitie príkazu COPY INTO na nahranie dát z .csv súborov do príslušných staging tabuliek:

```sql
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
```

Overovanie kodu:

SELECT * FROM stage_customer;

### **3.2 Transfor (Transformácia dát)**

Transformácia dát zahŕňala čistenie, obohacovanie a reorganizáciu údajov do dimenzionálnych a faktových tabuliek, ktoré podporujú efektívnu viacdimenzionálnu analýzu.

Príklad transformácie:

Dimenzia `dim_customer`: Táto dimenzia obsahuje informácie o zákazníkoch, ako sú meno, adresa, mesto a krajina, ktoré sú odvodené zo staging tabuľky `customer_stage`

```sql
CREATE OR REPLACE TABLE dim_customer AS
SELECT DISTINCT
    customerid,
    firstname,
    lastname,
    company,
    null as gender 
FROM stage_customer;

select * from dim_customer;
```

### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktových tabuliek boli staging tabuľky odstránené, aby sa uvoľnilo úložisko a optimalizovala jeho kapacita. Príklad príkazu na čistenie staging tabuliek:

```sql
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
```

Tento krok zabezpečil, že v systéme nezostali zbytočné dočasné dáta po ukončení ETL procesu.

## **4 Vizualizácia dát**

### 4.1 

Tento dotaz vytvára zobrazenie **`dayly_sales_analysis`**, ktoré vykonáva analýzu denných predajov. Sčíta celkové predaje z tabuľky **`fact_sales`** podľa dní, pričom používa tabuľku **`dim_date`** na spojenie s dátumami. Výsledky sú zoskupené podľa dní a zoradené podľa dátumu.

```sql
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
```

### 4.2

Tento dotaz vytvára zobrazenie **`top_10_country_sales`**, ktoré zobrazuje top 10 krajín podľa počtu objednávok v roku 2021. Spojuje tabuľky **`fact_sales`**, **`dim_address`** a **`stage_invoice`**, sčíta množstvo objednaných položiek (quantity) a zoskupí ich podľa krajiny. Výsledky sú zoradené podľa počtu objednávok a obmedzené na 10 krajín s najvyšším počtom objednávok.

```sql
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
```

### 4.3

Tento dotaz vytvára zobrazenie **`top_10_albums_orders_and_sales`**, ktoré zobrazuje top 10 albumov podľa počtu objednávok a celkového predaja. Spojuje tabuľky **`fact_sales`**, **`dim_track`** a **`stage_album`**, počíta počet objednávok (COUNT) a sumu predaja albumu (unitprice * quantity). Výsledky sú zoradené podľa počtu objednávok a následne podľa celkového predaja a obmedzené na 10 najlepších albumov.

```sql
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
```

### 4.4 

Tento dotaz vytvára zobrazenie **`monthly_revenue_2022`**, ktoré zobrazuje mesačné príjmy za rok 2022. Spojuje tabuľky **`fact_sales`** a **`dim_date`**, sčíta celkové príjmy (total) z predajov pre každý mesiac v roku 2022. Výsledky sú zoskupené podľa roka a mesiaca a zoradené podľa týchto atribútov.

```sql
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
```

### 4.5

Tento dotaz vytvára zobrazenie **`popular_genres`**, ktoré zobrazuje top 10 najpopulárnejších žánrov podľa počtu predajov. Spojuje tabuľky **`fact_sales`**, **`dim_track`** a **`stage_genre`**, počíta počet predajov pre každý žáner a zobrazuje ich zoradené podľa počtu predajov v zostupnom poradí. Výsledky sú obmedzené na 10 najpopulárnejších žánrov.

```sql
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
```

### 4.6 

Tento dotaz vytvára zobrazenie **`top_10_albums_by_listens`**, ktoré zobrazuje top 10 albumov podľa počtu počúvaní. Spojuje tabuľky **`fact_sales`**, **`dim_track`** a **`stage_album`**, počíta počet počúvaní (za každý predaj) pre každý album a zoradí výsledky podľa počtu počúvaní v zostupnom poradí. Výsledky sú obmedzené na 10 albumov s najvyšším počtom počúvaní.

```sql
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
```

### 4.7 

Tento dotaz vytvára zobrazenie **`loyal_customer`**, ktoré zobrazuje top 10 najvernejších zákazníkov na základe počtu predajov. Spojuje tabuľky **`fact_sales`** a **`dim_customer`**, počíta počet predajov pre každého zákazníka a zobrazuje ich zoradené podľa počtu predajov v zostupnom poradí. Výsledky obsahujú meno zákazníka (spojené z `firstname` a `lastname`) a sú obmedzené na 10 najvernejších zákazníkov.

```sql
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
```

#### Overenie

```sql
select * from dayly_sales_analysis;
select * from top_10_country_sales;
select * from top_10_albums_orders_and_sales;
select * from monthly_revenue_2022;
select * from popular_genres;
select * from top_10_albums_by_listens;
select * from loyal_customer;
```

### Vizualizacii

---
<p align="center">
  <img src="https://github.com/dqkme/dt_projekt_ch/blob/main/vizualizacia/vizualicia1.png" alt="ERD Schema">
  <br>
  <em> dayly sales analysis </em>
</p>

---
<p align="center">
  <img src="https://github.com/dqkme/dt_projekt_ch/blob/main/vizualizacia/vizualizacia2.png" alt="ERD Schema">
  <br>
  <em> top 10 country sales </em>
</p>

---
<p align="center">
  <img src="https://github.com/dqkme/dt_projekt_ch/blob/main/vizualizacia/vizualizacia3.png" alt="ERD Schema">
  <br>
  <em> top 10 albums orders and sales </em>
</p>

---
<p align="center">
  <img src="https://github.com/dqkme/dt_projekt_ch/blob/main/vizualizacia/vizualizacia4.png" alt="ERD Schema">
  <br>
  <em> monthly revenue 2022 </em>
</p>

---
<p align="center">
  <img src="https://github.com/dqkme/dt_projekt_ch/blob/main/vizualizacia/vizualizacia5.png" alt="ERD Schema">
  <br>
  <em> popular_genres </em>
</p>

---
<p align="center">
  <img src="https://github.com/dqkme/dt_projekt_ch/blob/main/vizualizacia/vizualizacia6.png" alt="ERD Schema">
  <br>
  <em> top 10 albums by listens </em>
</p>

---
<p align="center">
  <img src="https://github.com/dqkme/dt_projekt_ch/blob/main/vizualizacia/vizualizacia7.png" alt="ERD Schema">
  <br>
  <em> loyal customer </em>
</p>

---

**Akgul Belkozhayeva**
