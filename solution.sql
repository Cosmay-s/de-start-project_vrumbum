DROP SCHEMA IF EXISTS raw_data cascade;
CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.sales (
	id SMALLINT PRIMARY KEY,
	auto VARCHAR,
	gasoline_consumption VARCHAR,
	price NUMERIC(9, 2),
	date date,
	person_name VARCHAR,
	phone VARCHAR,
	discount SMALLINT,
	brand_origin VARCHAR
);

COPY raw_data.sales(id,auto,gasoline_consumption,price,date,person_name,phone,discount,brand_origin)
FROM 'C:\Temp\cars.csv' WITH CSV HEADER NULL 'null';

DROP SCHEMA IF EXISTS car_shop CASCADE;

CREATE SCHEMA car_shop;

CREATE TABLE IF NOT EXISTS car_shop.country (
    country_id SERIAL PRIMARY KEY,                                /* автоинкрементируемый идентификатор */
    country_name VARCHAR NOT NULL                                 /* страна происхождения бренда — VARCHAR, так как название страны может содержать буквы и пробелы */
);

CREATE TABLE IF NOT EXISTS car_shop.brand (
    brand_id SERIAL PRIMARY KEY,                                  /* автоинкрементируемый идентификатор */
    brand_name VARCHAR NOT NULL,                                  /* название бренда — VARCHAR, так как поле короткое и содержит буквы */
    country_id INTEGER REFERENCES car_shop.country(country_id)    /* внешний ключ на таблицу country */
);

CREATE TABLE IF NOT EXISTS car_shop.model (
    model_id SERIAL PRIMARY KEY,                                  /* автоинкрементируемый идентификатор */
    model_name VARCHAR NOT NULL,                                  /* название модели — VARCHAR, так как поле короткое и содержит буквы буквы, цифры и пробелы */
    brand_id INTEGER REFERENCES car_shop.brand(brand_id),         /* внешний ключ на таблицу brand */
    gasoline_consumption NUMERIC(3, 1)                            /* расход топлива — NUMERIC(3, 1), так как он хранит число с точностью до одной десятичной цифры */
);

CREATE TABLE IF NOT EXISTS car_shop.color (
    color_id SERIAL PRIMARY KEY,                                   /* автоинкрементируемый идентификатор */
    color_name VARCHAR NOT NULL,                                   /* название цвета — VARCHAR подходит, так как поле короткое и содержит буквы */
    CONSTRAINT color_unique_name UNIQUE (color_name)               /* ограничение уникальности */
);

CREATE TABLE IF NOT EXISTS car_shop.customer (
    customer_id SERIAL PRIMARY KEY,  		                       /* автоинкрементируемый идентификатор */
    customer_name VARCHAR NOT NULL,                                /* имя и фамилия клиента — VARCHAR, так как поле короткое и содержит буквы, знаки и пробелы */
    customer_phone VARCHAR NOT NULL                                /* номер телефона клиента — VARCHAR, так как могут быть дефисы, скобки и другие символы */
);

CREATE TABLE IF NOT EXISTS car_shop.sales (
    sale_id SERIAL PRIMARY KEY,                                    /* автоинкрементируемый идентификатор */
    model_id INTEGER REFERENCES car_shop.model(model_id),          /* внешний ключ на таблицу model */
    customer_id INTEGER REFERENCES car_shop.customer(customer_id), /* внешний ключ на таблицу customer */
    color_id INTEGER REFERENCES car_shop.color(color_id),          /* внешний ключ на таблицу color */
    sale_date DATE,                                                /* дата покупки — тип DATE подходит, так как хранит только год, месяц и день */
    price NUMERIC(9, 2),                                           /* цена покупки — NUMERIC(9, 2), так как цена может содержать только сотые, и она не будет превышать 7 знаков до запятой */
    discount SMALLINT DEFAULT 0,                                   /* скидка — SMALLINT, так как скидка хранится как целое число и не превышает диапазона SMALLINT */
    CONSTRAINT positive_discount CHECK (discount >= 0),            /* скидка не может быть отрицательной */
    CONSTRAINT positive_price CHECK (price > 0)                    /*цена не может быть отрицательной */
);

INSERT INTO car_shop.country (country_name)
SELECT DISTINCT 
    COALESCE(brand_origin, 'Unknown') AS country_name
FROM raw_data.sales;

INSERT INTO car_shop.brand (brand_name, country_id)
SELECT DISTINCT
    split_part(s.auto, ' ', 1) AS brand_name,
    c.country_id
FROM raw_data.sales s
LEFT JOIN car_shop.country c 
    ON COALESCE(s.brand_origin, 'Unknown') = c.country_name;

INSERT INTO car_shop.model (model_name, brand_id, gasoline_consumption)
SELECT DISTINCT
    substr(s.auto, strpos(s.auto, ' '), strpos(s.auto, ',') - strpos(s.auto, ' ')) AS model_name,
    b.brand_id,
    CASE 
		WHEN gasoline_consumption = 'null' THEN NULL
	ELSE gasoline_consumption::NUMERIC(3,1)
	END
FROM raw_data.sales s
LEFT JOIN car_shop.brand b 
    ON split_part(s.auto, ' ', 1) = b.brand_name;

INSERT INTO car_shop.color (color_name)
SELECT DISTINCT
    split_part(s.auto, ',', -1) AS color_name
FROM raw_data.sales s;

INSERT INTO car_shop.customer (customer_name, customer_phone)
SELECT DISTINCT 
    person_name AS customer_name, 
    phone AS customer_phone
FROM raw_data.sales;

INSERT INTO car_shop.sales (model_id, customer_id, color_id, sale_date, price, discount)
SELECT DISTINCT
    m.model_id,
    cu.customer_id,
    co.color_id,
    s.date AS sale_date,
    s.price,
    s.discount
FROM raw_data.sales s
LEFT JOIN car_shop.model m
    ON substr(s.auto, strpos(s.auto, ' '), strpos(s.auto, ',') - strpos(s.auto, ' ')) = m.model_name
LEFT JOIN car_shop.customer cu
	ON s.person_name = cu.customer_name
LEFT JOIN car_shop.color co 
	ON split_part(s.auto, ',', -1) = co.color_name;

-- Задание 1
SELECT 
    ((1 - (
        COUNT(gasoline_consumption)::NUMERIC / COUNT(*)
    )) * 100) AS nulls_percentage_gasoline_consumption
FROM 
    car_shop.model;

-- Задание 2
SELECT 
    b.brand_name AS brand_name,
    EXTRACT(YEAR FROM s.sale_date) AS year,
    ROUND(AVG(s.price), 2) AS price_avg
FROM car_shop.sales s
LEFT JOIN car_shop.model m ON s.model_id = m.model_id
LEFT JOIN car_shop.brand b on b.brand_id = m.brand_id
GROUP BY brand_name, year
ORDER BY brand_name, year;

-- Задание 3
SELECT 
    EXTRACT(MONTH FROM s.sale_date) AS month,
    2022 AS year,
    ROUND(AVG(s.price), 2) AS price_avg
FROM car_shop.sales s
WHERE EXTRACT(YEAR FROM s.sale_date) = 2022
GROUP BY MONTH
ORDER BY month;

-- Задание 4
SELECT
    cu.customer_name AS person,
    STRING_AGG(DISTINCT CONCAT(b.brand_name, m.model_name), ', ') AS cars
FROM car_shop.sales s
LEFT JOIN car_shop.customer cu ON s.customer_id = cu.customer_id
LEFT JOIN car_shop.model m ON m.model_id = s.model_id
LEFT JOIN car_shop.brand b ON b.brand_id = m.brand_id
GROUP BY person
ORDER BY person;

-- Задание 5
SELECT 
    c.country_name AS brand_origin, 
    MAX((s.price / (1 - s.discount::NUMERIC / 100))::NUMERIC(9,2)) AS price_max,
    MIN((s.price / (1 - s.discount::NUMERIC / 100))::NUMERIC(9,2)) AS price_min
FROM car_shop.sales s
LEFT JOIN car_shop.model m ON s.model_id = m.model_id
LEFT JOIN car_shop.brand b ON b.brand_id = m.brand_id
LEFT JOIN car_shop.country c ON c.country_id = b.country_id
GROUP BY brand_origin;

-- Задание 6
SELECT 
    COUNT(*) AS persons_from_usa_count
FROM car_shop.customer
WHERE customer_phone LIKE '+1%';
