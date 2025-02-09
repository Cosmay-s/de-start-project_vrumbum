DROP SCHEMA IF EXISTS raw_data cascade;

CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.sales (
	id smallint PRIMARY KEY,
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
FROM 'C:\Temp\cars.csv' WITH CSV HEADER;

DROP SCHEMA IF EXISTS car_shop CASCADE;
CREATE SCHEMA car_shop;

CREATE TABLE IF NOT EXISTS car_shop.cars (
    car_id SERIAL PRIMARY KEY,  		                        /* автоинкрементируемый идентификатор */
    brand VARCHAR NOT NULL,                                     /* название бренда — VARCHAR, так как поле короткое и содержит буквы */
    model VARCHAR NOT NULL,                                     /* название модели — VARCHAR, так как поле короткое и содержит буквы буквы, цифры и пробелы */
    brand_origin VARCHAR,                                       /* страна происхождения бренда — VARCHAR, так как название страны может содержать буквы и пробелы */
    gasoline_consumption NUMERIC(3, 1)							/* расход топлива — NUMERIC(3, 1), так как он хранит число с точностью до одной десятичной цифры (например, 6.5) */
);

CREATE TABLE IF NOT EXISTS car_shop.colors (
    color_id SERIAL PRIMARY KEY,  		                        /* автоинкрементируемый идентификатор */
    color_name VARCHAR NOT NULL                                 /* название цвета — VARCHAR подходит, так как поле короткое и содержит буквы */
);

CREATE TABLE IF NOT EXISTS car_shop.car_colors (
    car_id INT REFERENCES car_shop.cars(car_id)                 /* внешний ключ на таблицу cars */
    	ON DELETE CASCADE,                                      /* при удалении машины из таблицы cars, соответствующие записи из car_colors также будут удалены */
    color_id INT REFERENCES car_shop.colors(color_id)           /* внешний ключ на таблицу colors */
    	ON DELETE CASCADE,                                      /* при удалении машины из таблицы colors, соответствующие записи из car_colors также будут удалены */
    PRIMARY KEY (car_id, color_id)                              /* составной первичный ключ, который гарантирует, что каждая пара car_id и color_id уникальна */
);

CREATE TABLE IF NOT EXISTS car_shop.customers (
    customer_id SERIAL PRIMARY KEY,  		                    /* автоинкрементируемый идентификатор */
    full_name VARCHAR NOT NULL,                                 /* имя и фамилия клиента — VARCHAR, так как поле короткое и содержит буквы, знаки и пробелы */
    phone VARCHAR NOT NULL                                      /* номер телефона клиента — VARCHAR, так как могут быть дефисы, скобки и другие символы */
);

CREATE TABLE IF NOT EXISTS car_shop.sales (
    sale_id SERIAL PRIMARY KEY,  		                        /* автоинкрементируемый идентификатор */
    car_id INT REFERENCES car_shop.cars(car_id),                /* внешний ключ на таблицу автомобилей */
    customer_id INT REFERENCES car_shop.customers(customer_id), /* внешний ключ на таблицу клиентов */
    sale_date DATE,                                             /* дата покупки — тип DATE подходит, так как хранит только год, месяц и день */
    price NUMERIC(9, 2),                                        /* цена покупки — NUMERIC(9, 2), так как цена может содержать только сотые, и она не будет превышать 7 знаков до запятой */
    discount SMALLINT DEFAULT 0                                 /* скидка — SMALLINT, так как скидка хранится как целое число и не превышает диапазона SMALLINT */
    constraint positive_discount check (discount >= 0),         /* скидка не может быть отрицательной */
    constraint positive_price check (price > 0)                 /*цена не может быть отрицательной */
);

INSERT INTO car_shop.cars (brand, model, brand_origin, gasoline_consumption)
SELECT 
	split_part(auto,' ', 1) AS brand,
    substr(split_part(auto,',', 1), strpos(auto, ' ')) AS model,
    brand_origin AS brand_origin,
    CASE 
		WHEN gasoline_consumption = 'null' THEN NULL
	ELSE gasoline_consumption::NUMERIC(3,1)
	END
FROM raw_data.sales;

INSERT INTO car_shop.colors (color_name)
SELECT DISTINCT 
	trim(split_part(auto, ',', -1)) AS color_name
FROM raw_data.sales;

INSERT INTO car_shop.car_colors (car_id, color_id)
SELECT
    c.car_id,
    col.color_id
FROM raw_data.sales s
LEFT JOIN car_shop.cars c ON c.brand = split_part(s.auto,' ', 1)
					AND c.model = substr(split_part(s.auto,',', 1), strpos(s.auto, ' '))
LEFT JOIN car_shop.colors col ON col.color_name = trim(split_part(s.auto, ',', -1))
	ON CONFLICT (car_id, color_id) DO NOTHING;

INSERT INTO car_shop.customers(full_name, phone)
SELECT DISTINCT 
	person_name AS customer_name,
	phone AS customer_phone
FROM raw_data.sales;

INSERT INTO car_shop.sales (car_id, customer_id, sale_date, price, discount)
SELECT DISTINCT 
    c.car_id,
    cu.customer_id,
    date::DATE AS sale_date,
    price::NUMERIC(9, 2),
    discount::SMALLINT 
FROM raw_data.sales s
LEFT JOIN car_shop.cars c ON c.brand = split_part(s.auto,' ', 1)
                     AND c.model = substr(split_part(s.auto,',', 1), strpos(s.auto, ' '))
LEFT JOIN car_shop.customers cu ON cu.full_name = s.person_name
                                AND cu.phone = s.phone;

-- Задание 1
SELECT 
    ((1 - (
        COUNT(gasoline_consumption)::NUMERIC / COUNT(*)
    )) * 100) AS nulls_percentage_gasoline_consumption
FROM 
    car_shop.cars;

-- Задание 2
SELECT 
    c.brand AS brand_name,
    EXTRACT(YEAR FROM s.sale_date) AS year,
    ROUND(AVG(s.price), 2) AS price_avg
FROM car_shop.sales s
LEFT JOIN car_shop.cars c ON s.car_id = c.car_id
GROUP BY c.brand, year
ORDER BY c.brand, year;

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
    cu.full_name AS person,
    STRING_AGG(DISTINCT CONCAT(c.brand, c.model), ', ') AS cars
FROM car_shop.sales s
LEFT JOIN car_shop.cars c ON s.car_id = c.car_id
LEFT JOIN car_shop.customers cu ON s.customer_id = cu.customer_id
GROUP BY cu.full_name
ORDER BY cu.full_name;

-- Задание 5
SELECT 
    brand_origin, 
    MAX((s.price / (1 - s.discount::NUMERIC / 100))::NUMERIC(9,2)) AS price_max,
    MIN((s.price / (1 - s.discount::NUMERIC / 100))::NUMERIC(9,2)) AS price_min
FROM car_shop.sales s
LEFT JOIN car_shop.cars c ON s.car_id = c.car_id
GROUP BY brand_origin;

-- Задание 6
SELECT 
    COUNT(*) AS persons_from_usa_count
FROM car_shop.customers
WHERE phone LIKE '+1%';
