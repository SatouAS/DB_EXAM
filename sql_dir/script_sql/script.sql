drop table if exists orders, products, order_items;

CREATE TABLE orders (
    order_id int PRIMARY KEY,
    customer_id int NOT NULL,
    order_date date NOT NULL
);

CREATE TABLE products (
    product_id int PRIMARY KEY,
    product_name varchar(100) NOT NULL,
    category varchar(100),
    price float
);

CREATE TABLE order_items (
    order_item_id int PRIMARY KEY,
    order_id int NOT NULL,
    product_id int NOT NULL,
    quantity int,
    price float,
    FOREIGN KEY (order_id)
    REFERENCES orders (order_id)
    ON DELETE CASCADE,
    FOREIGN KEY (product_id)
    REFERENCES products (product_id)
    ON DELETE CASCADE
);

COPY orders (order_id, customer_id, order_date)
FROM '/var/lib/postgres/table/orders.csv'
WITH (FORMAT csv, HEADER TRUE, DELIMITER ',');

COPY products (product_id, product_name, category, price)
FROM '/var/lib/postgres/table/products.csv'
WITH (FORMAT csv, HEADER TRUE, DELIMITER ',');

COPY order_items (order_item_id, order_id, product_id, quantity, price)
FROM '/var/lib/postgres/table/order_items.csv'
WITH (FORMAT csv, HEADER TRUE, DELIMITER ',');

CREATE TABLE IF NOT EXISTS product_analytics_monthly (
    product_id int4 NOT NULL,
    total_quantity int8,
    total_revenue float8,
    order_count int8,
    avg_rating float8,
    positive_reviews int8,
    negative_reviews int8,
    total_reviews int8,
    processing_date date
);

-- Далее идет скрипт для проверки чисто psql

/*CREATE TABLE IF NOT EXISTS reviews (
    _id text,
    review_id      text,
    product_id     int      NOT NULL,
    customer_id    int,
    rating         int      CHECK (rating BETWEEN 1 AND 5),
    created_at     timestamptz,
    helpful_votes  int
);

COPY reviews (_id, review_id, product_id, customer_id, rating, created_at, helpful_votes)
FROM '/var/lib/postgres/table/data.reviews.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

CREATE TABLE IF NOT EXISTS analyticss (
    product_id int4 NOT NULL,
    total_quantity int8,
    total_revenue float8,
    order_count int8,
    avg_rating float8,
    positive_reviews int8,
    negative_reviews int8,
    total_reviews int8,
    processing_date date
);

WITH params AS (
    SELECT DATE '2025-08-01' AS date_from,   -- начиная с 01.08.2025 00:00:00
           DATE '2025-08-31' AS date_to      -- по 31.08.2025 23:59:59
),

sales_metrics AS (
    SELECT
        oi.product_id,
        COUNT(DISTINCT oi.order_id)                 AS order_count,
        SUM(oi.quantity)                            AS total_quantity,
        SUM(oi.quantity * oi.price)::numeric(18,2) AS total_revenue
    FROM order_items oi
    JOIN orders o         ON o.order_id = oi.order_id
    JOIN params p         ON o.order_date BETWEEN p.date_from AND p.date_to
    GROUP BY oi.product_id
),

reviews_metrics AS (
    SELECT
        r.product_id,
        AVG(r.rating)::numeric(10,2)                           AS avg_rating,
        COUNT(*)                                               AS total_reviews,
        SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END)        AS positive_reviews,
        SUM(CASE WHEN r.rating <= 2 THEN 1 ELSE 0 END)        AS negative_reviews
    FROM reviews r
    JOIN params p ON r.created_at::date BETWEEN p.date_from AND p.date_to
    GROUP BY r.product_id
),

analytics AS (
    SELECT
        COALESCE(s.product_id, rm.product_id)  AS product_id,
        COALESCE(s.total_quantity,     0)      AS total_quantity,
        COALESCE(s.total_revenue,      0)      AS total_revenue,
        COALESCE(s.order_count,        0)      AS order_count,
        COALESCE(rm.avg_rating,        0)      AS avg_rating,
        COALESCE(rm.positive_reviews,  0)      AS positive_reviews,
        COALESCE(rm.negative_reviews,  0)      AS negative_reviews,
        COALESCE(rm.total_reviews,     0)      AS total_reviews,
        CURRENT_DATE                              AS processing_date
    FROM sales_metrics   s
    FULL OUTER JOIN reviews_metrics rm USING (product_id)
)

INSERT INTO analyticss(
    product_id,
    total_quantity,
    total_revenue,
    order_count,
    avg_rating,
    positive_reviews,
    negative_reviews,
    total_reviews,
    processing_date
)
SELECT
    product_id,
    total_quantity,
    total_revenue,
    order_count,
    avg_rating,
    positive_reviews,
    negative_reviews,
    total_reviews,
    processing_date
FROM analytics;*/