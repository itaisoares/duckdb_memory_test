-- init.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    uuid UUID DEFAULT gen_random_uuid(),
    name TEXT,
    birthdate DATE,
    address TEXT,
    telephone TEXT
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT,
    description TEXT
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_id INT REFERENCES products(id),
    order_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    price NUMERIC,
    quantity INT
);

-- Insert 100 million rows into users
INSERT INTO users (name, birthdate, address, telephone)
SELECT
    'User ' || generate_series(1, 50000000),
    DATE '1970-01-01' + (random() * (DATE '2000-01-01' - DATE '1970-01-01'))::int,
    'Address ' || generate_series(1, 50000000),
    'Telephone ' || generate_series(1, 50000000);

-- Insert 10,000 rows into products
INSERT INTO products (name, description)
SELECT
    'Product ' || generate_series(1, 10000),
    'Description for product ' || generate_series(1, 10000);

-- Insert 100 million rows into orders
INSERT INTO orders (user_id, product_id, price, quantity)
SELECT
    (random() * 49999999 + 1)::int,
    (random() * 9999 + 1)::int,
    (random() * 100)::numeric,
    (random() * 10)::int + 1
FROM generate_series(1, 50000000);
