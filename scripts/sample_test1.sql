-- sample_test.sql

-- Create a table to store customer data
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert some dummy data
INSERT INTO customers (customer_id, first_name, last_name, email)
VALUES 
    (1, 'Alice', 'Smith', 'alice@example.com'),
    (2, 'Bob', 'Johnson', 'bob@example.com');

-- A deliberately inefficient query (for testing review findings)
-- This uses SELECT * instead of column names and no WHERE clause
SELECT * 
FROM customers
JOIN orders ON customers.customer_id = orders.customer_id;

-- Update query without WHERE clause (possible critical issue)
UPDATE customers
SET email = 'test@example.com';
