DROP TABLE IF EXISTS mock_data;

CREATE TABLE mock_data (
    id INTEGER,

    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_age INTEGER,
    customer_email VARCHAR(255),
    customer_country VARCHAR(100),
    customer_postal_code VARCHAR(50),
    customer_pet_type VARCHAR(100),
    customer_pet_name VARCHAR(100),
    customer_pet_breed VARCHAR(100),

    seller_first_name VARCHAR(100),
    seller_last_name VARCHAR(100),
    seller_email VARCHAR(255),
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(50),

    product_name VARCHAR(255),
    product_category VARCHAR(100),
    product_price NUMERIC(10, 2),
    product_quantity INTEGER,

    sale_date DATE,
    sale_customer_id INTEGER,
    sale_seller_id INTEGER,
    sale_product_id INTEGER,
    sale_quantity INTEGER,
    sale_total_price NUMERIC(10, 2),

    store_name VARCHAR(255),
    store_location VARCHAR(255),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_country VARCHAR(100),
    store_phone VARCHAR(50),
    store_email VARCHAR(255),

    pet_category VARCHAR(100),

    product_weight NUMERIC(10, 2),
    product_color VARCHAR(100),
    product_size VARCHAR(100),
    product_brand VARCHAR(100),
    product_material VARCHAR(100),
    product_description TEXT,
    product_rating NUMERIC(3, 2),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE,

    supplier_name VARCHAR(255),
    supplier_contact VARCHAR(255),
    supplier_email VARCHAR(255),
    supplier_phone VARCHAR(50),
    supplier_address VARCHAR(255),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);
