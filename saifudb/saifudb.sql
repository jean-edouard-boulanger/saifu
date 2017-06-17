CREATE TABLE saifu_users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL
);

CREATE TABLE saifu_ports (
    id SERIAL PRIMARY KEY,
    user_id int REFERENCES saifu_users(id),
    name VARCHAR(255) NOT NULL,
    description TEXT NULL
);

CREATE TABLE saifu_port_positions (
    portfolio_id int REFERENCES saifu_ports(id),
    ticker VARCHAR(30) NOT NULL,
    size DOUBLE PRECISION NOT NULL CHECK(size >= 0),
    PRIMARY KEY (portfolio_id, ticker)
);

CREATE TABLE saifu_port_price_histo (
    portfolio_id int REFERENCES saifu_portfolios(id),
    price DOUBLE PRECISION NOT NULL CHECK(price >= 0),
    created_at TIMESTAMP
);

CREATE TABLE saifu_asset_price_histo (
    ticker VARCHAR(30) NOT NULL,
    price DOUBLE PRECISION NOT NULL CHECK(price >= 0),
    quote_time TIMESTAMP
);
