CREATE TABLE saifu_users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL
);

CREATE TABLE saifu_portfolios (
    id SERIAL PRIMARY KEY,
    user_id int REFERENCES saifu_users(id),
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP,
    description TEXT NULL
);

CREATE TABLE saifu_portfolio_positions (
    portfolio_id int REFERENCES saifu_portfolios(id),
    ticker VARCHAR(30) NOT NULL,
    size DOUBLE PRECISION NOT NULL CHECK(size >= 0),
    PRIMARY KEY (portfolio_id, ticker)
);

CREATE TABLE saifu_portfolio_historical_prices (
    portfolio_id int REFERENCES saifu_portfolios(id),
    balance DOUBLE PRECISION NOT NULL CHECK(balance >= 0),
    currency VARCHAR(30) NOT NULL,
    quote_time TIMESTAMP
);

CREATE TABLE saifu_portfolio_pricing_jobs (
    id CHAR(32) PRIMARY KEY,
    portfolio_id int REFERENCES saifu_portfolios(id),
    status CHAR(1) NOT NULL,
    target_ccy VARCHAR(30) NOT NULL,
    started_by VARCHAR(255) NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    start_time TIMESTAMP NOT NULL DEFAULT NOW(),
    end_time TIMESTAMP
);

CREATE TABLE saifu_portfolio_pricing_settings (
    portfolio_id int REFERENCES saifu_portfolios(id),
    pricing_interval int NOT NULL,
    target_ccy VARCHAR(30)
);

CREATE TABLE saifu_ccy_historical_prices (
    ticker VARCHAR(30) NOT NULL,
    price DOUBLE PRECISION NOT NULL CHECK(price >= 0),
    quote_time TIMESTAMP
);
