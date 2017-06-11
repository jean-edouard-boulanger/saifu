CREATE TABLE saifu_users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL
);

CREATE TABLE saifu_portfolios (
    id SERIAL PRIMARY KEY,
    user_id int REFERENCES saifu_users(id),
    name VARCHAR(255) NOT NULL,
    description TEXT NULL
);

CREATE TABLE saifu_portfolio_positions (
    portfolio_id int REFERENCES saifu_portfolios(id),
    ticker CHAR(3) NOT NULL,
    size DOUBLE PRECISION NOT NULL CHECK(size >= 0),
    PRIMARY KEY (portfolio_id, ticker)
);

INSERT INTO saifu_users (id, email, username) VALUES (1, 'jean.edouard.boulanger@gmail.com', 'jean.boulanger');
INSERT INTO saifu_portfolios (id, user_id, name, description) VALUES (1, 1, 'default', 'Default portfolio');
INSERT INTO saifu_portfolio_positions (portfolio_id, ticker, size) VALUES (1, 'XRP', 2500.0);
INSERT INTO saifu_portfolio_positions (portfolio_id, ticker, size) VALUES (1, 'XMR', 30.0);
INSERT INTO saifu_portfolio_positions (portfolio_id, ticker, size) VALUES (1, 'XML', 14000.0);
INSERT INTO saifu_portfolio_positions (portfolio_id, ticker, size) VALUES (1, 'ETH', 25.0);
INSERT INTO saifu_portfolio_positions (portfolio_id, ticker, size) VALUES (1, 'XBT', 0.71865);
