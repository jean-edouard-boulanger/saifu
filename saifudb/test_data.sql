INSERT INTO saifu_users (id, email, username) VALUES (1, 'jean.edouard.boulanger@gmail.com', 'jean.boulanger');
INSERT INTO saifu_portfolios (id, user_id, name, description) VALUES (1, 1, 'default', 'Default portfolio');
INSERT INTO saifu_portfolio_positions (portfolio_id, ticker, size) VALUES (1, 'XRP', 2500.0);
INSERT INTO saifu_portfolio_positions (portfolio_id, ticker, size) VALUES (1, 'XMR', 30.0);
INSERT INTO saifu_portfolio_positions (portfolio_id, ticker, size) VALUES (1, 'XML', 14000.0);
INSERT INTO saifu_portfolio_positions (portfolio_id, ticker, size) VALUES (1, 'ETH', 25.0);
INSERT INTO saifu_portfolio_positions (portfolio_id, ticker, size) VALUES (1, 'BTC', 0.71865);
INSERT INTO saifu_portfolio_pricing_settings (portfolio_id, pricing_interval, target_ccy) VALUES (1, 10, 'USD');
