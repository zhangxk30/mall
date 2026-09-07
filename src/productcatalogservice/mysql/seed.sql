-- Seed data generated from ../products.json (9 products).
-- Run after schema.sql: mysql -h <host> -u <user> -p <db> < seed.sql

INSERT INTO products (id, name, description, picture, price_currency_code, price_units, price_nanos, categories) VALUES
('OLJCESPC7Z', 'Sunglasses', 'Add a modern touch to your outfits with these sleek aviator sunglasses.', '/static/img/products/sunglasses.jpg', 'USD', 19, 990000000, 'accessories'),
('66VCHSJNUP', 'Tank Top', 'Perfectly cropped cotton tank, with a scooped neckline.', '/static/img/products/tank-top.jpg', 'USD', 18, 990000000, 'clothing,tops'),
('1YMWWN1N4O', 'Watch', 'This gold-tone stainless steel watch will work with most of your outfits.', '/static/img/products/watch.jpg', 'USD', 109, 990000000, 'accessories'),
('L9ECAV7KIM', 'Loafers', 'A neat addition to your summer wardrobe.', '/static/img/products/loafers.jpg', 'USD', 89, 990000000, 'footwear'),
('2ZYFJ3GM2N', 'Hairdryer', 'This lightweight hairdryer has 3 heat and speed settings. It''s perfect for travel.', '/static/img/products/hairdryer.jpg', 'USD', 24, 990000000, 'hair,beauty'),
('0PUK6V6EV0', 'Candle Holder', 'This small but intricate candle holder is an excellent gift.', '/static/img/products/candle-holder.jpg', 'USD', 18, 990000000, 'decor,home'),
('LS4PSXUNUM', 'Salt & Pepper Shakers', 'Add some flavor to your kitchen.', '/static/img/products/salt-and-pepper-shakers.jpg', 'USD', 18, 490000000, 'kitchen'),
('9SIQT8TOJO', 'Bamboo Glass Jar', 'This bamboo glass jar can hold 57 oz (1.7 l) and is perfect for any kitchen.', '/static/img/products/bamboo-glass-jar.jpg', 'USD', 5, 490000000, 'kitchen'),
('6E92ZMYYFZ', 'Mug', 'A simple mug with a mustard interior.', '/static/img/products/mug.jpg', 'USD', 8, 990000000, 'kitchen');
