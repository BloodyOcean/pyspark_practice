-- psql -U rw_user;

CREATE DATABASE rw_db;

-- \C rw_db;

CREATE TABLE IF NOT EXISTS rw_keys (
    encription_key varchar(60) NOT NULL,
    ecription_date date NOT NULL
);

INSERT INTO rw_keys (encription_key, ecription_date)
VALUES 
('S0dWhWFvS0dWhWFv', '2024-07-11'),
('vEdQWZyyS0dWhWFv', '2024-07-12');