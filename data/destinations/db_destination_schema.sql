PRAGMA foreign_keys = ON;

create table car_sales_dataset (
	sale_id int not null primary key ,
	brand_id int not null,
	brand_name varchar not null,
	car_id int not null,
	car_name varchar not null,
	sale_price numeric not null check (sale_price > 0),
	currency_id int not null,
	currency_name varchar not null,
	sale_price_normalized numeric not null check (sale_price_normalized > 0),
	sale_price_normalized_currency varchar not null,
	exchange_rate_date datetime with time zone not null,
	transaction_date datetime not null
)
