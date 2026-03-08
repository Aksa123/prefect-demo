PRAGMA foreign_keys = ON;

create table brands (
	id integer not null primary key autoincrement,
	name text unique not null,
	created_at datetime not null default current_timestamp,
	updated_at datetime,
	deleted_at datatime
);

create table types (
	id integer not null primary key autoincrement,
	name text unique not null,
	created_at datetime not null default current_timestamp,
	updated_at datetime,
	deleted_at datatime
);

create table currencies (
	id integer not null primary key autoincrement,
	name text not null unique,
	created_at datetime not null default current_timestamp,
	updated_at datetime,
	deleted_at datatime
);

create table cars (
	id integer not null primary key autoincrement,
	name text not null,
	brand_id integer not null,
	type_id integer not null,
	price numeric not null check(price >= 0),
	currency_id integer not null,
	stock integer not null default 0 check(stock >= 0),
	release_date date not null,
	created_at datetime not null default current_timestamp,
	updated_at datetime,
	deleted_at datatime,
	foreign key (brand_id) references brands(id) on delete cascade,
	foreign key (type_id) references types(id),
	foreign key (currency_id) references currencies(id)
);

create table cities (
	id integer not null primary key autoincrement,
	name text not null unique,
	created_at datetime not null default current_timestamp,
	updated_at datetime,
	deleted_at datatime
)

create table customers (
	id integer not null primary key autoincrement,
	name text not null unique,
	address_city_id integer not null,
	created_at datetime not null default current_timestamp,
	updated_at datetime,
	deleted_at datatime,
	foreign key (address_city_id) references cities(id)
);


create table car_sales (
	id integer not null primary key autoincrement,
	car_id integer not null,
	customer_id integer not null,
	price numeric not null check(price >= 0), -- snapshot transaction price, must not change
	currency_id integer not null,
	transaction_date datetime not null,
	created_at datetime not null default current_timestamp,
	updated_at datetime,
	deleted_at datatime,
	foreign key (car_id) references cars(id),
	foreign key (customer_id) references customers(id),
	foreign key (currency_id) references currencies(id)
);



