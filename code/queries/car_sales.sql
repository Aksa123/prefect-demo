select 
	sales.id as sale_id,
	brands.id as brand_id,
	brands.name as brand_name,
	cars.id as car_id,
	cars.name as car_name,
	sales.price as sale_price,
	sales.currency_id,
	currencies.name as currency_name,
	sales.transaction_date
from car_sales as sales 
left join cars on sales.car_id = cars.id
left join brands on cars.brand_id = brands.id
left join customers on sales.customer_id = customers.id
left join cities on customers.address_city_id = cities.id
left join currencies on sales.currency_id = currencies.id
where
sales.transaction_date >= ? 
and sales.transaction_date <= ? 