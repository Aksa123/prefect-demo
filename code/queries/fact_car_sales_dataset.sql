select 
	sales.id as sale_id,
	brands.id as brand_id,
	brands.name as brand_name,
	cars.id as car_id,
	cars.name as car_name,
	sales.price as sale_price,
	sales.currency_id,
	currencies.name as currency_name,
	sales.price / rates.rate as sale_price_normalized,
	ccode.code as sale_price_normalized_currency,
	rates.updated_at  as exchange_rate_date,
	sales.transaction_date
from car_sales as sales 
inner join cars on sales.car_id = cars.id
inner join brands on cars.brand_id = brands.id
inner join customers on sales.customer_id = customers.id
inner join cities on customers.address_city_id = cities.id
inner join currencies on sales.currency_id = currencies.id
inner join currency_rates rates on currencies.name = rates.code
inner join (
	select code
	from currency_rates
	where rate = 1
	limit 1
) as ccode on true
where coalesce(sales.updated_at, sales.created_at) between ? and ?