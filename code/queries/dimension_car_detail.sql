select 
	cars.id, 
	cars.name as car_name, 
	brands.id as brand_id, 
	brands.name as brand_name, 
	types.id as type_id, 
	types.name as type_name,
	cars.created_at ,
	case 
		when coalesce(cars.updated_at, cars.created_at) >= coalesce(brands.updated_at, brands.created_at) then cars.updated_at
		else brands.updated_at
	end as updated_at,
	case 
		when coalesce(cars.deleted_at, cars.created_at) >= coalesce(brands.deleted_at, brands.created_at) then cars.deleted_at
		else brands.deleted_at
	end as deleted_at
from cars
inner join brands on cars.brand_id = brands.id 
inner join types on cars.type_id = types.id
where
(coalesce(cars.updated_at, cars.created_at) between ? and ?)
or (coalesce(brands.updated_at, brands.created_at) between ? and ? )
or (coalesce(types.updated_at, types.created_at) between ? and ? )
