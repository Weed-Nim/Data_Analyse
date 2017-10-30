CREATE TABLE da_settings (
	id int primary key auto increment not null,
	id_portal int not null,
	max_day int not null,
	prom_price decimal null,
	min_price decimal null,
	max_price decimal null
);