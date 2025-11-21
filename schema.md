### TimescaleDB schema



#### zie documentatie

https://github.com/timescale/timescaledb



#### Aanmaken van tabellen



(-> …) is geen deel vd code, gwn comments



##### machine\_sensors



CREATE TABLE machine\_sensors (
	time TIMESTAMPTZ NOT NULL,

&nbsp;	location TEXT NOT NULL,

&nbsp;	sensor\_id INTEGER CHECK (sensor\_id > 0), -> ?
	...
	PRIMARY KEY (time, sensor\_id)

) WITH (

&nbsp;	timescaledb.hypertable,

&nbsp;	timescaledb.partition\_column='time', -> ?

&nbsp;	timescaledb.segementby='location' -> ?

&nbsp;	



##### sensor\_aggragates





#### aanpassen van de tabellen:



ALTER TABLE tablename

&nbsp;	ADD COLUMN kolomnaam datatype;



#### Data retention policy



SELECT add\_retention\_policy('hypertable name', INTERVAL '90 days')

