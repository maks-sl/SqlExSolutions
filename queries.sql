
select s.name
from
(Select *, rank() over(partition by displacement order by numGuns desc) AS pos 
from 
	(
	select c.class, displacement, numGuns
	from 
	classes c join ships s ON c.class = s.class
	UNION
	select c.class, displacement, numGuns
	from 
	classes c join outcomes o ON c.class = o.ship
	) a
where displacement IS NOT NULL AND numGuns IS NOT NULL) x
join ships s ON x.class = s.class
where x.pos = 1

UNION

select o.ship
from
(Select *, rank() over(partition by displacement order by numGuns desc) AS pos 
from 
	(
	select c.class, displacement, numGuns
	from 
	classes c join ships s ON c.class = s.class
	UNION
	select c.class, displacement, numGuns
	from 
	classes c join outcomes o ON c.class = o.ship
	) a
where displacement IS NOT NULL AND numGuns IS NOT NULL) x
join outcomes o ON x.class = o.ship
where x.pos = 1

-- ************************************ 55 *****************************



select c.class, s.y_min
from
(select class from ships where name IN(select class from classes) and launched IS NULL) c
join (select class, min(launched) y_min from ships group by class) s
ON c.class = s.class

UNION

select c.class, s.launched
from
classes c join ships s
ON c.class = s.name AND s.launched IS NOT NULL

UNION

select c.class, s.y_min
from 
(select class from classes where class NOT IN (select name from ships)) c 
left join 
(select class, min(launched) y_min from ships group by class) s
ON 
c.class = s.class

-- **********************57****************************

with cte as (
	select a_cs.class, a_cs.name, o.isSunk
	from
	(
		select c.class, a_s.name
		from
		classes c
		LEFT JOIN
		(
			select name, class from ships
			UNION
			select ship, ship from outcomes
		) a_s
		ON c.class = a_s.class
	) a_cs
	LEFT JOIN
	(
		select ship, 1 as isSunk from outcomes where result = 'sunk'
	) o
	ON a_cs.name = o.ship
)

select class, count(isSunk) from cte
group by class
having count(name)>=3 AND count(isSunk)>=1

-- ***********************58*************************

SELECT 
	b.maker, 
	b.type , 
	CASE   
         WHEN a.pr IS NULL THEN CAST(0 as NUMERIC(6,2))
         ELSE a.pr
	END  prr 
FROM
(
	Select 
		np_mt.maker, 
		np_mt.type,
		CAST((np_mt.np_mt/(NULLIF(np_m.np_m,0)*0.01)) AS NUMERIC(6,2)) pr
	from
		(
			select maker, count(*) np_m from product group by maker
		) np_m
		JOIN
		(
			select maker, type, count(model) np_mt from product group by maker, type
		) np_mt
		ON np_m.maker = np_mt.maker
) a
RIGHT JOIN (
	Select maker,type
	from
	(
		(select distinct maker from product) x
		CROSS JOIN
		(select distinct type from product) y
	)
) b
ON a.maker = b.maker AND a.type = b.type

-- **************************59**************************************


select x.point, 
CASE 
WHEN sum_pi IS NULL 
THEN 0 
ELSE sum_pi
END
 - 
CASE 
WHEN sum_po IS NULL 
THEN 0 
ELSE sum_po
END
as dif
from
(select pi.point, pi.sum_pi, po.sum_po
from
(Select point, sum(inc) sum_pi from income_o group by point) pi
full join
(Select point, sum(out) sum_po from outcome_o group by point) po
ON pi.point = po.point) x



-- *********************************60************************************


SELECT p.name FROM
(
	SELECT distinct pit.ID_psg FROM
	(
	select distinct
	ID_psg,
	ROW_NUMBER() over(partition by ID_psg, place order by ID_psg) as num_tp
	from pass_in_trip
	) pit
	where pit.num_tp > 1
) pit1
JOIN Passenger p 
ON p.ID_psg = pit1.ID_psg


select i.point, 
(CASE 	WHEN i.sum_i IS NULL THEN 0	ELSE i.sum_i	END) - 
(CASE	WHEN o.sum_o IS NULL THEN 0	ELSE o.sum_o	END) as total  
from i full join o
on i.point=o.point

-- ********************************61************************************

select sum(detail_p.diff)
from
(
select x.point, 
CASE 
WHEN sum_pi IS NULL 
THEN 0 
ELSE sum_pi
END
 - 
CASE 
WHEN sum_po IS NULL 
THEN 0 
ELSE sum_po
END
as diff
from
(select pi.point, pi.sum_pi, po.sum_po
from
(Select point, sum(inc) sum_pi from income_o group by point) pi
full join
(Select point, sum(out) sum_po from outcome_o group by point) po
ON pi.point = po.point) x 
) detail_p

-- *******************************62********************************

select sum(detail_p.diff)
from
(
select x.point, 
CASE 
WHEN sum_pi IS NULL 
THEN 0 
ELSE sum_pi
END
 - 
CASE 
WHEN sum_po IS NULL 
THEN 0 
ELSE sum_po
END
as diff
from
(select pi.point, pi.sum_pi, po.sum_po
from
(Select point, sum(inc) sum_pi from income_o WHERE date < CONVERT(DATETIME, '2001-04-15', 120) group by point) pi
full join
(Select point, sum(out) sum_po from outcome_o WHERE date < CONVERT(DATETIME, '2001-04-15', 120) group by point) po
ON pi.point = po.point) x 
) detail_p

-- ***************************63***********************************

SELECT p.name FROM
(
	SELECT distinct pit.ID_psg FROM
	(
	select distinct
	ID_psg,
	ROW_NUMBER() over(partition by ID_psg, place order by ID_psg) as num_tp
	from pass_in_trip
	) pit
	where pit.num_tp > 1
) pit1
JOIN Passenger p 
ON p.ID_psg = pit1.ID_psg


-- ***************************** 64*******************************

with i(point, date, sum) as (
SELECT point, date, sum(inc) sum_i
FROM Income
group by point, date),

o (point, date, sum) as (
SELECT point, date, sum(out) sum_o
FROM Outcome
group by point, date)

SELECT 

CASE WHEN i_p IS NULL THEN o_p ELSE i_p END as point,
CASE WHEN i_d IS NULL THEN o_d ELSE i_d END as date,
CASE WHEN i_d IS NULL THEN 'out' ELSE 'inc' END as type,
CASE WHEN i_s IS NULL THEN o_s ELSE i_s END as sum
FROM
(
(select i.point i_p, i.date i_d, i.sum i_s, o.point o_p, o.date o_d, o.sum o_s from
o full join i ON o.point = i.point AND o.date = i.date)
except
(select i.point i_p, i.date i_d, i.sum i_s, o.point o_p, o.date o_d, o.sum o_s from
o join i ON o.point = i.point AND o.date = i.date)
)x

-- ****************************65****************************
select num_pair, CASE m_row WHEN 1 THEN CAST(maker AS char(10)) ELSE '' END as maker, type
from
	(select maker, type,
	ROW_NUMBER() over(partition by maker order by t_order) as m_row,
	ROW_NUMBER() over(order by maker, t_order) as num_pair
	from
		(select distinct maker, type, t_order from 
			(select maker, type, 
			CASE type WHEN 'PC' THEN 1
					WHEN 'Laptop' THEN 2
					WHEN 'Printer' THEN 3 END as t_order
					from product
			) a
		) b 
	) c
order by num_pair

-- ******************************66********************************

SELECT
d.date, CASE WHEN x.num_trips IS NULL THEN '0' ELSE x.num_trips END num_trips
FROM
	(SELECT CONVERT(DATETIME, '2003-04-01', 120) as date
	UNION SELECT CONVERT(DATETIME, '2003-04-02', 120)
	UNION SELECT CONVERT(DATETIME, '2003-04-03', 120)
	UNION SELECT CONVERT(DATETIME, '2003-04-04', 120)
	UNION SELECT CONVERT(DATETIME, '2003-04-05', 120)
	UNION SELECT CONVERT(DATETIME, '2003-04-06', 120)
	UNION SELECT CONVERT(DATETIME, '2003-04-07', 120)
	) d
LEFT JOIN
	(SELECT pit.date, count(distinct pit.trip_no) num_trips
	FROM
		Pass_in_trip pit
		join
		Trip t
		on t.trip_no = pit.trip_no 
			and pit.date >= CONVERT(DATETIME, '2003-04-01', 120)
			and pit.date <= CONVERT(DATETIME, '2003-04-07', 120)
			and t.town_from = 'Rostov'
	GROUP BY pit.date) x
ON d.date = x.date
		
-- **************************67*************************
SELECT count(route_numtrips_pos) as num_trips
FROM
	(Select rank() over(order by route_numtrips desc) as route_numtrips_pos
	FROM
		(SELECT count(distinct trip_no) route_numtrips
		FROM
		Trip 
		group by town_from, town_to) a
	) b
WHERE route_numtrips_pos = 1
GROUP BY route_numtrips_pos

-- **************************68***************************
with a(tf, tt, n_trips) as(
	SELECT town_from, town_to, count(distinct trip_no) n_trips
	FROM
	Trip
	group by town_from, town_to
),

b(n_trips) as(
		select a.n_trips + a1.n_trips as n_trips
		from
		a join a a1
		on a.tf = a1.tt AND a.tt = a1.tf AND a.tt < a1.tt
	union all
		select a.n_trips
		from
		a left join a a1
		on a.tf = a1.tt AND a.tt = a1.tf
		where a1.tf IS NULL
)

select count(n_trips_pos)
from
	(select rank() over(order by n_trips desc) as n_trips_pos
	from b) c
where n_trips_pos = 1
group by n_trips_pos

-- *************************69*****************************
	
	with i(point, date, inc) as(
	select point, date, sum(inc) inc
	from income
	group by point, date
),
o(point, date, out) as(
	select point, date, sum(out) out
	from outcome
	group by point, date
)

SELECT 
point,
CONVERT(date , date, 103) date, 
sum(inc) over(partition by point order by date RANGE UNBOUNDED PRECEDING) -
sum(out) over(partition by point order by date RANGE UNBOUNDED PRECEDING) as total
FROM
(
	SELECT
	COALESCE(i.point, o.point) point, 
	COALESCE(i.date, o.date) date, 
	COALESCE(i.inc, '0') inc, 
	COALESCE(o.out, '0') out
	FROM
	i full join o
	on i.date=o.date and i.point = o.point) x

-- **************70**********************

select battle, count(ship)
from
(
	Select o.battle, c.country, o.ship
	from 
	outcomes o join ships s on o.ship = s.name
	join classes c on s.class = c.class
	union all
	select o.battle, c.country, o.ship
	from
	outcomes o join classes c on o.ship = c.class
	where o.ship not in (select name from ships)
) x

group by battle, country
having count(ship) >= 3

-- ********************71*******************
select maker from product where type='pc'
except
select p.maker
from
product p left join pc ON p.model = pc.model
where pc.model IS NULL AND p.type='pc'

*******************72********************
with a(id_psg, id_comp) as 
(select distinct pit.id_psg, t.id_comp
from
pass_in_trip pit join trip t on pit.trip_no=t.trip_no)

select p.name, trips
	from (
	select id_psg, trips, rank() over(order by trips desc) n_trips_pos
	from (
		select id_psg, count(trip_no) over (partition by id_psg) as trips
		from pass_in_trip
		where id_psg in (
			select id_psg
			 from a 
			group by id_psg
			having count(id_comp)=1)) x
			) a
			join passenger p on a.id_psg = p.id_psg
	where n_trips_pos = 1
	
-- *************************73**************************

select c.country, b.name
from (select distinct country from classes) c, (select distinct name from battles) b

EXCEPT
	(
	select c.country, o.battle
	from outcomes o join classes c on o.ship = c.class and o.ship NOT IN (select name from ships)
	union
	select c.country, o.battle
	from outcomes o join ships s on s.name = o.ship join classes c on c.class = s.class
	)

-- ***********************74*****************************

select class
from
classes
where country like (
	case
	when (select count(class) cnt from classes where country = 'Russia') = 0 then '%'
	else 'Russia' end
)

-- ****************************75**************************

select s.name, s.launched, 
	(
		CASE WHEN s.launched IS NULL THEN (select top(1) name from battles order by date desc)
		ELSE (
				CASE WHEN 
					convert(datetime, CAST(s.launched AS VARCHAR)+'01'+'01', 112) > ALL(select date from battles) THEN NULL
				ELSE (SELECT top(1) name from battles where date >= convert(datetime, CAST(s.launched AS VARCHAR)+'01'+'01', 112) order by date)
				END
			)
		END
	) cand_f_b
from ships s 

-- ***************************76*****************************

select (select top(1) name from passenger where id_psg = x.id_psg) name, sum(dur) as total
from
	(
	select a.id_psg, p.name, (
			case when time_out > time_in then DATEDIFF(mi, time_out, DATEADD(day, 1, time_in))
			else DATEDIFF(mi, time_out, time_in) 
			end
	) dur
	from
		(select 
		id_psg, 
		
		(select count(distinct place) from pass_in_trip where id_psg = pit.id_psg) num_difp, 
		count(place) over(partition by id_psg) num_p,
		t.time_out,
		t.time_in
		from pass_in_trip pit join trip t on t.trip_no = pit.trip_no) a
		join passenger p on p.id_psg = a.id_psg
	where num_difp = num_p
	) x
group by id_psg

-- ******************************** 77 ******************************

select n_trips, date
from
	(
	select date, n_trips, rank() over(order by n_trips desc) rate
	from
		(select date, count(distinct pit.trip_no) n_trips
		from
		pass_in_trip pit join trip t on t.trip_no = pit.trip_no
		where town_from = 'Rostov'
		group by date) fr
	) x
where rate = 1

-- **************************78*********************************


select name, f_d,
	convert(char(4), DATEPART(yyyy, l_d))
	+'-'+
	RIGHT('0'+convert(varchar, DATEPART(mm, l_d)), 2)
	+'-'+
	RIGHT('0'+convert(varchar, DATEPART(dd, l_d)), 2)	l_d
from
	(
	Select name, 
	convert(char(4), DATEPART(yyyy, date))
	+'-'+
	RIGHT('0'+convert(varchar, DATEPART(mm, date)), 2)
	+'-01' f_d,


	dateadd(dd, -1, 
		dateadd(mm, 1, 
			convert(datetime,
				convert(char(4), DATEPART(yyyy, date))
				+
				RIGHT('0'+convert(varchar, DATEPART(mm, date)), 2)
				+
				'01'
			, 112)
		)
	) l_d
	from battles
	) x

-- **************************79****************************
select name, t_in_t
from
	(select b.id_psg, p.name, t_in_t, rank() over(order by t_in_t desc) as t_in_t_pos
	from
		(select id_psg, sum(t_in_t) as t_in_t
		from
			(Select id_psg,
				case
				when time_in < time_out then datediff(mi, time_out, dateadd(day, 1, time_in))
				else datediff(mi, time_out, time_in)
				end t_in_t
			from
			pass_in_trip pit join trip t on pit.trip_no = t.trip_no) a
		group by id_psg) b
	join passenger p on p.id_psg = b.id_psg) c
where t_in_t_pos = 1

--*************************80*****************************

SELECT maker from product
except
select maker from product p left join pc on p.model = pc.model  where pc.model IS NULL and p.type='pc'

--************************81*************************
select o.code, o.point, o.date, o.out
from
	(select a.y, a.m, sum_o, rank() over(order by sum_o desc) as pos
	from
		(select year(date) y, month(date) m, sum(out) sum_o
		from outcome
		group by year(date), month(date) ) a
	) b
	join
		outcome o
		on year(o.date) = b.y and month(o.date) = b.m
	where pos = 1
	
-- ***************************82******************************

select TOP(SELECT COUNT(code)-5 from pc)
	code,
	min(code) over(order by rn ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) as min_code,
	avg(price) over(order by rn ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) as avg
from
	(select row_number() over(order by code) rn, count(code) over() n_codes, code, price
	from pc) a

-- ***************************83*********************************

with a(name, class, numGuns, bore, displacement, type, launched, class, country) 
as	(select s.name, c.class, c.numGuns, c.bore, c.displacement, c.type, c.launched, c.class, c.country
	from 
	ships s join classes c on c.class = s.class)

	
select name, count(name)
from
	(
	select name from a where numGuns = 8
	union all
	select name from a where bore = 15 
	union all
	select name from a where displacement = 32000 
	union all
	select name from a where type = 'bb' 
	union all
	select name from a where launched = 1915 
	union all
	select name from a where class='Kongo' 
	union all
	select name from a where country='USA' 
	) b
group by name
having count(name) >= 4

-- *************************84**********************************
with b(name, decade, n_pass) as(select distinct name, decade, count(*) over(partition by id_comp, decade) n_pass
	from
		(select c.name, c.id_comp, date, 
			case
			when day(date) between 1 and 10 then 1
			when day(date) between 11 and 20 then 2
			when day(date) between 21 and 31 then 3
			end as decade
		from
		pass_in_trip pit join trip t on t.trip_no = pit.trip_no join company c on t.id_comp = c.id_comp
		where year(date)=2003 and month(date)=4
		) a
	)

select distinct name, 
	coalesce((select n_pass from b b1 where b1.name=b.name and decade = 1),0) as q,
	coalesce((select n_pass from b b1 where b1.name=b.name and decade = 2),0) as w,
	coalesce((select n_pass from b b1 where b1.name=b.name and decade = 3),0) as e
from
	 b

--*****************************85***********************

(
		select maker
		from
		product where type = 'printer'
	except 
		select maker
		from
		product where type = 'laptop' or type = 'pc'
)

union

(
		select maker from product
		where type='pc'
		group by maker
		having count(model) >= 3
	except 
		select maker
		from
\\	 WHERE b1.maker = b.maker
	 FOR XML PATH('')) types
	FROM b) x

-- **********************87************************

select p.name,
(select count(town_to) 
from pass_in_trip pit join trip t on t.trip_no = pit.trip_no
where town_to = 'Moscow' 
and id_psg = b.id_psg) q

from
	(select id_psg
	from
	pass_in_trip pit join trip t on t.trip_no = pit.trip_no
	where t.town_to = 'Moscow'
	group by id_psg
	having count(town_to) > 1
	intersect
	select id_psg
	from
		(select id_psg, town_from, rank() over(partition by id_psg order by date, time_out) rnk
		from pass_in_trip pit join trip t on t.trip_no = pit.trip_no
		) a
	where town_from <> 'Moscow' and rnk = 1
	)b
	join passenger p on p.id_psg = b.id_psg

-- ***********************88*************************

select p.name, num_tr, 
	(select top(1) co.name 
	from 
	pass_in_trip pit join trip t on t.trip_no = pit.trip_no join company co on co.id_comp = t.id_comp
	where pit.id_psg = c.id_psg
	) comp
from
	(select 
		a.id_psg, 
		num_tr, 
		rank() over(order by num_tr desc) trips_rnk
	from
		(select id_psg, count(trip_no) num_tr
		from pass_in_trip
		group by id_psg) a
	join
		(select id_psg, count(distinct t.id_comp) n_comps
		from pass_in_trip pit join trip t on t.trip_no = pit.trip_no
		group by id_psg
		having count(distinct t.id_comp) = 1) b
	on a.id_psg = b.id_psg) c
join passenger p
on c.id_psg = p.id_psg
where trips_rnk = 1

-- ****************************89*********************************
with b (maker, cnt_model, min_rnk, max_rnk) as
	(select maker, cnt_model, 
		rank() over(order by cnt_model) min_rnk, 
		rank() over(order by cnt_model desc) max_rnk
	from
		(select maker, count(model) cnt_model
		from
		product
		group by maker) a
	)
	
select maker, cnt_model
from b
where min_rnk = 1
union
select maker, cnt_model
from b
where max_rnk = 1

-- ****************************90******************************
select maker, model, type
from
	(select maker, model, type, 
		dense_rank() over(order by model) min_rnk,
		dense_rank() over(order by model desc) max_rnk
	from
	product) a
where min_rnk > 3 and max_rnk > 3

-- ****************************91*****************************
select count(maker) cnt
from
	(select maker
	from
	product
	group by maker
	having count(model) = 1) a
	
-- ****************************92*****************************

with a(empt_b) as
	(select b_v_id empt_b
	from utB
	group by b_v_id
	having sum(b_vol) = 255),
b(wh_q) as	
	(select b_q_id wh_q
	from
	utB
	group by b_q_id
	having sum(b_vol) = 765)
	
select utQ.q_name
from
	b
left join
	(select utB.b_q_id not_empt_wh_q
	from
	b join utB on utB.b_q_id = b.wh_q
	where utB.b_v_id NOT IN (select empt_b from a)) c
on b.wh_q = c.not_empt_wh_q
join utQ on utQ.q_id = b.wh_q
where c.not_empt_wh_q IS NULL

-- ***************************93***************************

with a (trip_no, n_trips) as
(select trip_no, count(distinct date) as n_trips
from pass_in_trip
group by trip_no
having count(id_psg) > 0),
b (name, trip_no, diff) as 
(select c.name, trip_no, 
	case when time_in < time_out
	then datediff(mi, time_out, dateadd(day, 1, time_in) )
	else datediff(mi, time_out, time_in)
	end diff
from trip t join company c on c.id_comp = t.id_comp)

select name, sum(mi_trip) as total
from 
	(select name, a.n_trips * b.diff as mi_trip
	from
	a join b on a.trip_no = b.trip_no) x
group by name

-- ****************************94***************************

with a (date, num_trips) as 
(select date, count(t.trip_no) num_trips
from pass_in_trip pit join trip t on pit.trip_no = t.trip_no
where town_from = 'Rostov'
group by date),
c (date) as
(select top(1) date from
	(select date, num_trips, rank() over(order by num_trips desc) n_trips_rnk from a
	) b
where n_trips_rnk = 1
order by date)


select d.date, coalesce(a.num_trips, 0) nTripsFromRostov
from
	(select (select top 1 date from c) as date
	union
	select dateadd(day, 1, (select top 1 date from c))
	union
	select dateadd(day, 2, (select top 1 date from c))
	union
	select dateadd(day, 3, (select top 1 date from c))
	union
	select dateadd(day, 4, (select top 1 date from c))
	union
	select dateadd(day, 5, (select top 1 date from c))
	union
	select dateadd(day, 6, (select top 1 date from c))) d
left join a on d.date = a.date

-- ***************************95***************************
	
	
select distinct
	c.name,
	(select count(*) from 
		(select distinct date, pit1.trip_no from 
		pass_in_trip pit1 join trip t1 on pit1.trip_no = t1.trip_no
		where t1.id_comp = t.id_comp) a
	) as n_all_trips,
	(select count(distinct plane) from 
		pass_in_trip pit1 join trip t1 on pit1.trip_no = t1.trip_no
	where t1.id_comp = t.id_comp
	) as num_planes,
	(select count(distinct id_psg) from 
		pass_in_trip pit1 join trip t1 on pit1.trip_no = t1.trip_no
	where t1.id_comp = t.id_comp
	) as num_peoples,
	(select count(id_psg) from 
		pass_in_trip pit1 join trip t1 on pit1.trip_no = t1.trip_no
	where t1.id_comp = t.id_comp
	) as num_tickets
from

pass_in_trip pit
join trip t on t.trip_no = pit.trip_no
join company c on t.id_comp = c.id_comp

-- **************************96***************************

select V_NAME
from
(	select utV.V_ID
	from utV join utB on utV.V_ID = utB.B_V_ID
	where utV.V_COLOR = 'R'
	group by utV.V_ID
	having count(utB.B_V_ID) > 1
intersect
	select utB.B_V_ID
	from
	utB join utQ on utB.B_Q_ID = utQ.Q_ID
	join utB utB1 on utB1.B_Q_ID = utQ.Q_ID
	join utV utV1 on utV1.V_ID = utB1.B_V_ID
	where utV1.V_COLOR = 'B'
) V
join utV on utV.V_ID = V.V_ID

-- *************************97****************************

with x (code, par, ord) as
	(
	select code, par, row_number() over(partition by code order by par) ord
	from laptop 
	cross apply
	(values(speed), (ram), (price), (screen)) ca(par)
	)

select l.code, speed, ram, price, screen
from
	(select distinct code
	from x 
	cross apply (select top(1) par p1 from x xt1 where xt1.code = x.code and ord=1) x1
	cross apply (select top(1) par p2 from x xt1 where xt1.code = x.code and ord=2) x2
	cross apply (select top(1) par p3 from x xt1 where xt1.code = x.code and ord=3) x3
	cross apply (select top(1) par p4 from x xt1 where xt1.code = x.code and ord=4) x4
	where p1*2 <= p2 AND p2*2 <= p3 AND p3*2 <= p4
	) a
	join laptop l on l.code = a.code
	
-- *************************98****************************

with bspeed as
(select code, speed|ram as num_orig, cast(speed|ram as int) as working_level, cast('' as varchar(max)) as binval
from pc
union all 
select b.code, b.num_orig, b.working_level / 2, cast(b.working_level % 2 as varchar(max)) + b.binval
from bspeed b
where b.working_level > 0
) 
select
	a.code, pc.speed, pc.ram
from
	(select code, num_orig, binval 
	from bspeed 
	where working_level = 0 ) a
join pc on pc.code = a.code
where a.binval LIKE '%1111%'

-- **************************99***************************


-- **********XXXXXXXXXXXXXXX 'через рекурсивную cte и outer apply' XXXXXXXXXXXXXXX**********

with dates(date, max_date) as (
select 
	(select top(1) date from income_o order by date) as date,
	(select max(x_date) from
		(select dateadd(day, 1, max(date)) x_date from outcome_o
		union
		select max(date) from income_o) x
	) as max_date
union all
select 
	case when datepart(dw, dateadd(day, 1, d.date)) = 1 then dateadd(day, 2, d.date)
		else dateadd(day, 1, d.date) end,
	d.max_date
from dates d
where date <= d.max_date
)

select i.point, i.date, x.date
from
income_o i
cross apply
(select top(1) d.date
from dates d left join outcome_o o1 on d.date = o1.date and i.point = o1.point
where d.date >= i.date 
	and o1.date IS NULL 
) x(date)




-- **********XXXXXXXXXXXXXXX 'через таблицу дат и cross apply' XXXXXXXXXXXXXXX**********


with dates(date) as(
	select dateadd(day, n.n, s.st_date) as date
	from
			(select min(date) as st_date from income_o) s
		cross join
			(select (a1 * 10  + a2 * 100 + a3 * 1000 + a4 * 10000 + a5) as n
			from 
					(select 0 as a1 
					union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9
					) a1
				cross join
					(select 0 as a2 
					union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9
					) a2
				cross join
					(select 0 as a3 
					union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9
					) a3
				cross join
					(select 0 as a4 
					union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9
					) a4
				cross join
					(select 0 as a5 
					union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9
					) a5
			) n
	)

select i.point, i.date, x.date
from
income_o i
cross apply
(select top(1) d.date
from dates d left join outcome_o o1 on d.date = o1.date and i.point = o1.point
where d.date >= i.date 
	and o1.date IS NULL 
	and datepart(dw,d.date) <> 1
	order by d.date
) x(date)

-- **********XXXXXXXXXXXXXXX 'через задание доп таблицы' XXXXXXXXXXXXXXX**********

with odi(date, point, di) as (select od.date, od.point,
		case when o1.date IS NOT NULL
			then NULL
		else 
			case when od.d1_dw = 1
				then 
					case when o2.date IS NOT NULL
						then NULL
					else od.d2
					end
			else od.d1
			end
		end as di
	from
		(select point, date, dateadd(day,1,date) d1, datepart(dw, dateadd(day,1,date)) d1_dw, dateadd(day,2,date) d2
		from outcome_o) od 
		left join outcome_o o1 on o1.date = od.d1 and o1.point = od.point
		left join outcome_o o2 on o2.date = od.d2 and o2.point = od.point
	)

select i.point, i.date d, i.date di
from income_o i left join outcome_o o on i.date = o.date and i.point = o.point where o.date IS NULL
union
select i.point, i.date d, (select top(1) di from odi where odi.date >= i.date and odi.point = i.point and odi.di IS NOT NULL) di 
from income_o i join outcome_o o on i.date = o.date and i.point = o.point	


-- **********XXXXXXXXXXXXXXX 'пытался убрать подзапросы в select' XXXXXXXXXXXXXXX**********

with odi(date, point, di, di_prev) as (
	select date, point, di, lag(di) over(partition by point order by di) as di_prev
	from
		(select od.date, od.point,
			case when o1.date IS NOT NULL
				then NULL
			else 
				case when od.d1_dw = 1
					then 
						case when o2.date IS NOT NULL
							then NULL
						else od.d2
						end
				else od.d1
				end
			end as di
		from
			(select point, date, dateadd(day,1,date) d1, datepart(dw, dateadd(day,1,date)) d1_dw, dateadd(day,2,date) d2
			from outcome_o) od 
			left join outcome_o o1 on o1.date = od.d1 and o1.point = od.point
			left join outcome_o o2 on o2.date = od.d2 and o2.point = od.point
		) od1
	)

select x.point, x.date, x.di 
from
(
	select o.date, o.point, o1.di 
	from
	(select * from odi where di is not null and di_prev is null) o1
	right join outcome_o o
	on o.date <= o1.di and o1.point = o.point
union
	select o.date, o.point, o1.di
	from
	(select * from odi where di is not null and di_prev is not null) o1
	right join outcome_o o
	on o.date between o1.di_prev AND o1.di and o1.point = o.point
) x

join income_o i on i.point = x.point and i.date = x.date
where x.di is not null

union

select i.point, i.date d, i.date di
from income_o i left join outcome_o o on i.date = o.date and i.point = o.point where o.date IS NULL



-- *************************************100*****************************************

select *
from
(select coalesce(i.date, o.date) date, coalesce(i_code, o_code) code, i.point pi, i.inc, o.point po, o.out
 from
(select *, row_number() over(partition by date order by code) i_code
from income) i 
full join 
(select *, row_number() over(partition by date order by code) o_code
from outcome) o
on o.date = i.date and o_code = i_code) x
order by date, code

-- ************************************101**************************************

with x(code, model, color, type, price, gr) as 
	(Select code, model, color, type, price, 
	case 
		when code = 1 then 0
		else (select count(*) n_cnt from printer p1 where code > 1 and p1.code <= p.code and p1.color='n') 
	end gr
	from printer p)

select code, model, color, type, price, 
max(model) over(partition by gr) max_model,
(select count(distinct type) from x x1 where x.gr = x1.gr) distinct_types_cou,
avg(price) over(partition by gr) avg_price
from x

-- **********************************102***************************************

select p.name
from
	(select t.trip_no, gr
	from
		(select t1, t2, row_number() over(order by t1) gr
		from 
			(select town_to t1, town_from t2 from trip where town_from > town_to
			union
			select town_from, town_to from trip where town_from < town_to
			) g1
		) g
		join trip t on t.town_to = g.t1 and t.town_from = g.t2 or t.town_to = g.t2 and t.town_from = g.t1
	) gr
	join pass_in_trip pit on pit.trip_no = gr.trip_no
	join passenger p on pit.id_psg = p.id_psg
	group by pit.id_psg, p.name
	having count(distinct gr) = 1

-- *******************************103*****************************************

SELECT
 [1],[2],[3],[4],[5],[6]
 
 FROM (SELECT trip_no, row_number() over(order by trip_no) rn
	FROM  (select top(3) trip_no from trip order by trip_no
			 union
			 select top(3) trip_no from trip order by trip_no desc) a
	) x
 PIVOT
 (avg(trip_no)
 FOR rn
 IN([1],[2],[3],[4],[5],[6])
 ) pvt
 
 
 SELECT [avg_],
 [11],[12],[14],[15]
 FROM (SELECT 'average price' AS 'avg_', screen, price FROM Laptop) x
 PIVOT
 (AVG(price)
 FOR screen
 IN([11],[12],[14],[15])
 ) pvt
 
 
-- ******************************104**************************************
 
 with n(n) as(
select (a2 * 10  + a1 + 1) as n
			from 
					(select 0 as a1 
					union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9
					) a1
				cross join
					(select 0 as a2 
					union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9
					) a2
)

select c.class, 'bc-' + cast(n1.n as varchar(2)) n
from classes c cross apply (select top(c.numGuns) n from n) n1
where c.type = 'bc'
and c.numGuns IS NOT NULL
 
-- *****************************105************************************
select 
	maker, model, a, b, min(a) over(partition by maker) v, 
	max(a) over(partition by maker) g
from
	(select 
		maker, model, row_number() over(order by maker, model) a, 
		dense_rank() over(order by maker) b
	from
	product) x

	
-- *****************************106************************************


with a(b_datetime, b_q_id, b_v_id, b_vol, v) as (
SELECT b_datetime, b_q_id, b_v_id, cast(B_VOL as float) b_vol,
	row_number() over(order by b_datetime, b_q_id, b_v_id) v
from utB
)

select b_datetime, b_q_id, b_v_id, cast(b_vol as int),
	cast((select
	exp(sum(
		case when v%2 = 0 
			then -log(b_vol)
		else log(b_vol)
		end
	))
	from a a1
	where a1.v <= a.v
	) as '.00000000') as pi
from a a

-- *****************************107************************************
select name, trip_no, date
from
	(select c.name, t.trip_no, pit.date , row_number() over(order by pit.date, t.time_out) n
	from
	pass_in_trip pit join trip t on pit.trip_no = t.trip_no
	join company c on c.id_comp = t.id_comp
	where t.town_from = 'Rostov' and datepart(yy, pit.date) = 2003 and datepart(mm, pit.date) = 4) x
where n = 5

-- *****************************108************************************

with abc(a,b,c) as(
	select cast(a.b_vol as float) a, cast(b.b_vol as float) b, cast(c.b_vol as float) c
	from 
		utB a 
		join 
			utB b on a.b_vol<b.b_vol
		join 
			utB c on b.b_vol<c.b_vol
)

	select distinct cast(a as int) a, cast(b as int) b, cast(c as int) c
	from abc
	where (a+b)>c and  c <= sqrt(b*b+a*a)

-- *****************************109***********************************

with w(qid) as (
	select distinct b_q_id
	from utB
	group by b_q_id
	having sum(b_vol) = 255*3
),
b(qid) as (
	select q_id from utQ
	except
	select b_q_id from utB
)

select q.q_name, w.w, b.b
from
(select qid from w
union 
select qid from b) x
join utQ q on x.qid = q.q_id
cross join 
(select count(*) w from w) w
cross join 
(select count(*) b from b) b

-- *****************************110***********************************

select p.name
from
(SELECT distinct pit.id_psg
from
pass_in_trip pit
join trip t on t.trip_no = pit.trip_no
where DATEPART(dw, pit.date) = 1
and t.time_in < t.time_out) i
join passenger p
on i.id_psg = p.id_psg

-- *****************************111***********************************

select q.q_name, r.b_vol
from
	(select b_q_id, sum(b_vol) b_vol
	from utB b join utV v on b.b_v_id = v.v_id
	where v.v_color = 'R'
	group by b.b_q_id) r
join 
	(select b_q_id, sum(b_vol) b_vol
	from utB b join utV v on b.b_v_id = v.v_id
	where v.v_color = 'G'
	group by b.b_q_id) g 
on r.b_q_id = g.b_q_id
join 
	(select b_q_id, sum(b_vol) b_vol
	from utB b join utV v on b.b_v_id = v.v_id
	where v.v_color = 'B'
	group by b.b_q_id) b
	on g.b_q_id = b.b_q_id
	join utQ q on q.q_id = b.b_q_id
where 
	r.b_vol = g.b_vol and g.b_vol = b.b_vol
	and
	r.b_vol IS NOT NULL and r.b_vol < 255
	
-- *****************************112***********************************

select min(x.num)
from
(select distinct v_color from utV) vc
cross apply 
(select ((select count(v_id) from utV where v_color = vc.v_color)*255 - sum(b.b_vol)) / 255 as num
		from 
		utB b join utV v on b.b_v_id = v.v_id
		where v.v_color = vc.v_color) x

