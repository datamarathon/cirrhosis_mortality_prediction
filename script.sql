drop table if exists tmp_first_wbc;
create table tmp_first_wbc as
select icustay_id, charttime, value1num 
from (
	select ce.icustay_id, ce.charttime, ce.value1num,
		row_number() over(partition by ce.icustay_id order by ce.charttime) as rang
	from mimic2v26.chartevents ce
	join mimic2v26.icustay_detail id on 
		ce.icustay_id = id.icustay_id and 
		ce.charttime < (id.icustay_intime + INTERVAL '24 HOUR')
	where itemid in (
		1542,
		1127,
		861,
		4200
	)
	and ce.icustay_id in (select * from tmp_chirr_cohort)
) v where rang = 1;

-- drop table if exists tmp_first_bili;
-- create table tmp_first_bili as
-- select icustay_id, charttime, value1num 
-- from (
-- 	select ce.icustay_id, ce.charttime, ce.value1num,
-- 		row_number() over(partition by ce.icustay_id order by ce.charttime) as rang
-- 	from mimic2v26.chartevents ce
-- 	join mimic2v26.icustay_detail id on 
-- 		ce.icustay_id = id.icustay_id and 
-- 		ce.charttime < (id.icustay_intime + INTERVAL '24 HOUR')
-- 	where itemid in (
-- 		848,
-- 		1538
-- 	)
-- 	and ce.icustay_id in (select * from tmp_chirr_cohort)
-- ) v where rang = 1;

drop table if exists tmp_first_bili;
create table tmp_first_bili as
select *
from (
	select bili.icustay_id, bili.valuenum, bili.charttime, 
		row_number() over(partition by bili.icustay_id order by bili.charttime) as rang
	from mimic2v26.labevents bili
	join mimic2v26.icustay_detail id on 
		bili.icustay_id = id.icustay_id and 
		bili.charttime < (id.icustay_intime + INTERVAL '24 HOUR') 
	where bili.icustay_id in (select * from tmp_chirr_cohort) 
	and bili.itemid = 50170
) v where rang = 1;

drop table if exists tmp_first_sodium;
create table tmp_first_sodium as
select icustay_id, charttime, value1num 
from (
	select ce.icustay_id, ce.charttime, ce.value1num,
		row_number() over(partition by ce.icustay_id order by ce.charttime) as rang
	from mimic2v26.chartevents ce
	join mimic2v26.icustay_detail id on 
		ce.icustay_id = id.icustay_id and 
		ce.charttime < (id.icustay_intime + INTERVAL '24 HOUR')
	where itemid in (
		837,
		1536,
		3803
	)
	and ce.icustay_id in (select * from tmp_chirr_cohort)
) v where rang = 1;

drop table if exists tmp_first_inr;
create table tmp_first_inr as
select *
from (
	select inr.icustay_id, inr.valuenum, inr.charttime, 
		row_number() over(partition by inr.icustay_id order by inr.charttime) as rang
	from mimic2v26.labevents inr
	join mimic2v26.icustay_detail id on 
		inr.icustay_id = id.icustay_id and 
		inr.charttime < (id.icustay_intime + INTERVAL '24 HOUR') 
	where inr.icustay_id in (select * from tmp_chirr_cohort) 
	and inr.itemid = 50399
) v where rang = 1;


drop table if exists tmp_first_creat;
create table tmp_first_creat as
select *
from (
	select creat.icustay_id, creat.valuenum, creat.charttime, 
		row_number() over(partition by creat.icustay_id order by creat.charttime) as rang
	from mimic2v26.labevents creat
	join mimic2v26.icustay_detail id on 
		creat.icustay_id = id.icustay_id and 
		creat.charttime < (id.icustay_intime + INTERVAL '24 HOUR') 
	where creat.icustay_id in (select * from tmp_chirr_cohort) 
	and creat.itemid = 50090
) v where rang = 1;



drop table if exists tmp_cirrh_meld;
create table tmp_cirrh_meld as
select a.icustay_id, creattime as meldtime,
	case when (0.957 * ln(creatval) + 0.378 * ln(bilival) + 1.120 * ln(inrval) + 0.643 ) * 10 < 1 then 1
	else (0.957 * ln(creatval) + 0.378 * ln(bilival) + 1.120 * ln(inrval) + 0.643 ) * 10 end as meldscore
from (
	select  creat.icustay_id,
		creat.charttime as creattime, creat.valuenum as creatval, creat.valueuom as creattype,
		bili.charttime as bilitime, bili.valuenum as bilival, bili.valueuom as vilitype, 
		inr.charttime as inrtime, inr.valuenum as inrval, inr.valueuom as inrtype,
		rank() over(partition by creat.icustay_id, creat.charttime order by 
			(case when (creat.charttime - bili.charttime) < INTERVAL '0' THEN
				-(creat.charttime - bili.charttime)
			ELSE (creat.charttime - bili.charttime) END),
			(case when (creat.charttime - inr.charttime) < INTERVAL '0' THEN
				-(creat.charttime - inr.charttime)
			ELSE (creat.charttime - inr.charttime) END)
		) as rang
	from mimic2v26.labevents creat
	join mimic2v26.labevents bili on 
		bili.icustay_id = creat.icustay_id and
		bili.charttime >= (creat.charttime - INTERVAL '24 HOURS') and
		bili.charttime <= (creat.charttime + INTERVAL '24 HOURS') and
		bili.itemid = 50170
	join mimic2v26.labevents inr on 
		inr.icustay_id = creat.icustay_id and
		inr.charttime >= (creat.charttime - INTERVAL '24 HOURS') and
		inr.charttime <= (creat.charttime + INTERVAL '24 HOURS') and
		inr.itemid = 50399
	where creat.itemid=50090
	and creat.icustay_id in (select * from tmp_chirr_cohort)
) a
where rang = 1
order by icustay_id, meldtime;

drop table if exists tmp_first_meld;
create table tmp_first_meld as
select * 
from (
	select meld.*, row_number() over(partition by meld.icustay_id order by meldtime) as rang
	from tmp_cirrh_meld meld
	join mimic2v26.icustay_detail id on 
		meld.icustay_id = id.icustay_id and 
		meld.meldtime < (id.icustay_intime + INTERVAL '24 HOUR') 
) v 
where v.rang = 1;

select *
from mimic2v26.d_chartitems ci
where lower(ci.label) like '%sofa%'

drop table if exists tmp_chirr_dayvalues;
create table tmp_chirr_dayvalues as 
select icustay_id,
respsofa1,
hepsofa1,
hemasofa1,
cardpressofa1,
cardmapsofa1,
neursofa1,
renalsofa1,
totalsofa1,
respsofa2,
hepsofa2,
hemasofa2,
cardpressofa2,
cardmapsofa2,
neursofa2,
renalsofa2,
totalsofa2,
respsofa3,
hepsofa3,
hemasofa3,
cardpressofa3,
cardmapsofa3,
neursofa3,
renalsofa3,
totalsofa3,
totalsofa2-totalsofa1 as sofadelta2_1,
totalsofa3-totalsofa2 as sofadelta3_2,
totalsofa3-totalsofa1 as sofadelta3_1

from (
	select v.icustay_id,
			max(respsofa1) as respsofa1,
			max(hepsofa1) as hepsofa1,
			max(hemasofa1) as hemasofa1,
			max(cardpressofa1) as cardpressofa1,
			max(cardmapsofa1) as cardmapsofa1,
			max(neursofa1) as neursofa1,
			max(renalsofa1) as renalsofa1,
			max(totalsofa1) as totalsofa1,
			max(respsofa2) as respsofa2,
			max(hepsofa2) as hepsofa2,
			max(hemasofa2) as hemasofa2,
			max(cardpressofa2) as cardpressofa2,
			max(cardmapsofa2) as cardmapsofa2,
			max(neursofa2) as neursofa2,
			max(renalsofa2) as renalsofa2,
			max(totalsofa2) as totalsofa2,
			max(respsofa3) as respsofa3,
			max(hepsofa3) as hepsofa3,
			max(hemasofa3) as hemasofa3,
			max(cardpressofa3) as cardpressofa3,
			max(cardmapsofa3) as cardmapsofa3,
			max(neursofa3) as neursofa3,
			max(renalsofa3) as renalsofa3,
			max(totalsofa3) as totalsofa3
	from (
		select iday.icustay_id, 
			case when iday.seq = 1 and ce.itemid = 20002 then ce.value1num end as respsofa1,
			case when iday.seq = 1 and ce.itemid = 20003 then ce.value1num end as hepsofa1,
			case when iday.seq = 1 and ce.itemid = 20004 then ce.value1num end as hemasofa1,
			case when iday.seq = 1 and ce.itemid = 20005 then ce.value1num end as cardpressofa1,
			case when iday.seq = 1 and ce.itemid = 20006 then ce.value1num end as cardmapsofa1,
			case when iday.seq = 1 and ce.itemid = 20007 then ce.value1num end as neursofa1,
			case when iday.seq = 1 and ce.itemid = 20008 then ce.value1num end as renalsofa1,
			case when iday.seq = 1 and ce.itemid = 20009 then ce.value1num end as totalsofa1,
			case when iday.seq = 2 and ce.itemid = 20002 then ce.value1num end as respsofa2,
			case when iday.seq = 2 and ce.itemid = 20003 then ce.value1num end as hepsofa2,
			case when iday.seq = 2 and ce.itemid = 20004 then ce.value1num end as hemasofa2,
			case when iday.seq = 2 and ce.itemid = 20005 then ce.value1num end as cardpressofa2,
			case when iday.seq = 2 and ce.itemid = 20006 then ce.value1num end as cardmapsofa2,
			case when iday.seq = 2 and ce.itemid = 20007 then ce.value1num end as neursofa2,
			case when iday.seq = 2 and ce.itemid = 20008 then ce.value1num end as renalsofa2,
			case when iday.seq = 2 and ce.itemid = 20009 then ce.value1num end as totalsofa2,
			case when iday.seq = 3 and ce.itemid = 20002 then ce.value1num end as respsofa3,
			case when iday.seq = 3 and ce.itemid = 20003 then ce.value1num end as hepsofa3,
			case when iday.seq = 3 and ce.itemid = 20004 then ce.value1num end as hemasofa3,
			case when iday.seq = 3 and ce.itemid = 20005 then ce.value1num end as cardpressofa3,
			case when iday.seq = 3 and ce.itemid = 20006 then ce.value1num end as cardmapsofa3,
			case when iday.seq = 3 and ce.itemid = 20007 then ce.value1num end as neursofa3,
			case when iday.seq = 3 and ce.itemid = 20008 then ce.value1num end as renalsofa3,
			case when iday.seq = 3 and ce.itemid = 20009 then ce.value1num end as totalsofa3
		from mimic2v26.icustay_days iday
		join mimic2v26.chartevents ce on 
			ce.icustay_id = iday.icustay_id and
			ce.itemid between 20002 and 20009 and
			ce.charttime between iday.begintime and iday.endtime
		where iday.icustay_id in (select * from tmp_chirr_cohort)
		and iday.seq in (1,2,3)
	) v
	group by v.icustay_id
) f;

select *
from mimic2v26.d_chartitems ci where lower(label) like '%peep%'

drop table if exists tmp_chirr_vent;
create table tmp_chirr_vent as 
select v.icustay_id,
	max(ventday1) as ventday1,
	max(ventday2) as ventday2,
	max(ventday3) as ventday3
from (
	select iday.icustay_id,
		case when iday.seq=1 then 1 else 0 end as ventday1,
		case when iday.seq=2 then 1 else 0 end as ventday2,
		case when iday.seq=3 then 1 else 0 end as ventday3
	from mimic2v26.chartevents ce
	join mimic2v26.icustay_days iday on 
		ce.icustay_id = iday.icustay_id and 
		seq in (1,2,3) and
		ce.charttime between iday.begintime and iday.endtime
	where ce.itemid in (505,506)
	and ce.value1num > 0
	and ce.icustay_id in (select * from tmp_chirr_cohort)
) v
group by v.icustay_id;

select * from tmp_chirr_vent

select 
case when id.gender = 'F' then 1 else 0 end as gender,
id.icustay_admit_age as age,
id.sapsi_first as saps1,
id.sofa_first as sofa1, 
inr.valuenum as inr1,
na.value1num as na1,
bili.valuenum as bili1,
creat.valuenum as creat1,
meld.meldscore as meld,
wbc.value1num as wbc,
case when respsofa1 is null then 0 else  respsofa1 end as  respsofa1,
case when hepsofa1 is null then 0 else  hepsofa1 end as  hepsofa1,
case when hemasofa1 is null then 0 else  hemasofa1 end as  hemasofa1,
case when cardpressofa1 is null then 0 else  cardpressofa1 end as  cardpressofa1,
case when cardmapsofa1 is null then 0 else  cardmapsofa1 end as  cardmapsofa1,
case when neursofa1 is null then 0 else  neursofa1 end as  neursofa1,
case when renalsofa1 is null then 0 else  renalsofa1 end as  renalsofa1,
case when totalsofa1 is null then 0 else  totalsofa1 end as  totalsofa1,
case when respsofa2 is null then 0 else  respsofa2 end as  respsofa2,
case when hepsofa2 is null then 0 else  hepsofa2 end as  hepsofa2,
case when hemasofa2 is null then 0 else  hemasofa2 end as  hemasofa2,
case when cardpressofa2 is null then 0 else  cardpressofa2 end as  cardpressofa2,
case when cardmapsofa2 is null then 0 else  cardmapsofa2 end as  cardmapsofa2,
case when neursofa2 is null then 0 else  neursofa2 end as  neursofa2,
case when renalsofa2 is null then 0 else  renalsofa2 end as  renalsofa2,
case when totalsofa2 is null then 0 else  totalsofa2 end as  totalsofa2,
case when respsofa3 is null then 0 else  respsofa3 end as  respsofa3,
case when hepsofa3 is null then 0 else  hepsofa3 end as  hepsofa3,
case when hemasofa3 is null then 0 else  hemasofa3 end as  hemasofa3,
case when cardpressofa3 is null then 0 else  cardpressofa3 end as  cardpressofa3,
case when cardmapsofa3 is null then 0 else  cardmapsofa3 end as  cardmapsofa3,
case when neursofa3 is null then 0 else  neursofa3 end as  neursofa3,
case when renalsofa3 is null then 0 else  renalsofa3 end as  renalsofa3,
case when totalsofa3 is null then 0 else  totalsofa3 end as  totalsofa3,
case when sofadelta2_1 is null then 0 else  sofadelta2_1 end as  sofadelta2_1,
case when sofadelta3_2 is null then 0 else  sofadelta3_2 end as  sofadelta3_2,
case when sofadelta3_1 is null then 0 else  sofadelta3_1 end as  sofadelta3_1,
case when vent.ventday1 is null then 0 else vent.ventday1 end as ventday1,
case when vent.ventday2 is null then 0 else vent.ventday2 end as ventday2,
case when vent.ventday3 is null then 0 else vent.ventday3 end as ventday3,
case when id.icustay_expire_flg = 'Y' then 1 else 0 end as died
from mimic2v26.icustay_detail id
join tmp_first_inr inr on inr.icustay_id = id.icustay_id
join tmp_first_sodium na on na.icustay_id = id.icustay_id
join tmp_first_meld meld on meld.icustay_id = id.icustay_id
join tmp_first_wbc wbc on wbc.icustay_id = id.icustay_id
join tmp_first_bili bili on bili.icustay_id = id.icustay_id
join tmp_first_creat creat on creat.icustay_id = id.icustay_id
join tmp_chirr_dayvalues dv on dv.icustay_id = id.icustay_id
left join tmp_chirr_vent vent on vent.icustay_id = id.icustay_id
where id.icustay_id in (
	select *
	from tmp_chirr_cohort
)
and id.sapsi_first is not null
and id.sofa_first is not null


















select distinct id.icustay_id
from tmp_cirrh_meld meld
join mimic2v26.icustay_detail id on 
	id.icustay_id = meld.icustay_id and
	meld.meldtime < (id.icustay_intime + INTERVAL '48 hours') 

	and
	id.icustay_expire_flg = 'Y'

select distinct id.icustay_id
from tmp_cirrh_meld meld
join mimic2v26.icustay_detail id on id.icustay_id = meld.icustay_id
where id.sofa_first is null

select distinct id.icustay_id,
from mimic2v26.chartevents ev 
join mimic2v26.icustay_detail id on id.icustay_id = ev.icustay_id
where ev.icustay_id in (
	select *
	from tmp_chirr_cohort
)
and ev.itemid = 20009
and ev.charttime < (id.icustay_intime + INTERVAL '24 HOURS')



select distinct id.icustay_id
from mimic2v26.labevents ev 
join mimic2v26.icustay_detail id on id.icustay_id = ev.icustay_id
where ev.icustay_id in (
	select *
	from tmp_chirr_cohort
)
and ev.itemid = 50060
and ev.charttime < (id.icustay_intime + INTERVAL '24 HOURS')

--sodium 50159


select * from mimic2v26.d_labitems where lower(test_name) like '%wbc%' 

select * 
from mimic2v26.chartevents where itemid in (
1542,
1127,
861,
4200
)
LIMIT 100

select * 
from mimic2v26.d_chartitems 
where itemid in (
1542,
1127,
861,
4200
)


