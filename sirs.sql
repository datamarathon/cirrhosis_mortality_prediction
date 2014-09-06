-- TITLE: SIRS (and SIRS score) in MIMIC
-- DESCRIPTION: Checks wether or not a patient has SIRS. Therefore some temporary tables are created. Since Heart rate seems to have the highest frequency of registrations we're using the time points for Heart rate values to select the closes values for WBC (24 hour window) and Respiratory Rate, Temperature and Arterial PaCO2 (8 hour window)
-- We add a score to the calculated SIRS indication to note if it was based in 2, 3 or 4 of the conditions and assign it a value 1, 2 of 3.
-- AUTHOR: Bram Gadeyne (bram.gadeyne@intec.ugent.be)
-- NOTES: Please notify me if you find any errors in this script.

create table tmp_sirs_part1 as
select icustay_id,hrtime, hrvalue, hrunit,wbctime, wbcvalue, wbcunit,wbclabel
from (
	select hr.icustay_id,hr.charttime as hrtime, hr.value1num as hrvalue, hr.value1uom as hrunit,
		wbc.charttime as wbctime, wbc.value1num as wbcvalue, wbc.value1uom as wbcunit,wbci.label as wbclabel, 
		rank() over(partition by hr.icustay_id,hr.charttime 
			order by (CASE WHEN ((hr.charttime - wbc.charttime) < INTERVAL '0') THEN (-(hr.charttime - wbc.charttime)) ELSE (hr.charttime - wbc.charttime) END),wbci.label) as rang
	from mimic2v26.d_chartitems hri
	join mimic2v26.chartevents hr on 
		hr.itemid = hri.itemid and 
		hr.value1num > 90
	left join mimic2v26.d_chartitems wbci on 
		wbci.label in ('WBC (4-11,000)','WBC   (4-11,000)','WBC 4.0-11.0','WhiteBloodC 4.0-11.0','WBC')
	left join mimic2v26.chartevents wbc on 
		wbc.itemid = wbci.itemid and
		wbc.icustay_id = hr.icustay_id and 
		wbc.charttime >= (hr.charttime - INTERVAL '12 hours') and 
		wbc.charttime <= (hr.charttime + INTERVAL '12 hours')
	where hri.label = 'Heart Rate'
) k
where k.rang = 1;

create table tmp_sirs_part2 as 
select icustay_id,hrtime, hrvalue, hrunit,wbctime, wbcvalue, wbcunit,wbclabel,
	rrtime, respvalue, respunit,rrlabel
from (
	select l.*,rev.charttime as rrtime, rev.value1num as respvalue, rev.value1uom as respunit,rri.label as rrlabel,
		rank() over(partition by l.icustay_id,l.hrtime 
			order by (CASE WHEN ((l.hrtime - rev.charttime) < INTERVAL '0') THEN (-(l.hrtime - rev.charttime)) ELSE (l.hrtime - rev.charttime) END),rri.label) as rang
	from tmp_sirs_part1 l
	left join mimic2v26.d_chartitems rri on 
		rri.label in ('Respiratory Rate','Resp. Rate','Resp Rate')
	left join mimic2v26.chartevents rev on 
		rri.itemid = rev.itemid and
		rev.icustay_id = l.icustay_id and 
		rev.charttime >= (l.hrtime - INTERVAL '4 hours') and 
		rev.charttime <= (l.hrtime + INTERVAL '4 hours')
) i
where i.rang = 1;

create table tmp_sirs_part3 as
select icustay_id,hrtime, hrvalue, hrunit,wbctime, wbcvalue, wbcunit,wbclabel,
	rrtime, respvalue, respunit,rrlabel,
	temptime, tempvalue, tempunit
from (
	select j.*, 
		tev.charttime as temptime, tev.value1num as tempvalue, tev.value1uom as tempunit,
		rank() over(partition by j.icustay_id,j.hrtime 
			order by (CASE WHEN ((j.hrtime - tev.charttime) < INTERVAL '0') THEN (-(j.hrtime - tev.charttime)) ELSE (j.hrtime - tev.charttime) END)) as rang 
	from tmp_sirs_part2 j
	left join mimic2v26.d_chartitems ti on 
		ti.label = 'Temperature C'
	left join mimic2v26.chartevents tev on 
		ti.itemid = tev.itemid and
		tev.icustay_id = j.icustay_id and 
		tev.charttime >= (j.hrtime - INTERVAL '4 hours') and 
		tev.charttime <= (j.hrtime + INTERVAL '4 hours')
) m
where m.rang = 1;

create table tmp_sirs_variables as
select icustay_id,hrtime, hrvalue, hrunit,wbctime, wbcvalue, wbcunit,wbclabel,
		rrtime, respvalue, respunit,rrlabel,
		temptime, tempvalue, tempunit,
		pftime, pfvalue, pfunit
from (
	select n.*,
		pfev.charttime as pftime, pfev.value1num as pfvalue, pfev.value1uom as pfunit,
		rank() over(partition by n.icustay_id,n.hrtime 
			order by (CASE WHEN ((n.hrtime - pfev.charttime) < INTERVAL '0') THEN (-(n.hrtime - pfev.charttime)) ELSE (n.hrtime - pfev.charttime) END)) as rang 
	from tmp_sirs_part3 n
	left join mimic2v26.d_chartitems pfi on 
		pfi.label in ('Arterial PaCO2')
	left join mimic2v26.chartevents pfev on 
		pfi.itemid = pfev.itemid and
		pfev.icustay_id = n.icustay_id and 
		pfev.charttime >= (n.hrtime - INTERVAL '12 hours') and 
		pfev.charttime <= (n.hrtime + INTERVAL '12 hours')
) o
where o.rang = 1;


drop table if exists tmp_sirs;
create table tmp_sirs as
select icustay_id,hrtime, hrvalue, hrunit,wbctime, wbcvalue, wbcunit,wbclabel,
	rrtime, respvalue, respunit,rrlabel,
	temptime, tempvalue, tempunit,
	case when (tempvalue > 38 or tempvalue < 36) then 1 else 0 end +
	case when (hrvalue > 90) then 1 else 0 end +
	case when (respvalue > 20 or pfvalue < 32) then 1 else 0 end +
	case when (wbcvalue < 4 or wbcvalue > 12) then 1 else 0 end as score
from tmp_sirs_variables
where 
(tempvalue > 38 and hrvalue > 90) or
(tempvalue < 36 and hrvalue > 90) or
(tempvalue > 38 and respvalue > 20) or
(tempvalue < 36 and respvalue > 20) or
(tempvalue > 38 and pfvalue < 32) or
(tempvalue < 36 and pfvalue < 32) or
(tempvalue > 38 and wbcvalue < 4) or
(tempvalue < 36 and wbcvalue < 4) or
(tempvalue > 38 and wbcvalue > 12) or
(tempvalue < 36 and wbcvalue > 12) or
(hrvalue > 90 and respvalue > 20) or
(hrvalue > 90 and pfvalue < 32) or
(hrvalue > 90 and wbcvalue < 4) or
(hrvalue > 90 and wbcvalue > 12) or
(respvalue > 20 and wbcvalue < 4) or
(respvalue > 20 and wbcvalue < 12) or
(pfvalue < 32 and wbcvalue < 4) or
(pfvalue < 32 and wbcvalue < 12);

drop table if exists tmp_sirs_part1;
drop table if exists tmp_sirs_part2;
drop table if exists tmp_sirs_part3;
drop table if exists tmp_sirs_variables;
