drop table if exists tmp_chirr_cohort;
create table tmp_chirr_cohort as
select distinct id.icustay_id
from mimic2v26.icustay_detail id
join mimic2v26.poe_order io on 
	io.icustay_id = id.icustay_id and 
	lower(io.medication) like '%lactulose%' and
	io.frequency in (
	'5X/D',
	'BID',
	'Q1-2H:PRN',
	'Q1H',
	'Q1H:PRN',
	'Q2-3H',
	'Q2-3H:PRN',
	'Q2-4H',
	'Q2-4H:PRN',
	'Q2H',
	'Q2H:PRN',
	'Q3-6H:PRN',
	'Q3H',
	'Q3H:PRN',
	'Q4-6H',
	'Q4H',
	'Q4H:PRN',
	'Q6-8H',
	'Q6-8H:PRN',
	'Q6H',
	'Q6H:PRN',
	'Q8H',
	'Q 8H',
	'Q8H:PRN',
	'QAM',
	'QD',
	'QID',
	'QID:PRN',
	'TID',
	'TID:PRN'
	)
where icustay_seq = 1
and id.icustay_first_service IN ('MICU')
and id.icustay_los >= (72*60)
and lower(id.icustay_age_group) = 'adult';
