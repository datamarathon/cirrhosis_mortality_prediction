-- TITLE: MELD-score in MIMIC
-- DESCRIPTION: Calculates the MELD score based on time points for creatinine values. The closest values to this creatinine time point for bilirubine and inr are then looked up in a time span of 12 hours prior or after the event.
-- AUTHOR: Bram Gadeyne (bram.gadeyne@intec.ugent.be)
-- NOTES: Please notify me if you find any errors in this script.

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
		bili.charttime >= (creat.charttime - INTERVAL '12 HOURS') and
		bili.charttime <= (creat.charttime + INTERVAL '12 HOURS') and
		bili.itemid = 50170
	join mimic2v26.labevents inr on 
		inr.icustay_id = creat.icustay_id and
		inr.charttime >= (creat.charttime - INTERVAL '12 HOURS') and
		inr.charttime <= (creat.charttime + INTERVAL '12 HOURS') and
		inr.itemid = 50399
	where creat.itemid=50090
) a
where rang = 1
order by icustay_id, meldtime
