select distinct id.icustay_id
from mimic2v26.icustay_detail id
join mimic2v26.icd9 cirrh on cirrh.hadm_id = id.hadm_id and cirrh.code in ('571.2','571.5','571.6')
join mimic2v26.poe_order io on io.icustay_id = id.icustay_id and lower(io.medication) like '%lactulose%'
and icustay_seq = 1
and id.icustay_first_service IN ('MICU')
and id.icustay_los >= (72*60)
and lower(id.icustay_age_group) = 'adult'
