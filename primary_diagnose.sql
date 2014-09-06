-- TITLE: Primary diagnosis in MIMIC
-- DESCRIPTION: In the MIMIC FAQ it is noted that the primary diagnosis is the first ICD-9 code for the hospital admission.
-- AUTHOR: Bram Gadeyne (bram.gadeyne@intec.ugent.be)
-- NOTES: Please notify me if you find any errors in this script.

select subject_id, hadm_id, code, description
from (
	select subject_id, hadm_id, code, description,
	row_number() over(partition by subject_id,hadm_id) as rn
	from mimic2v26.icd9
) v 
where rn=1
