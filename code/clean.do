* Merge OPM status file with dynamic file
* Churn Ken Lee
* 28 February 2022

capture log close
clear all
set graphics off

global root_dir = "C:\Users\churn\Documents\UCSD\Projects\government_shutdown_ken"
global code_dir = "${root_dir}\code"
global raw_dir = "${root_dir}\rawdata"
global data_dir = "${root_dir}\data"
global output_dir = "${root_dir}\output"

global opm_status_dir = "${raw_dir}\opm-federal-employment-data\data\1973-09-to-2014-06\non-dod\status"
global opm_dynamic_dir = "${raw_dir}\opm-federal-employment-data\data\1973-09-to-2014-06\non-dod\dynamic"

local start_year = 1982
local end_year = 2014


********************************************************************************
* Process dates of government shutdowns
********************************************************************************
/*
* Process dates of government shutdowns
import delimited using "${raw_dir}\federal_government_shutdowns.csv", clear
generate quarter = .
replace quarter = 3 if month <= 3
replace quarter = 6 if inrange(month, 4, 6)
replace quarter = 9 if inrange(month, 7, 9)
replace quarter = 12 if inrange(month, 10, 12)

keep year quarter
generate shutdown = 1

gduplicates drop year quarter, force // drop one of the 2 shutdowns that occured in q4 of 1995
save "${data_dir}\federal_government_shutdown_quarters.dta", replace

*/

********************************************************************************
* Import files
********************************************************************************
/*
* Import dynamic files
forvalues y = `start_year'(1)`end_year' {
	forvalues q = 1(1)4 {
		capture noisily {
			clear
			if `q' == 1 {
				local m MAR
			}
			else if `q' == 2 {
				local m JUN
			}
			else if `q' == 3 {
				local m SEP
			}
			else if `q' == 4 {
				local m DEC
			}
			infile using "${code_dir}/opm_dynamic_dict.dct", using("${opm_dynamic_dir}\\`m'`y'.NONDOD.FO05M3.txt")

			destring adj_basic_pay, replace force
			keep id agency accession_separation_ind effective_date age los_level occ occ_cat adj_basic_pay appt_type
			
			* Generate consistent date variable
			generate year = substr(effective_date, 1, 4)
			generate month = substr(effective_date, 5, 2)
			destring year, replace force
			destring month, replace force
			generate quarter = .
			replace quarter = 3 if month <= 3
			replace quarter = 6 if inrange(month, 4, 6)
			replace quarter = 9 if inrange(month, 7, 9)
			replace quarter = 12 if inrange(month, 10, 12)
			drop month
			
			scalar define q_since_1961q1 = ((`y'-1960)*4) - 1 + `q'
			local quart = q_since_1961q1
			
			save "${data_dir}/dynamic_q`quart'.dta", replace
		}
	}
}


* Import status files
forvalues y = `start_year'(1)`end_year'  {
	forvalues q = 1(1)4 {
		capture noisily {
			clear
			if `q' == 1 {
				local m 03
			}
			else if `q' == 2 {
				local m 06
			}
			else if `q' == 3 {
				local m 09
			}
			else if `q' == 4 {
				local m 12
			}
			infile using "${code_dir}/opm_status_dict.dct", using("${opm_status_dir}\Status_Non_DoD_`y'_`m'.txt")
			
			destring adj_basic_pay, replace force
			keep id age_range agency file_date age_range education los_level occ occ_cat adj_basic_pay appt_type
			
			* Generate consistent date variable
			generate year = substr(file_date, 1, 4)
			generate quarter = substr(file_date, 5, 2)
			destring year, replace force
			destring quarter, replace force
			
			scalar define q_since_1961q1 = ((`y'-1960)*4) - 1 + `q'
			local quart = q_since_1961q1
			
			save "${data_dir}/status_q`quart'.dta", replace
		}
	}
}

* Combine status files
clear
tempfile status_combined
save `status_combined', emptyok
forvalues y = `start_year'(1)`end_year' {
	forvalues q = 1(1)4 {
		capture noisily {
			scalar define q_since_1961q1 = ((`y'-1960)*4) - 1 + `q'
			local quart = q_since_1961q1
			
			append using "${data_dir}/status_q`quart'.dta"
		}
	}
}
save "${data_dir}\status_combined.dta", replace

*/

********************************************************************************
* Merge dynamics and status files to get education level of leavers
********************************************************************************

* Merge status files to dynamic files to obtain education level of separators
forvalues y = `start_year'(1)`end_year' {
	forvalues q = 1(1)4 {
		scalar define q_since_1961q1 = ((`y'-1960)*4) - 1 + `q'
		local quart = q_since_1961q1
		if `quart' < 218 {
			use "${data_dir}/status_q`quart'.dta", clear
	
			local q1 = `quart'-1
			capture noisily append using "${data_dir}/status_q`q1'.dta"
			local q1 = `quart'-2
			capture noisily append using "${data_dir}/status_q`q1'.dta"
			local q1 = `quart'-3
			capture noisily append using "${data_dir}/status_q`q1'.dta"
			local q1 = `quart'+1
			capture noisily append using "${data_dir}/status_q`q1'.dta"
			local q1 = `quart'+2
			capture noisily append using "${data_dir}/status_q`q1'.dta"
			
			keep id agency education

			gduplicates drop id agency, force

			tempfile status
			save `status'
				
			use "${data_dir}/dynamic_q`quart'.dta", clear
			merge m:1 id agency using `status'
			drop if _merge == 2
			drop _merge
			save "${data_dir}/dynamic_merged_q`quart'.dta", replace
		}
		else {
			continue
		}
		
	}
}

* Combine merged dynamic files
clear
tempfile merged_combined
save `merged_combined', emptyok
forvalues y = `start_year'(1)`end_year' {
	forvalues q = 1(1)4 {
		scalar define q_since_1961q1 = ((`y'-1960)*4) - 1 + `q'
		local quart = q_since_1961q1
		
		capture noisily append using "${data_dir}/dynamic_merged_q`quart'.dta"
	}
}

save "${data_dir}/dynamic_merged_combined.dta", replace

*/
********************************************************************************
* Calculate aggregate numbers within status files
********************************************************************************

* Calculate n employees
use "${data_dir}\status_combined.dta", clear
generate emp = 1 // indicator for summing over in egen
bysort year quarter: gegen n_all = total(emp)

* Calculate median pay within each occupation for each quarter and export
bysort year quarter occ: gegen med_adj_basic_pay = median(adj_basic_pay)

preserve
keep year quarter occ med_adj_basic_pay
gduplicates drop year quarter occ, force
drop if (occ == "" | occ == "****") // missing or redacted occ
save "${output_dir}/median_adj_basic_pay_by_occ.dta", replace
restore

* Calculate n employees above and below median pay
generate above_med_pay = .
replace above_med_pay = 1 if (adj_basic_pay > med_adj_basic_pay) & (adj_basic_pay != .) & (occ != "" | occ != "****")
replace above_med_pay = 0 if (adj_basic_pay <= med_adj_basic_pay) & (occ != "" | occ != "****")
bysort year quarter: gegen n_above_med_pay = total(emp) if above_med_pay == 1
bysort year quarter: gegen n_below_med_pay = total(emp) if above_med_pay == 0

* Calculate n employees by education
generate educ = education
replace educ = "98" if education == "**"
destring educ, replace
generate post_ugrad = 0
replace post_ugrad = 1 if inrange(educ, 0, 12)
replace post_ugrad = 2 if inrange(educ, 13, 97)
label variable post_ugrad "0 = missing, 1 = <ugrad, 2 = >ugrad"

bysort year quarter: gegen n_post_ugrad = total(emp) if post_ugrad == 2
bysort year quarter: gegen n_less_ugrad = total(emp) if post_ugrad == 1
bysort year quarter: gegen n_miss_educ = total(emp) if post_ugrad == 0

* Collapse
gcollapse (firstnm) n_*, by(year quarter)

save "${data_dir}/status_combined_collapsed.dta", replace

*/
********************************************************************************
* Calculate aggregate separation/accession numbers within dynamic files
********************************************************************************

* Calculate accession and separation numbers by subgroups within dynamic data
use "${data_dir}\dynamic_merged_combined.dta", clear

* Accession/separation indicators
generate accession_separation_type = substr(accession_separation_ind, 1, 1)
generate accession_separation_reason = substr(accession_separation_ind, 2, 1)
generate accession_ind = 0
replace accession_ind = 1 if accession_separation_type == "A"
generate separation_ind = 0
replace separation_ind = 1 if accession_separation_type == "S"

* Generate total counts
bysort year quarter: gegen n_seps_all = total(separation_ind)
bysort year quarter: gegen n_acce_all = total(accession_ind)

* Calculate n employees above and below median pay
merge m:1 year quarter occ using "${output_dir}/median_adj_basic_pay_by_occ.dta"
generate above_med_pay = .
replace above_med_pay = 1 if (adj_basic_pay > med_adj_basic_pay) & (adj_basic_pay != .)
replace above_med_pay = 0 if (adj_basic_pay <= med_adj_basic_pay)

bysort year quarter: gegen n_seps_above_med_pay = total(separation_ind) if above_med_pay == 1
bysort year quarter: gegen n_seps_below_med_pay = total(separation_ind) if above_med_pay == 0
bysort year quarter: gegen n_acce_above_med_pay = total(accession_ind) if above_med_pay == 1
bysort year quarter: gegen n_acce_below_med_pay = total(accession_ind) if above_med_pay == 0

* Education indicators (by ugrad completion)
generate educ = education
replace educ = "98" if education == "**"
destring educ, replace
generate post_ugrad = 0
replace post_ugrad = 1 if inrange(educ, 0, 12)
replace post_ugrad = 2 if inrange(educ, 13, 97)
label variable post_ugrad "0 = missing, 1 = <ugrad, 2 = >ugrad"

* Generate counts within each education subgroup
bysort year quarter: gegen n_seps_post_ugrad = total(separation_ind) if post_ugrad == 2
bysort year quarter: gegen n_acce_post_ugrad = total(accession_ind) if post_ugrad == 2
bysort year quarter: gegen n_seps_less_ugrad = total(separation_ind) if post_ugrad == 1
bysort year quarter: gegen n_acce_less_ugrad = total(accession_ind) if post_ugrad == 1

* Collapse across agencies
gcollapse (firstnm) n_seps* n_acce*, by(year quarter)

save "${data_dir}/dynamic_merged_combined_collapsed.dta", replace

*/
********************************************************************************
* Merge aggregate status and dynamic numbers to calculate rates
********************************************************************************

* Merge status and dynamic aggregate numbers
use "${data_dir}/status_combined_collapsed.dta", clear
merge 1:1 year quarter using "${data_dir}/dynamic_merged_combined_collapsed.dta", nogenerate


/*
* Calculate separation rates for each subgroup
local group_list "all post_ugrad less_ugrad above_med_pay below_med_pay"
foreach g of local group_list {
	generate sep_rate_`g' = n_seps_`g'/n_`g'
	generate acce_rate_`g' = n_acce_`g'/n_`g'
}

* Control for seasonal and trend effects
generate qdate = qofd(mdy(quarter, 1, year))

foreach g of local group_list {
	regress sep_rate_`g' qdate i.quarter, nocons
	predict sep_rate_`g'_adj, resid
	drop sep_rate_`g'
	regress acce_rate_`g' qdate i.quarter, nocons
	predict acce_rate_`g'_adj, resid
	drop acce_rate_`g'
}

* Calculate confidence intervals using proportion
preserve
keep qdate n_acce_all n_seps_all n_all
generate separation_count = n_seps
rename n_acce_all n1
rename n_seps_all n2
rename n_all n3
reshape long n, i(qdate) j(type)

generate accession = 0
replace accession = 1 if type == 1
generate separation = 0
replace separation = 1 if type == 2
replace n = n - separation_count if type == 3

proportion separation [fweight = n] if type != 1, over(qdate)
parmest, saving("${output_dir}/CI_sep_rate.dta", replace)
restore

* Calculate CI bands
frame create ci_frame
frame change ci_frame
use "${output_dir}/CI_sep_rate.dta", clear
split parm, parse("." "@")
destring parm1, generate(type)
destring parm3, generate(qdate)
drop if type == 0
generate sep_rate_all_CI_band = max95 - min95
keep qdate sep_rate_all_CI_band

save "${output_dir}/CI_sep_rate.dta", replace

* Plot confidence intervals
frame change default
merge 1:1 qdate using "${output_dir}/CI_sep_rate.dta", nogenerate
generate sep_rate_all_adj_max95 = sep_rate_all_adj + sep_rate_all_CI_band/2
generate sep_rate_all_adj_min95 = sep_rate_all_adj - sep_rate_all_CI_band/2
drop sep_rate_all_CI_band

format qdate %tq
tsset qdate
sort qdate
eclplot sep_rate_all_adj sep_rate_all_adj_min95 sep_rate_all_adj_max95 qdate
graph export "${output_dir}/separation_rate_CI.png", replace
drop sep_rate_all_adj_min95 sep_rate_all_adj_max95

* Produce plots
reshape n_ long sep_rate acce_rate, i(qdate) j(type) string

keep year quarter qdate type sep_rate acce_rate
gsort type qdate

merge m:1 year quarter using "${data_dir}\federal_government_shutdown_quarters.dta", nogenerate

drop if qdate == . // shutdowns outside of available sample
replace shutdown = 0 if shutdown == .

generate cut_type = .
replace cut_type = 10 if type == "_less_ugrad_adj"
replace cut_type = 11 if type == "_post_ugrad_adj"
replace cut_type = 20 if type == "_below_med_pay_adj"
replace cut_type = 21 if type == "_above_med_pay_adj"


generate treated_group_dummy = 0
replace treated_group_dummy = 1 if cut_type == 11
regress sep_rate i.treated_group_dummy i.shutdown i.treated_group_dummy#i.shutdown











