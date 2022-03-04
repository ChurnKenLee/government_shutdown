* Merge OPM status file with dynamic file
* Churn Ken Lee
* 28 February 2022

capture log close
clear all
set graphics off

global root_dir = "C:\Users\churn\Documents\UCSD\Projects\government_shutdown"
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
replace quarter = 1 if month <= 3
replace quarter = 2 if inrange(month, 4, 6)
replace quarter = 3 if inrange(month, 7, 9)
replace quarter = 4 if inrange(month, 10, 12)

keep year quarter
generate shutdown = 1

gduplicates drop year quarter, force // drop one of the 2 shutdowns that occured in q4 of 1995

tostring year, replace
tostring quarter, replace
generate qdate_str = year + " " + "q" + quarter
generate qdate = quarterly(qdate_str, "YQ")
drop qdate_str
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
/*
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
/*
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

* Calculate n employees above and below "median" length of service
generate above_med_los = 1
replace above_med_los = 0 if (los_level == "< 1" | los_level == "1-2" | los_level == "3-4" | los_level == "5-9" | los_level == "10-14")
replace above_med_los = . if los_level == "UNSP"

bysort year quarter: gegen n_above_med_los = total(emp) if above_med_los == 1
bysort year quarter: gegen n_below_med_los = total(emp) if above_med_los == 0

* Collapse
gcollapse (firstnm) n_*, by(year quarter)

save "${data_dir}/status_combined_collapsed.dta", replace

*/
********************************************************************************
* Calculate aggregate separation/accession numbers within dynamic files
********************************************************************************
/*
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

* Calculate n employees above and below "median" length of service
generate above_med_los = 1
replace above_med_los = 0 if (los_level == "< 1" | los_level == "1-2" | los_level == "3-4" | los_level == "5-9" | los_level == "10-14")
replace above_med_los = . if los_level == "UNSP"

bysort year quarter: gegen n_seps_above_med_los = total(separation_ind) if above_med_los == 1
bysort year quarter: gegen n_seps_below_med_los = total(separation_ind) if above_med_los == 0
bysort year quarter: gegen n_acce_above_med_los = total(accession_ind) if above_med_los == 1
bysort year quarter: gegen n_acce_below_med_los = total(accession_ind) if above_med_los == 0

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

generate qdate = qofd(mdy(quarter, 1, year))

drop if qdate == 112 | qdate == 113 | qdate == 114 | qdate == 140 | qdate == 172

* Calculate separation rates for each subgroup
local group_list "all post_ugrad less_ugrad above_med_pay below_med_pay above_med_los below_med_los"
foreach g of local group_list {
	generate seps_rate_`g' = n_seps_`g'/n_`g'
	generate acce_rate_`g' = n_acce_`g'/n_`g'
}

* Control for seasonal and trend effects

foreach g of local group_list {
	regress seps_rate_`g' qdate i.quarter, nocons
	predict seps_rate_`g'_adj, resid
	regress acce_rate_`g' qdate i.quarter, nocons
	predict acce_rate_`g'_adj, resid
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
parmest, saving("${output_dir}/CI_seps_rate.dta", replace)

proportion accession [fweight = n] if type != 2, over(qdate)
parmest, saving("${output_dir}/CI_acce_rate.dta", replace)
restore


* Calculate CI bands
frame create ci_frame
frame change ci_frame
use "${output_dir}/CI_seps_rate.dta", clear
split parm, parse("." "@")
destring parm1, generate(type)
destring parm3, generate(qdate)
drop if type == 0
generate seps_rate_all_CI_band = max95 - min95
keep qdate seps_rate_all_CI_band

save "${output_dir}/CI_seps_rate.dta", replace

* Repeat for accessions
use "${output_dir}/CI_acce_rate.dta", clear
split parm, parse("." "@")
destring parm1, generate(type)
destring parm3, generate(qdate)
drop if type == 0
generate acce_rate_all_CI_band = max95 - min95
keep qdate acce_rate_all_CI_band

save "${output_dir}/CI_acce_rate.dta", replace


* Plot confidence intervals for separations
frame change default
merge 1:1 qdate using "${output_dir}/CI_seps_rate.dta", nogenerate
generate seps_rate_all_adj_max95 = seps_rate_all_adj + seps_rate_all_CI_band/2
generate seps_rate_all_adj_min95 = seps_rate_all_adj - seps_rate_all_CI_band/2

format qdate %tq
tsset qdate
sort qdate

eclplot seps_rate_all_adj seps_rate_all_adj_min95 seps_rate_all_adj_max95 qdate, eplottype(scatter) estopts(msize(tiny)) ciforeground ///
xline(99 108 123 143 215) xtitle("") ytitle("Separation rate")
graph export "${output_dir}/separation_rate_adj_CI.png", replace

* Plot confidence intervals for accessions
merge 1:1 qdate using "${output_dir}/CI_acce_rate.dta", nogenerate
generate acce_rate_all_adj_max95 = acce_rate_all_adj + acce_rate_all_CI_band/2
generate acce_rate_all_adj_min95 = acce_rate_all_adj - acce_rate_all_CI_band/2


eclplot acce_rate_all_adj acce_rate_all_adj_min95 acce_rate_all_adj_max95 qdate, eplottype(scatter) estopts(msize(tiny)) ciforeground ///
xline(99 108 123 143 215) xtitle("") ytitle("Accession rate")
graph export "${output_dir}/accession_rate_adj_CI.png", replace

* Plot confidence intervals for separations, unadjusted
*merge 1:1 qdate using "${output_dir}/CI_seps_rate.dta", nogenerate
generate seps_rate_all_max95 = seps_rate_all + seps_rate_all_CI_band/2
generate seps_rate_all_min95 = seps_rate_all - seps_rate_all_CI_band/2

eclplot seps_rate_all seps_rate_all_min95 seps_rate_all_max95 qdate, eplottype(scatter) estopts(msize(tiny)) ciforeground ///
xline(99 108 123 143 215) xtitle("") ytitle("Separation rate")
graph export "${output_dir}/separation_rate_CI.png", replace

* Plot confidence intervals for accessions, unadjusted
*merge 1:1 qdate using "${output_dir}/CI_acce_rate.dta", nogenerate
generate acce_rate_all_max95 = acce_rate_all + acce_rate_all_CI_band/2
generate acce_rate_all_min95 = acce_rate_all - acce_rate_all_CI_band/2

eclplot acce_rate_all acce_rate_all_min95 acce_rate_all_max95 qdate, eplottype(scatter) estopts(msize(tiny)) ciforeground ///
xline(99 108 123 143 215) xtitle("") ytitle("Accession rate")
graph export "${output_dir}/accession_rate_CI.png", replace


drop seps_rate_all_adj_min95 seps_rate_all_adj_max95 acce_rate_all_adj_min95 acce_rate_all_adj_max95


* Calculate number of stayers
local group_list "all post_ugrad less_ugrad above_med_pay below_med_pay above_med_los below_med_los"
foreach g of local group_list {
	generate n_stay_`g' = n_`g' - n_seps_`g'
}



reshape long n_, i(qdate) j(type_subgroup) string

generate group = substr(type_subgroup, -3, 3)
generate type = substr(type_subgroup, 1, 4)
drop if (type != "stay" & type != "seps" & type != "acce")
drop if group == "all"

generate separation_ind = type == "seps"
generate accession_ind = type == "acce"


* Produce regressions for education
preserve

drop if type == "acce"
keep if group == "rad"

generate educ_ind = substr(type_subgroup, 6,4) == "post"
regress separation_ind educ_ind i.qdate educ_ind#i.qdate [fweight = n_], nocons
parmest, saving("${output_dir}/seps_rate_educ.dta", replace)

restore

preserve

drop if type == "seps"
keep if group == "rad"

generate educ_ind = substr(type_subgroup, 6,4) == "post"
regress accession_ind educ_ind i.qdate educ_ind#i.qdate [fweight = n_], nocons
parmest, saving("${output_dir}/acce_rate_educ.dta", replace)

restore

* Run regressions for median pay
preserve

drop if type == "acce"
keep if group == "pay"

generate pay_ind = substr(type_subgroup, 6,5) == "above"

regress separation_ind pay_ind i.qdate pay_ind#i.qdate [fweight = n_], nocons
parmest, saving("${output_dir}/seps_rate_med_pay.dta", replace)

restore

preserve

drop if type == "seps"
keep if group == "pay"

generate pay_ind = substr(type_subgroup, 6,5) == "above"

regress accession_ind pay_ind i.qdate pay_ind#i.qdate [fweight = n_], nocons
parmest, saving("${output_dir}/acce_rate_med_pay.dta", replace)

restore

* Run regressions for median los
preserve

drop if type == "acce"
keep if group == "los"

generate los_ind = substr(type_subgroup, 6,5) == "above"

regress separation_ind los_ind i.qdate los_ind#i.qdate [fweight = n_], nocons
parmest, saving("${output_dir}/seps_rate_med_los.dta", replace)

restore

preserve

drop if type == "seps"
keep if group == "los"

generate los_ind = substr(type_subgroup, 6,5) == "above"

regress accession_ind los_ind i.qdate los_ind#i.qdate [fweight = n_], nocons
parmest, saving("${output_dir}/acce_rate_med_los.dta", replace)

restore
*/

********************************************************************************
* Plot differences in rates over time with CIs
********************************************************************************

use "${output_dir}/seps_rate_educ.dta", clear
split parm, parse("." "#")
keep if parm1 == "1"
destring parm3, generate(qdate) force
keep qdate estimate min95 max95
drop if qdate == . // not sure what is with first period
format qdate %tq

eclplot estimate min95 max95 qdate, eplottype(scatter) estopts(msize(tiny)) ciforeground ///
xline(99 108 123 143 215) xtitle("") ytitle("Separation rate")
graph export "${output_dir}/separation_rate_educ.png", replace


use "${output_dir}/acce_rate_educ.dta", clear
split parm, parse("." "#")
keep if parm1 == "1"
destring parm3, generate(qdate) force
keep qdate estimate min95 max95
drop if qdate == . // not sure what is with first period
format qdate %tq

eclplot estimate min95 max95 qdate, eplottype(scatter) estopts(msize(tiny)) ciforeground ///
xline(99 108 123 143 215) xtitle("") ytitle("Accession rate")
graph export "${output_dir}/accession_rate_educ.png", replace


use "${output_dir}/seps_rate_med_pay.dta", clear
split parm, parse("." "#")
keep if parm1 == "1"
destring parm3, generate(qdate) force
keep qdate estimate min95 max95
drop if qdate == . // not sure what is with first period
format qdate %tq

eclplot estimate min95 max95 qdate, eplottype(scatter) estopts(msize(tiny)) ciforeground ///
xline(99 108 123 143 215) xtitle("") ytitle("Separation rate")
graph export "${output_dir}/separation_rate_med_pay.png", replace


use "${output_dir}/acce_rate_med_pay.dta", clear
split parm, parse("." "#")
keep if parm1 == "1"
destring parm3, generate(qdate) force
keep qdate estimate min95 max95
drop if qdate == . // not sure what is with first period
format qdate %tq

eclplot estimate min95 max95 qdate, eplottype(scatter) estopts(msize(tiny)) ciforeground ///
xline(99 108 123 143 215) xtitle("") ytitle("Accession rate")
graph export "${output_dir}/accession_rate_med_pay.png", replace


use "${output_dir}/seps_rate_med_los.dta", clear
split parm, parse("." "#")
keep if parm1 == "1"
destring parm3, generate(qdate) force
keep qdate estimate min95 max95
drop if qdate == . // not sure what is with first period
format qdate %tq

eclplot estimate min95 max95 qdate, eplottype(scatter) estopts(msize(tiny)) ciforeground ///
xline(99 108 123 143 215) xtitle("") ytitle("Separation rate")
graph export "${output_dir}/separation_rate_med_los.png", replace


use "${output_dir}/acce_rate_med_los.dta", clear
split parm, parse("." "#")
keep if parm1 == "1"
destring parm3, generate(qdate) force
keep qdate estimate min95 max95
drop if qdate == . // not sure what is with first period
format qdate %tq

eclplot estimate min95 max95 qdate, eplottype(scatter) estopts(msize(tiny)) ciforeground ///
xline(99 108 123 143 215) xtitle("") ytitle("Accession rate")
graph export "${output_dir}/accession_rate_med_los.png", replace














