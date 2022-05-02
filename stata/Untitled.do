* Produce results
capture log close
clear all

use "opm_oews_merged.dta", clear

gegen group_id = group(msa_code _2000_census_occ_code)
destring year, replace
drop if year < 2005 // Prior to 2015, not sure about MSA code used by BLS
xtset group_id year

* OPM delta
generate d3_opm_n_emp = opm_n_emp - l3.opm_n_emp
generate d3_opm_n_emp_share_tot = d3_opm_n_emp/l3.tot_emp

* OEWS delta, removing fed workers
generate d3_tot_emp = tot_emp - l3.tot_emp
generate d3_tot_emp_net = d3_tot_emp - d3_opm_n_emp
generate d3_tot_emp_net_percent = d3_tot_emp_net/l3.tot_emp
generate d3_tot_emp_percent = d3_tot_emp/l3.tot_emp


generate d3_a_mean = a_mean - l3.a_mean
generate d3_a_mean_percent = d3_a_mean/l3.a_mean

keep if year == 2008 | year == 2011 | year == 2014

encode msa_code, generate(msa_encode)

label variable d3_tot_emp_net_percent "\% $\Delta$ in non-federal emp"
label variable d3_opm_n_emp_share_tot "\% $\Delta$ in federal emp as share of total"

eststo: quietly regress d3_tot_emp_net_percent d3_opm_n_emp_share_tot, vce(cluster msa_encode)
estadd local weight No
estadd local year_fe No
estadd local year_msa_fe No
estadd local year_occ_fe No

eststo: quietly regress d3_tot_emp_net_percent d3_opm_n_emp_share_tot [w = l3.tot_emp], vce(cluster msa_encode)
estadd local weight Yes
estadd local year_fe No
estadd local year_msa_fe No
estadd local year_occ_fe No

eststo: quietly regress d3_tot_emp_net_percent d3_opm_n_emp_share_tot i.year [w = l3.tot_emp], vce(cluster msa_encode)
estadd local weight Yes
estadd local year_fe Yes
estadd local year_msa_fe No
estadd local year_occ_fe No

eststo: quietly regress d3_tot_emp_net_percent d3_opm_n_emp_share_tot i.msa_encode#i.year i.year [w = l3.tot_emp], vce(cluster msa_encode)
estadd local weight Yes
estadd local year_fe Yes
estadd local year_msa_fe Yes
estadd local year_occ_fe No

encode _2000_census_occ_code, generate(occ_encode)
eststo: quietly regress d3_tot_emp_net_percent d3_opm_n_emp_share_tot i.occ_encode#i.year i.year [w = l3.tot_emp], vce(cluster msa_encode)
estadd local weight Yes
estadd local year_fe Yes
estadd local year_msa_fe No
estadd local year_occ_fe Yes

esttab using "reg_results.tex", booktabs replace nostar se noomitted label nodepvars ///
mtitles("\% change in non-federal employment") ///
keep(d3_opm_n_emp_share_tot) s(weight year_fe year_msa_fe year_occ_fe N, label("Weight" "Year FE" "Year X MSA FE" "Year X OCC FE")) 






