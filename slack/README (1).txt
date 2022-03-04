Date: 2019/06/06
Data source: https://archive.org/details/opm-federal-employment-data/page/n1

The file Reduced_data.csv contains the following headers:
AGYSUBT, (Agency and subagency with code)
Adjusted_Basic_Pay, (Salary not including bonuses or other income)
Age_Lower, (Lower bound of age range)
Age_Upper,
Duty_Station, (Country or city code where employee was stationed e.g. 011730089 corresponds to Huntsville, Alabama)
Education,
File_Date, (Date the data was recorded, i.e. final day of quarter gives explicit description of quarter)
Final_Quarter, (1 if the employee departs this quarter, 0 if the employee is present in the proceeding quarter)
Grade, (pay grade, should expect similar pay grade to mean similar basic pay from my understanding)
ID,
Occupation_Name, (e.g. secretary)
Plan_Name, (Type of pay plan, which determines how pay grade scales from my understanding)
Quarter_Number, (Numerical order in which this data appears, between [0, 83])
TOA (Type of Appointment)


There is an errant column named "..1" which appears in R, not in Pandas. This was removed from the .dta file.

The data was cleaned up to change indicator variables to explicit values e.g. Education_level was changed from "01"
to "No formal education". Indicator variables themselves were dropped. The rows where a person had a TOA value of 55
were dropped, as they were political appointments from my understanding of the Buzzfeed article. Employee names were
dropped. Race and gender are not included in the data.

On departures, those least affected by the late 1986 - early 1989 departures were those with little to no formal education.
Those with some college and degrees were most heavily affected.