# Dev/Prod Result Checker

The purpose of this script is to compare results in Dev mode and Production. This is an important step to ensuring Development Changes do not make any unwanted changes to existing Production results. 
This script is intended to augment manual unit test and QA testing. By automating some of the testing, Developers and QA testers can perform less manual checks. 

*Please note: Manual checks should be part of all QA testing methodologies. With the addition of these automated tests, your QA testers can focus more of their time on verification of things like drills, links, visualizations etc.* 

This script runs the **queries** for every Look-based or query-based tile on the dashboard. Subsequently, the script compares the **query results** from Dev to Production mode and flags any differences. For this reason, this script ensures Data consistency only and does not test UI components. 


# Setup

1. Configure an API service account to run the script
2. 

# Known Caveats/Issues

**The below caveats/issues are important to understand as they impact what is tested **
1. Text tiles are skipped (there should be no difference between Dev/Prod) 
2. Merged queries are skipped and are NOT compared. Merged queries are done post processing and actually generate two or more query results. The Dev/Prod Result Checker does not currently support this 
3. For large data tables, if sort orders are not specified in the visualization, Discrepancies will be thrown due to how SQL randomly sorts ties for rows that have the same measure values. All large data tables should have sorts specified. If necessary, secondardy sorts should also be specified by holding down the shift key on your keyboard within Looker and clicking on the secondary dimension. To check your exact specified sort orders, you can verify with the Order By clause in the SQL tab of the explore. 

# Support 
The Dev/Prod Result Checker is NOT an official supported product of Looker. Support for The Dev/Prod Result Checker is not included and there are no guarantees that it will work for you. The Dev/Prod Result Checker was built by Greg Li, a consultant in Looker's Professional Services organization. This is **not an open source product and should not be shared or distributed without the prior written consent of the Looker Professional Services organization**. 

