# dev-prod-result-checker

The purpose of this script is to compare results in Dev mode and Production. This is an important step to ensuring Development Changes do not make any unwanted changes to existing Production results. 
This script is intended to augment manual unit test and QA testing. By automating some of the testing, Developers and QA testers can perform less manual checks. 

*Please note: Manual checks should be part of all QA testing methodologies. With the addition of these automated tests, your QA testers can focus more of their time on verification of things like drills, links, visualizations etc.* 

This script runs the **queries** for every tile on the dashboard. Subsequently, the script compares the **query results** from Dev to Production mode and flags any differences. For this reason, this script ensures Data consistency only and does not test UI components. 


