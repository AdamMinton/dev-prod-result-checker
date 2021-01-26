# Dev/Prod Result Checker

The purpose of this script is to compare results in Dev mode and Production. This is an important step to ensuring Development Changes do not make any unwanted changes to existing Production results. 
This script is intended to augment manual unit test and QA testing. By automating some of the testing, Developers and QA testers can perform less manual checks. Testing automation is highly recommended in Embedded scenarios, however is also broadly applicable to any Looker scenario. 

*Please note: Manual checks should be part of all QA testing methodologies. With the addition of these automated tests, your QA testers can focus more of their time on verification of things like drills, links, visualizations etc.* 

This script runs the **queries** for every Look-based or query-based tile on the dashboard. Subsequently, the script compares the **query results** from Dev to Production mode and flags any differences. For this reason, this script ensures Data consistency only and does not test UI components. 


# Setup

1. Configure an API service account to run the script
2. Generate API keys for the service account. 
3. Store your API keys in a secure location. This script is currently configured to pull from a .ini file to allow for quick testing. This is not secure in a production environment. The actual API keys should be stored in environment variables or an alterative secure location. 
4. Configure your automated tests in the dashboard_tests_config.csv file. Each line is a new test. Specify the dashboard name and ID. From there, add Filter Name and Filter Value Pairs. Unlimited pairs can be added as the script just loops through pairs of Columns. The name of the Filter must match the name as displayed on the dashboard. Dashboards can be specified more than once and with different filter values. 
5. Prior to running the script, decide which arugments should be fed in: *--branch* will tell the script which branch to test in development mode. The script will reset the API Service Account's local branch to match the Git version of the branch prior to comparison *--output_tile_results* will output each tile's results in Dev and Production mode. Proceed with caution with this argument because if many tests are configured the overall output can be quite large *--output_differences* will output only tiles that do not match. Both the Development and Product Mode results will be output for easy comparison! 
6. Run the script. 

It is recommended that Pull Requests be Required for the Looker Project. 

# Interpreting Output
The script will either:

1. Show that the dashboard test ran successfully in which case no action is required (unless a change was anticipated) 

   Example: 
    *2020-09-08,15:47:51.554 dashboard_tests: INFO     Dashboard 493 Matches*

2. Show that one or more tiles have discrepancies:

   Example: 
    *2020-09-08,15:44:55.150 dashboard_tests: WARNING  Discrepancies found in results. Dashboard 493's Tile with Title 'check' Does Not Match. Proceed with           caution and fix any errors prior to committing*

In the case of discrepancies, it is up to the Developer to remediate. This discrepancy will either be correct and due to a planned LookML change or need to be troubleshooted and corrected due to an accidental breaking LookML change. 



# Known Caveats/Issues

**The below caveats/issues are important to understand as they impact what is tested**
1. This script should be run under an API service account. The reset git connection API endpoint used makes sure that the script is always testing the latest checked in code of the branch. However, this endpoint also deletes any non-committed changes. If the API credentials used are of an actual developer that has Saved but Non-Committed changes for the branch in question work will be lost. 
2. Text tiles are skipped (there should be no difference between Dev/Prod) 
3. Merged queries are skipped and are NOT compared. Merged queries are done post processing and actually generate two or more query results. The Dev/Prod Result Checker does not currently support this 
4. For large data tables, if sort orders are not specified in the visualization, Discrepancies will be thrown due to how SQL randomly sorts ties for rows that have the same measure values. All large data tables should have sorts specified. If necessary, secondardy sorts should also be specified by holding down the shift key on your keyboard within Looker and clicking on the secondary dimension. To check your exact specified sort orders, you can verify with the Order By clause in the SQL tab of the explore. 
5. Script takes Dashboard IDs as input. Slugs have not been tested

# Support 
The Dev/Prod Result Checker is NOT an officially supported product of Looker. Support for The Dev/Prod Result Checker is not included and there are no guarantees that it will work for you. The Dev/Prod Result Checker was built by Greg Li, a consultant in Looker's Professional Services organization. This is **not an open source product and should not be shared or distributed without the prior written consent of the Looker Professional Services organization**. 

