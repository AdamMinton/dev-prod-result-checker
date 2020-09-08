# Purpose:::
# Run a dashboard based on user selected filters in dev and prod mode and check for any discrepancies.
# If all dashboard test run successfully, you can have confidence that your dev changes have not changed your production results

# Script Overview:::
# This test scripts takes user input in the form of the dashboards_test_config file
# Users specify a dashboard name, id, and a list of fitlers with each distinct filter having one column for the name and one for the desired value
# Filter logic pulls default filters, listening filters and tile filters. User input filter override defaults, which are applied on a per tile basis along wiht the tile's filters
# Every tile is looped through and the results are stored in dictionaries. The dev and prod dictionaries are checked against each other
# If no differences are found, the entire dashboard outputs as successful. 
# Discrepancies are flagged on a per tile basis for user remediation 

import looker_sdk 
import csv
from looker_sdk import models
import json
from pyparsing import nestedExpr
import logging
import prettyprinter
from collections import OrderedDict
prettyprinter.install_extras(include=['attrs'])

# Set up logger
logging.getLogger().setLevel(logging.DEBUG)
dash_logs = logging.getLogger('dashboard_tests:')
dash_log_handler = logging.StreamHandler()
dash_log_handler.setLevel(logging.INFO)
formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(message)s',datefmt='%Y-%m-%d,%H:%M:%S')
dash_log_handler.setFormatter(formatter)        
# Add handlers to the logger
dash_logs.addHandler(dash_log_handler)

# Define variables for use in functions
results_dev = {} # type: dict
results_prod = {} # type: dict
environments = ['dev','production']
dashboard_tile_titles = [] # type: list
sdk = looker_sdk.init31()  # or init40() for v4.0 API

# Function to switch branches
def switch_session(dev_or_production):
        sdk.session()
        sdk.update_session(body = models.WriteApiSession(workspace_id = dev_or_production))

# def checkout_dev_branch(branch_name, project_name):
#     """Enter dev workspace and pull remote"""
#     branch = models.WriteGitBranch(name=branch_name)
#     sdk.update_git_branch(project_id=project_name, body=branch)
#     # Pull remote branch
#     sdk.reset_project_to_remote(project_id=project_name)

def compare_results():
        discrepancy_counter=0
        for key in results_dev:
                if results_dev[key] != results_prod[key]:
                        dash_logs.warning("Discrepancies found in results. Dashboard " +  key.split('||||')[0] + "'s Tile with Title '" + key.split('||||')[1] + "' Does Not Match. Proceed with caution and fix any errors prior to committing")
                        discrepancy_counter += 1
        if discrepancy_counter == 0:
                  dash_logs.info("Dashboard " +  key.split('||||')[0] + " Matches")


def get_default_dashboard_filter_values(dashboard_id):
        dashboard_filter_details = sdk.dashboard_dashboard_filters(dashboard_id)
        dashboard_filter_defaults = [] # type: list
        for filter in dashboard_filter_details:
                dashboard_filter_defaults.append(
                        {
                                "dashboard_filter_title":filter.title,
                                "dashboard_filter_name":filter.name,
                                "filter_default_value":filter.default_value,
                        }
                )
        return(dashboard_filter_defaults)

def generate_results(dashboard_id,dashboard_config_filters):    
                        #  Get default filter values
                        default_dashboard_filter_values = get_default_dashboard_filter_values(dashboard_id)
                        # Update the default values with the user fed test values
                        for key in dashboard_config_filters:
                                for dic in default_dashboard_filter_values:
                                        if dic['dashboard_filter_name'] == key:
                                                dic['filter_default_value'] = dashboard_config_filters[key]
                        # Loop through Dev and Prod mode
                        for environment in environments:
                                dev_or_production = environment
                                switch_session(environment)
                                # Loop through the tiles and generate results
                                elements = sdk.dashboard_dashboard_elements(dashboard_id)
                                for element in elements:
                                         if (element.query == None and element.look_id ==  None)or element.merge_result_id != None:
                                                # Skip text tiles and merged results
                                                continue
                                         else:
                                                 if element.look_id !=  None:
                                                 #If Look, need to access the Look object first
                                                        element_query = element.look.query
                                                        title = element.look.title
                                                 else:
                                                        element_query = element.query
                                                        title = element.title
                                #  Pull all the parts of the tile's query: 
                                                 tile_model = element_query.model
                                                 tile_view = element_query.view
                                                 tile_fields = element_query.fields
                                                 tile_pivots = element_query.pivots
                                                 tile_sorts = element_query.sorts
                                                 tile_query_timezone = element_query.query_timezone
                                                 tile_limit = element_query.limit
                                                 tile_total = element_query.total
                                                 tile_row_total = element_query.row_total
                                                 tile_fill_fields = element_query.fill_fields
                                                 tile_dynamic_fields= element_query.dynamic_fields
                                                #  These are the filters that were created when the tile was created
                                                 tile_level_filters = element_query.filters
                                                #  If no filters are applied, set to blank dictionary
                                                 if tile_level_filters == None:
                                                         tile_level_filters = {} # type: dict
                                                # These are the filters that are applied via the dashboard and listening. 
                                                # Since dashboard level filters do not have to be applied to every tile, this needs to be checked per tile
                                                 dashboard_level_filters_for_tile = element.result_maker.filterables[0]
                                                 listeners = vars(dashboard_level_filters_for_tile)['listen']
                                                # Setup blank dict to hold the tiles default filters
                                                 tile_default_filters =  [] # type: list
                                                # Loop through and get the dashboard defaults for the current tile and populate list. 
                                                # This is compared against the dashboard defaults so only the defaults that apply to the listener are pulled
                                                 for listener in listeners:
                                                         listener = vars(listener)
                                                         for default in default_dashboard_filter_values:
                                                                 if listener['dashboard_filter_name'] == default['dashboard_filter_name']:
                                                                         listener.update({'value':default['filter_default_value']})
                                                                         listener.update({'field':listener['field']})
                                                                         tile_default_filters.append(listener)
                                                # Reformat tile defaults into single dict to make for easier comparison
                                                 tile_defaults_for_comp = {}
                                                 for dic in tile_default_filters:
                                                         del dic['dashboard_filter_name']
                                                         field = dic['field']
                                                         value = dic['value']
                                                         tile_defaults_for_comp[field]=value
                                                
                                                # Use Dictionary unpacking to merge. Filters set in the dashboard_tests_config.csv file take precedence, then default filters, then finally the tile filters
                                                 all_applicable_filters = {**tile_level_filters,**tile_defaults_for_comp}                                              
                                                # # Create the final Write Query Model                                       
                                                 prep_query = models.WriteQuery(
                                                         model=tile_model,
                                                         view=tile_view,
                                                         fields=tile_fields,
                                                         pivots=tile_pivots,
                                                         sorts=tile_sorts,
                                                         query_timezone=tile_query_timezone,
                                                         limit=tile_limit,
                                                         total=tile_total,
                                                         row_total=tile_row_total,
                                                         fill_fields=tile_fill_fields,
                                                         dynamic_fields=tile_dynamic_fields,
                                                         filters=all_applicable_filters
                                                )
                                                # Run the tile and output it to either the dev or prod dictionary 
                                                 if dev_or_production == "dev":
                                                         results_dev[dashboard_id + "||||" + title] = json.loads(sdk.run_inline_query(result_format="json",body=prep_query),object_pairs_hook=OrderedDict)
                                                 else:
                                                         results_prod[dashboard_id + "||||" + title] = json.loads(sdk.run_inline_query(result_format="json",body=prep_query),object_pairs_hook=OrderedDict)
                                # Run the compare results function. Then clean up dictionaries used for comparisons
                        compare_results()
                        results_dev.clear()
                        results_prod.clear()

def main():
        # Load user config file
        with open('dashboard_tests_config.csv', newline='') as csvfile:
                dashboard_config = csv.reader(csvfile, delimiter=',')
#  Skip the first line of the CSV as the headers are just for data entry aid
                next(csvfile)  
                # Pull dashboard id and format the filter columns into a single filter dictionary, then generate results
                for row in dashboard_config:
                        dashboard_config_filters = {} # type: dict
                        # Pull Dashboard Name and ID
                        dashboard_name = row[0]
                        dashboard_id = row[1]
                        # Add all the user-entered filters into one dictionary
                        for i in range(len(row)):
                                if i >1 and i % 2 == 0:
                                        name =row[i]
                                        value =row[i+1]
                                        dashboard_config_filters[name] = value
                        generate_results(dashboard_id,dashboard_config_filters)           

if __name__ == "__main__":
    main()