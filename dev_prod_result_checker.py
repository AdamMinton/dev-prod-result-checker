# Purpose:::
# Run a dashboard based on user selected filters in dev and prod mode and check for any discrepancies.
# If all dashboard test run successfully, you can have confidence that your dev changes have not changed your production results

# Script Overview:::
# This test scripts takes user input in the form of the dashboards_test_config file
# Users specify a dashboard name, id, and a list of filters with each distinct filter having one column for the name and one for the desired value
# Filter logic pulls default filters, listening filters and tile filters. User input filter override defaults, which are applied on a per tile basis along wiht the tile's filters
# Every tile is looped through and the results are stored in dictionaries. The dev and prod dictionaries are checked against each other
# If no differences are found, the entire dashboard outputs as successful. 
# Discrepancies are flagged on a per tile basis for user remediation 

# import argparse 
# import compare
import csv
import json
import logging
import looker_sdk
import os
import pandas as pd
import pathlib
import prettyprinter
from collections import OrderedDict
from datetime import datetime
from looker_sdk import models
from pyparsing import nestedExpr

prettyprinter.install_extras(include=['attrs'])

# Set up logger
logging.getLogger().setLevel(logging.DEBUG)
dash_logs = logging.getLogger('content_tests:')
dash_log_handler = logging.StreamHandler()
dash_log_handler.setLevel(logging.INFO)
formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(message)s',datefmt='%Y-%m-%d,%H:%M:%S')
dash_log_handler.setFormatter(formatter)        
dash_logs.addHandler(dash_log_handler)

# Define variables for use in functions
results_a = pd.DataFrame() #{} # type: dict
results_b = pd.DataFrame() #{} # type: dict
environments = ['dev','production']
test_components = ['a','b']
compare_result = pd.DataFrame(columns=["Test Name","Sorted Result","Unsorted Result"])

#Create directory for storing testing results
target_directory = str(pathlib.Path().absolute()) + "/comparing_content_" + str(datetime.today().strftime('%Y-%m-%d-%H_%M'))
os.mkdir(target_directory)

#Initialize Looker SDK
sdk = looker_sdk.init31(section="sample")  # or init40() for v4.0 API

# Function to switch branches
def switch_session(dev_or_production):
        sdk.session()
        sdk.update_session(body = models.WriteApiSession(workspace_id = dev_or_production))

# Function to checkout git branches in Looker
def checkout_dev_branch(branch_name,lookml_project):
  branch = models.WriteGitBranch(name=branch_name)
  sdk.update_git_branch(project_id=lookml_project, body=branch)

# Funtion to pull from the remote repo. Any non-committed changes will NOT be considered during the dashboard checks
def sync_dev_branch_to_remote(lookml_project):
  sdk.reset_project_to_remote(project_id=lookml_project)

def compare_dataframes(test_name,df1,df2):

  df_compare_results = pd.DataFrame()
  df_compare_results_sorted = pd.DataFrame()
  #Order Matters
  try:
    df_compare_results = df1.equals(df2)
  except:
    df_compare_results = False

  #Order Does Not Matter
  try: 
    df1_sorted = df1.sort_values(by=df1.columns.tolist()).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=df2.columns.tolist()).reset_index(drop=True)
    df_compare_results_sorted = df1_sorted.compare(df2_sorted)
  except:
    df_compare_results_sorted = pd.DataFrame(['Unable to Compare Results'],columns=["Error Message"])

  #Set Results
  if df_compare_results:
    compare_results = 'Passed'
  else:
    compare_results = 'Failed'
  
  if df_compare_results_sorted.empty:
    compare_results_sorted = 'Passed'
  else:
    compare_results_sorted = 'Failed'

  compare_result = pd.DataFrame(columns=["Test Name","Sorted Result","Unsorted Result"])
  compare_result.loc[0] = [test_name,compare_results,compare_results_sorted]  
  return(compare_result)

def create_query_request(query):
    q = query
    return models.WriteQuery(
        model=q.model,
        view=q.view,
        fields=q.fields,
        pivots=q.pivots,
        fill_fields=q.fill_fields,
        filters=q.filters,
        sorts=q.sorts,
        limit=q.limit,
        column_limit=q.column_limit,
        total=q.total,
        row_total=q.row_total,
        subtotals=q.subtotals,
        dynamic_fields=q.dynamic_fields,
        query_timezone=q.query_timezone,
        filter_expression=q.filter_expression,
        vis_config=q.vis_config
    )

def Merge(dict1, dict2):
    res = {**dict1, **dict2}
    return res

def get_default_look_filter_values(look_id):
  elements = sdk.look(look_id)
  element_query = elements.query
  look_filter_details = element_query.filters
  if look_filter_details == None:
    look_filter_details = {}
  return(look_filter_details)

def get_default_dashboard_filter_values(dashboard_id):
  dashboard_filter_details = sdk.dashboard_dashboard_filters(dashboard_id)
  dashboard_filter_defaults = [] # type: list
  for filter in dashboard_filter_details:
    dashboard_filter_defaults.append(
    {
      "dashboard_filter_name":filter.name,
      "filter_default_value":filter.default_value,
    }
    )
  return(dashboard_filter_defaults)

def get_dashboard_element_query(dashboard_element):
  query = None
  if dashboard_element.type == 'vis':
    if dashboard_element.look_id:
      query = dashboard_element.look.query
    else:
      query = dashboard_element.query
  return(query)

def get_default_dashboard_element_filter_values(dashboard_element_id):
  dashboard_element = sdk.dashboard_element(dashboard_element_id)
  query = get_dashboard_element_query(dashboard_element)
  dashboard_element_filter_details = query.filters
  return(dashboard_element_filter_details)

def get_default_dashboard_tile_filter_values(dashboard_id, dashboard_element_id,dashboard_filters_config):
  dashboard_filters = get_default_dashboard_filter_values(dashboard_id)
  dashboard_element_filters = get_default_dashboard_element_filter_values(dashboard_element_id)
  result_maker = sdk.dashboard_element(dashboard_element_id).result_maker.filterables[0].listen

  #Loop Through Dashboard Filter Config to Update Dashboard Filters
  for dashboard_filter_config in dashboard_filters_config:
    for dashboard_filter in dashboard_filters:
      if dashboard_filter['dashboard_filter_name'] == dashboard_filter_config:
        dashboard_filter['filter_default_value'] = dashboard_filters_config[dashboard_filter_config]

  #Loop Through Dashboard Filters
  for dashboard_filter in dashboard_filters:
    #Determine if tile listens to dashboard filter
    for listen_filter in result_maker:
      if dashboard_filter['dashboard_filter_name'] == listen_filter.dashboard_filter_name:
        #Determine if overwriting tile filter or additional filter 
        if dashboard_element_filters:
          if listen_filter.field in dashboard_element_filters:
            dashboard_element_filters[listen_filter.field] = dashboard_filter['filter_default_value']
          else:
            dashboard_element_filters[listen_filter.field] = dashboard_filter['filter_default_value']
  return(dashboard_element_filters)

def generate_tile_results(dashboard_id, dashboard_element_id, dashboard_filters_config):
  #Determine Tile Filters
  dashboard_tile_filters = get_default_dashboard_tile_filter_values(dashboard_id,dashboard_element_id,dashboard_filters_config)
  #Obtain Query
  dashboard_element = sdk.dashboard_element(dashboard_element_id)
  query = get_dashboard_element_query(dashboard_element)
  query.filters = dashboard_tile_filters
  query.client_id = None  #Need to remove for WriteQuery/RunInlineQuery
  new_query = models.WriteQuery(model="initial",view="initial")
  new_query.__dict__.update(query.__dict__)
  results = sdk.run_inline_query(result_format='json',body=new_query,apply_formatting=True)
  return(results)

def determine_mode(branch_name):
  environment = None
  if branch_name == 'master':
    environment = 'production'
  else:
    environment = 'dev'
  return(environment)

def main():
  results_summary = pd.DataFrame(columns=["Test Name","Sorted Result","Unsorted Result"])
  # Load test config file
  with open('content_tests_config.csv', newline='') as csvfile:
    content_test_config = csv.reader(csvfile, delimiter=',')
  # Skip the first line of the CSV as the headers are just for data entry aid
    next(csvfile)  
  # Pull test configurations into variables
    for row in content_test_config:
      test_name = row[0]
      content_type = row[1]
      content_a_id = row[2]
      content_a_filter_config = json.loads(row[3])
      content_a_branch = row[4]
      content_b_id = row[5]
      content_b_filter_config = json.loads(row[6])
      content_b_branch = row[7]
      content_a_environment = determine_mode(content_a_branch)
      content_b_environment = determine_mode(content_b_branch)

      #If the content type is Dashboard, then test 
      if content_type == 'dashboards':
        #Dashboards can only be compared on the same object, only content_a_id is considered
        content_test_id = content_a_id
        dashboard = sdk.dashboard(content_test_id)
        #Comparisons will run on a tile by tile basis
        for dashboard_element in dashboard.dashboard_elements:
          #Only tests are being run on regular vis tiles, merge results are excluded
          if dashboard_element.type == 'vis' and dashboard_element.merge_result_id == None:
            content_test_element_id = dashboard_element.id
            for test_component in test_components:
              if test_component == 'a':
                content_test_filter_config = content_a_filter_config
                content_test_branch = content_a_branch
                content_test_environment = content_a_environment
              else:
                content_test_filter_config = content_b_filter_config
                content_test_branch = content_b_branch  
                content_test_environment = content_b_environment

              query = get_dashboard_element_query(dashboard_element)
              tile_model = sdk.lookml_model(query.model)
              tile_project = tile_model.project_name
              #Get into the correct environment
              switch_session(content_test_environment)
              if content_test_environment == 'dev' and content_test_branch:
                checkout_dev_branch(content_test_branch, tile_project)
                sync_dev_branch_to_remote(tile_project)
              results = generate_tile_results(content_test_id,content_test_element_id,content_test_filter_config)
              #Run Query
              if test_component == "a":
                try:
                  results_a = pd.read_json(results) #json.loads(,object_pairs_hook=OrderedDict)
                except:
                  results_a = pd.DataFrame(['Unable to obtain query results'],columns=["Error Message"])
              else:
                try:
                  results_b = pd.read_json(results)
                except:
                  results_b = pd.DataFrame(['Unable to obtain query results'],columns=["Error Message"])

            # Run the compare results function. Then clean up dictionaries used for comparisons
            compare_result = compare_dataframes(test_name+"_"+content_test_element_id,results_a,results_b)
            results_summary = results_summary.append(compare_result)
            results_a.to_csv(target_directory+"/"+test_name+"_"+content_test_element_id+"_result_a.csv",index=False)
            results_b.to_csv(target_directory+"/"+test_name+"_"+content_test_element_id+"_result_b.csv",index=False)
            results_a = pd.DataFrame()
            results_b = pd.DataFrame()
      elif content_type == 'looks':
        #For A and B
        for test_component in test_components:
          #Variables for test component being ran
          if test_component == 'a':
            content_test_id = content_a_id
            content_test_filter_config = content_a_filter_config
            content_test_branch = content_a_branch
            content_test_environment = content_a_environment
          else:
            content_test_id = content_b_id
            content_test_filter_config = content_b_filter_config
            content_test_branch = content_b_branch  
            content_test_environment = content_b_environment

          #Obtain a base query
          look = sdk.look(content_test_id)
          look_model = sdk.lookml_model(look.query.model)
          look_project = look_model.project_name
          #Get into the correct environment
          switch_session(content_test_environment)
          if content_test_environment == 'dev' and content_test_branch:
            checkout_dev_branch(content_test_branch, look_project)
            sync_dev_branch_to_remote(look_project)
          
          #modify with new filters
          # Get default filter values
          default_look_filter_values = get_default_look_filter_values(content_a_id)
          # Filter values for input the same way, merge will overwrite defaults from the csv
          default_look_filter_values = Merge(default_look_filter_values, content_test_filter_config)
          look.query.filters = default_look_filter_values
          #Obtain a new query definition
          look_query = create_query_request(look.query)
          #Run Query
          if test_component == "a":
            try:
              results_a = pd.read_json(sdk.run_inline_query(result_format="json",body=look_query)) #json.loads(,object_pairs_hook=OrderedDict)
            except:
              results_a = pd.DataFrame(['Unable to finish query in time for result a'],columns=["Error Message"])
          else:
            try:
              results_b = pd.read_json(sdk.run_inline_query(result_format="json",body=look_query))
            except:
              results_b = pd.DataFrame(['Unable to finish query in time for result b'],columns=["Error Message"])

        # Run the compare results function. Then clean up dictionaries used for comparisons
        compare_result = compare_dataframes(test_name,results_a,results_b)
        results_summary = results_summary.append(compare_result)
        results_a.to_csv(target_directory+"/"+test_name+"_result_a.csv",index=False)
        results_b.to_csv(target_directory+"/"+test_name+"_result_b.csv",index=False)
        results_a = pd.DataFrame()
        results_b = pd.DataFrame()
      else:
        print("Content " + content_type + " not recognized")
        
    results_summary.to_csv(target_directory+"/"+"00_test_summary.csv",index=False)

if __name__ == "__main__":
    main()