import csv
import json
import looker_sdk
import os
import pandas as pd
import pathlib
import argparse
from datetime import datetime
from looker_sdk import models
from mdutils.mdutils import MdUtils


sdk = None
test_summary_file = None
environments = ['dev', 'production']
test_components = ['a', 'b']
compare_result = pd.DataFrame(columns=["Test Name", "Sorted Result", "Unsorted Result"])
results_a = pd.DataFrame()
results_b = pd.DataFrame()
test_name = None
content_type = None
content_a_id = None
content_a_filter_config = None
content_a_branch = None
content_b_id = None
content_b_filter_config = None
content_b_branch = None
content_test_element_id = None
results_a = None
results_b = None
error_test_skipped_merge_results = None
error_query_fails_to_run = None
error_investigate_output = None
error_merged_result = None
error_content_type = None
test_error_message = None
exact_match_result = None
unsorted_match_result = None

parser = argparse.ArgumentParser()
parser.add_argument("--file", default="00_test_summary.csv", help="name of summary file")
parser.add_argument("--config", default="content_tests_config.csv", help="csv containing tests to run")
parser.add_argument("--project", help="LookML Project")
parser.add_argument("--ini", default="looker.ini", help="ini file to parse for credentials")
parser.add_argument("--section", default="looker", help="section for credentials")
parser.add_argument("--current_project", action="store_true", help="set logger to debug for more verbosity")
args = parser.parse_args()
sdk = looker_sdk.init31(config_file=args.ini, section=args.section)
test_summary_file = args.file
config_file = args.config


def get_projects_information():
    """
        Determine projects and production branches
    """
    projects = sdk.all_projects()
    return(projects)


def switch_session(dev_or_production):
    """
        Function to switch branches
    """
    sdk.session()
    sdk.update_session(body=models.WriteApiSession(workspace_id=dev_or_production))


def checkout_dev_branch(branch_name, lookml_project):
    """
        Function to checkout git branches in Looker
    """
    branch = models.WriteGitBranch(name=branch_name)
    sdk.update_git_branch(project_id=lookml_project, body=branch)


def sync_dev_branch_to_remote(lookml_project):
    """
        Funtion to pull from the remote repo. Any non-committed
        changes will NOT be considered during the dashboard checks
    """
    sdk.reset_project_to_remote(project_id=lookml_project)


def is_nested(result):
    if result != []:
        dictionary = result[0]
        try:
            result = any(isinstance(dictionary[i], dict) for i in dictionary)
        except: # NOQA
            result = False
    else:
        result = False
    return(result)


def compare_json(test_name, json1, json2):
    df_compare_results = pd.DataFrame()
    df_compare_results_sorted = pd.DataFrame()
    df_failed_to_sort = None
    df_failed_to_load = None
    df_empty_result = None

    try:
        df1 = pd.read_json(json1)
        json1 = json.loads(json1)
        df2 = pd.read_json(json2)
        json2 = json.loads(json2)
    except: # NOQA
        df_failed_to_load = True

    if json1 == [] and json2 == []:
        df_empty_result = True

    if not df_failed_to_load:
        # Order Matters
        try:
            df_compare_results = df1.equals(df2)
        except: # NOQA
            df_compare_results = False

        try:
            if is_nested(json1):
                df1 = pd.json_normalize(json1)
            df1 = df1.sort_values(by=df1.columns.tolist()).reset_index(drop=True)
            df1 = df1.reindex(sorted(df1.columns), axis=1)
            if is_nested(json2):
                df2 = pd.json_normalize(json2)
            df2 = df2.sort_values(by=df2.columns.tolist()).reset_index(drop=True)
            df2 = df2.reindex(sorted(df2.columns), axis=1)
        except: # NOQA
            df_failed_to_sort = True

        try:
            df_compare_results_sorted = df1.equals(df2)
        except: # NOQA
            df_compare_results_sorted = False

    # Set Results
    if df_failed_to_load:
        compare_results = 'Unable to Load'
    elif df_compare_results and df_empty_result:
        compare_results = 'Passed - No Results'
    elif df_compare_results:
        compare_results = 'Passed'
    else:
        compare_results = 'Failed'

    if df_failed_to_load:
        compare_results_sorted = 'Unable to Test'
    elif df_failed_to_sort:
        compare_results_sorted = 'Unable to Test'
    elif df_compare_results_sorted and df_empty_result:
        compare_results_sorted = 'Passed - No Results'
    elif df_compare_results_sorted:
        compare_results_sorted = 'Passed'
    else:
        compare_results_sorted = 'Failed'

    compare_result = pd.DataFrame(columns=["Test Name", "Sorted Result", "Unsorted Result"])
    compare_result.loc[0] = [test_name, compare_results, compare_results_sorted]
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


def get_default_look_filter_values(look_id):
    elements = sdk.look(look_id)
    element_query = elements.query
    look_filter_details = element_query.filters
    if look_filter_details is None:
        look_filter_details = {}
    return(look_filter_details)


def get_default_dashboard_filter_values(dashboard_filters):
    dashboard_filter_defaults = []
    for filter in dashboard_filters:
        dashboard_filter_defaults.append(
            {
                "dashboard_filter_name": filter.name,
                "filter_default_value": filter.default_value,
            }
        )
    return(dashboard_filter_defaults)


def get_dashboard_element_query(dashboard_element):
    query = None
    if dashboard_element.type == 'vis':
        if dashboard_element.look_id:
            query = dashboard_element.look.query
        elif dashboard_element.result_maker_id:
            query = dashboard_element.result_maker.query
        else:
            query = dashboard_element.query
    return(query)


def get_default_dashboard_tile_filter_values(dashboard_filters, dashboard_element, dashboard_filters_config):
    dashboard_filters = get_default_dashboard_filter_values(dashboard_filters)
    dashboard_element_filters = dashboard_element.result_maker.query.filters or {}
    result_maker = dashboard_element.result_maker.filterables[0].listen

    # Loop Through Dashboard Filter Config to Update Dashboard Filters
    for dashboard_filter_config in dashboard_filters_config:
        for dashboard_filter in dashboard_filters:
            if dashboard_filter['dashboard_filter_name'] == dashboard_filter_config:
                dashboard_filter['filter_default_value'] = dashboard_filters_config[dashboard_filter_config]

    # Loop Through Dashboard Filters
    for dashboard_filter in dashboard_filters:
        # Determine if tile listens to dashboard filter
        for listen_filter in result_maker:
            if dashboard_filter['dashboard_filter_name'] == listen_filter.dashboard_filter_name:
                dashboard_element_filters[listen_filter.field] = dashboard_filter['filter_default_value']
                # Determine if overwriting tile filter or additional filter
                # BUG: Unsure what this was doing before
                # if dashboard_element_filters:
                #     if listen_filter.field in dashboard_element_filters:
                #         dashboard_element_filters[listen_filter.field] = dashboard_filter['filter_default_value']
                #     else:
                #         dashboard_element_filters[listen_filter.field] = dashboard_filter['filter_default_value']
    return(dashboard_element_filters)


def generate_tile_results(dashboard_filters, dashboard_element, dashboard_filters_config):
    # Determine Tile Filters
    dashboard_tile_filters = get_default_dashboard_tile_filter_values(dashboard_filters, dashboard_element, dashboard_filters_config)
    # Obtain Query
    query = get_dashboard_element_query(dashboard_element)
    query.filters = dashboard_tile_filters
    query.client_id = None    # Need to remove for WriteQuery/RunInlineQuery
    new_query = models.WriteQuery(model="initial", view="initial")
    new_query.__dict__.update(query.__dict__)
    results = sdk.run_inline_query(result_format='json', body=new_query, apply_formatting=True)
    return(results)


def determine_mode(projects, query_model, branch_name):
    environment = None

    for project in projects:
        if project['name'] == query_model.project_name:
            query_production_branch_name = project['git_production_branch_name']

    if branch_name == query_production_branch_name:
        environment = 'production'
    else:
        environment = 'dev'
    return(environment)


def output_results(target_directory, test_name, content_type, content_a_id, content_b_id, content_test_element_id, exact_match_result, unsorted_match_result, error_test_skipped_merge_results, error_query_fails_to_run, error_investigate_output, error_merged_result, error_content_type):
    if error_test_skipped_merge_results:
        test_error_message = "TEST_SKIPPED_MERGE_RESULTS"
    elif error_query_fails_to_run:
        test_error_message = "QUERY_FAILS_TO_RUN"
    elif error_investigate_output:
        test_error_message = "INVESTIGATE_OUTPUT"
    elif error_merged_result:
        test_error_message = "MERGED_RESULT"
    elif error_content_type:
        test_error_message = "CONTENT_NOT_RECOGNIZED"
    else:
        test_error_message = ""

    csv_line = test_name + "," + content_type + "," + str(content_a_id) + "," + str(content_b_id) + "," + content_test_element_id + "," + exact_match_result + "," + unsorted_match_result + "," + test_error_message

    with open(target_directory + '/' + test_summary_file, 'a') as file:
        file.write(csv_line)
        file.write('\n')


def add_level(results):
    errors = 0
    warnings = 0
    passes = 0
    results = list(results)
    for row in results:
        if (row['exact_match_result'] == 'Failed' and row['unsorted_match_result'] == 'Failed') or row['test_error_message'] != '':
            row['level'] = '⛔'
            errors += 1
        elif row['exact_match_result'] == 'Failed' and row['unsorted_match_result'] == 'Passed':
            row['level'] = '🚧'
            warnings += 1
        else:
            row['level'] = '✅'
            passes += 1

    summary = f"{errors} ⛔ | {warnings} 🚧 | {passes} ✅"
    return(results, summary)


def output_markdown(target_directory, results, summary):
    mdFile = MdUtils(file_name=target_directory + '/' + 'Content_Test_results', title='Content Test ' + summary)

    column_headers = ["Test Name", "Content Type", "Content A ID", "Content B ID", "Content Element ID", "Exact Match", "Unsorted Match", "Test Error Message", "Level"]

    number_of_columns = len(column_headers)
    number_of_rows = 0
    for row in results:
        number_of_rows += 1
        column_headers.extend(row.values())

    mdFile.new_line()
    mdFile.new_table(columns=number_of_columns, rows=number_of_rows + 1, text=column_headers)
    mdFile.create_md_file()


def get_models_information(project_name):
    models = sdk.all_lookml_models(fields="project_name,name")
    project_models = []
    for model in models:
        if model.project_name == project_name:
            model.active_project = True
            project_models.append(model.name)
        else:
            model.active_project = False
    return (models, project_models)


def main():
    project_name = args.project

    # Create directory for storing testing results
    target_directory = str(pathlib.Path().absolute()) + "/comparing_content_" + str(datetime.today().strftime('%Y-%m-%d-%H_%M'))
    os.mkdir(target_directory)

    # Make summary file
    csv_header_line = "test_name" + "," + "content_type" + "," + "content_a_id" + "," + "content_b_id" + "," + "content_element_id" + "," + "exact_match_result" + "," + "unsorted_match_result" + "," + "test_error_message"
    with open(target_directory + '/' + test_summary_file, 'a') as file:
        file.write(csv_header_line)
        file.write('\n')

    projects = get_projects_information()
    (models, project_models) = get_models_information(project_name)

    # Load test config file
    with open(config_file, 'r', newline='') as csvfile:
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
            # Reset test
            results_a = pd.DataFrame()
            results_b = pd.DataFrame()
            error_test_skipped_merge_results = False
            error_query_fails_to_run = False
            error_investigate_output = False
            error_merged_result = False
            error_content_type = False
            print("Starting test: " + test_name)
            # If the content type is Dashboard, then test
            if content_type == 'dashboards':
                # Dashboards can only be compared on the same object, only content_a_id is considered
                content_test_id = content_a_id
                dashboard = sdk.dashboard(content_test_id)

                # Remove from dashboard elements any non-viz (i.e. non-query) tiles
                dashboard.dashboard_elements = [x for x in dashboard.dashboard_elements if x.type == 'vis']

                # Comparisons will run on a tile by tile basis
                for dashboard_element in dashboard.dashboard_elements:
                    # Reset test
                    results_a = pd.DataFrame()
                    results_b = pd.DataFrame()
                    error_test_skipped_merge_results = False
                    error_query_fails_to_run = False
                    error_investigate_output = False
                    error_merged_result = False
                    error_content_type = False
                    # Only tests are being run on regular vis tiles, merge results are excluded
                    if dashboard_element.result_maker.merge_result_id is None and (dashboard_element.result_maker.query.model in project_models if project_name else True):
                        content_test_element_id = dashboard_element.id
                        for test_component in test_components:
                            query = get_dashboard_element_query(dashboard_element)
                            tile_model = sdk.lookml_model(query.model)
                            tile_project = tile_model.project_name
                            if test_component == 'a':
                                content_test_filter_config = content_a_filter_config
                                content_test_branch = content_a_branch
                                content_test_environment = determine_mode(projects, tile_model, content_a_branch)
                            else:
                                content_test_filter_config = content_b_filter_config
                                content_test_branch = content_b_branch
                                content_test_environment = determine_mode(projects, tile_model, content_b_branch)

                            # Get into the correct environment
                            switch_session(content_test_environment)
                            try:
                                if content_test_environment == 'dev' and content_test_branch:
                                    checkout_dev_branch(content_test_branch, tile_project)
                                    sync_dev_branch_to_remote(tile_project)
                                results = generate_tile_results(dashboard.dashboard_filters, dashboard_element, content_test_filter_config)
                            except: # NOQA
                                results = pd.DataFrame(['Unable to obtain query results'], columns=["Error Message"])
                            # Run Query
                            if test_component == "a":
                                results_a = results
                            else:
                                results_b = results

                        # Run the compare results function. Then clean up dictionaries used for comparisons
                        compare_result = compare_json(test_name + "_" + content_test_element_id, results_a, results_b)

                        # Output Files
                        result_a_file = target_directory + "/" + test_name + "_" + content_test_element_id + "_result_a.csv"
                        result_b_file = target_directory + "/" + test_name + "_" + content_test_element_id + "_result_b.csv"
                        try:
                            results_a = pd.read_json(results_a)
                            results_a.to_csv(result_a_file, index=False)
                        except: # NOQA
                            error_investigate_output = True
                            with open(result_a_file, 'w', newline='') as csvfile:
                                my_writer = csv.writer(csvfile, delimiter=' ')
                                my_writer.writerow(results_a)
                        try:
                            results_b = pd.read_json(results_b)
                            results_b.to_csv(result_b_file, index=False)
                        except: # NOQA
                            error_investigate_output = True
                            with open(result_b_file, 'w', newline='') as csvfile:
                                my_writer = csv.writer(csvfile, delimiter=' ')
                                my_writer.writerow(results_b)

                        exact_match_result = compare_result["Sorted Result"].loc[0]
                        unsorted_match_result = compare_result["Unsorted Result"].loc[0]
                        output_results(target_directory, test_name, content_type, content_a_id, content_b_id, content_test_element_id, exact_match_result, unsorted_match_result, error_test_skipped_merge_results, error_query_fails_to_run, error_investigate_output, error_merged_result, error_content_type)
                    elif project_name and dashboard_element.result_maker.query.model not in project_models:
                        print("Unable to Test Element - Query defined outside of current project")
                    elif dashboard_element.result_maker.merge_result_id is not None:
                        print("Unable to Test Element - Merged Results Not Supported")
                        error_merged_result = True
                        output_results(target_directory, test_name, content_type, content_a_id, content_b_id, dashboard_element.id, "NA", "NA", error_test_skipped_merge_results, error_query_fails_to_run, error_investigate_output, error_merged_result, error_content_type)
                    else:
                        print("Unable to Test Element")

            elif content_type == 'looks':
                # For A and B
                for test_component in test_components:
                    # Variables for test component being ran
                    if test_component == 'a':
                        content_test_id = content_a_id
                        content_test_filter_config = content_a_filter_config
                        content_test_branch = content_a_branch
                        content_test_environment = determine_mode(projects, tile_model, content_a_branch)
                    else:
                        content_test_id = content_b_id
                        content_test_filter_config = content_b_filter_config
                        content_test_branch = content_b_branch
                        content_test_environment = determine_mode(projects, tile_model, content_b_branch)

                    # Obtain a base query
                    look = sdk.look(content_test_id)
                    look_model = sdk.lookml_model(look.query.model)
                    look_project = look_model.project_name
                    # Get into the correct environment
                    switch_session(content_test_environment)
                    try:
                        if content_test_environment == 'dev' and content_test_branch:
                            checkout_dev_branch(content_test_branch, look_project)
                            sync_dev_branch_to_remote(look_project)
                        # modify with new filters
                        # Get default filter values
                        default_look_filter_values = get_default_look_filter_values(content_a_id)
                        # Filter values for input the same way, merge will overwrite defaults from the csv
                        default_look_filter_values = {**default_look_filter_values, **content_test_filter_config}
                        look.query.filters = default_look_filter_values
                        # Obtain a new query definition
                        look_query = create_query_request(look.query)
                        results = sdk.run_inline_query(result_format="json", body=look_query)
                    except: # NOQA
                        results = pd.DataFrame(['Unable to obtain query results'], columns=["Error Message"])

                    if test_component == "a":
                        results_a = results
                    else:
                        results_b = results

                # Run the compare results function. Then clean up dictionaries used for comparisons
                compare_result = compare_json(test_name, results_a, results_b)
                result_a_file = target_directory + "/" + test_name + "_result_a.csv"
                result_b_file = target_directory + "/" + test_name + "_result_b.csv"
                try:
                    results_a = pd.read_json(results_a)
                    results_a.to_csv(result_a_file, index=False)
                except: # NOQA
                    error_investigate_output = True
                    with open(result_a_file, 'w', newline='') as csvfile:
                        my_writer = csv.writer(csvfile, delimiter=' ')
                        my_writer.writerow(results_a)
                try:
                    results_b = pd.read_json(results_b)
                    results_b.to_csv(result_b_file, index=False)
                except: # NOQA
                    error_investigate_output = True
                    with open(result_b_file, 'w', newline='') as csvfile:
                        my_writer = csv.writer(csvfile, delimiter=' ')
                        my_writer.writerow(results_b)

                exact_match_result = compare_result["Sorted Result"].loc[0]
                unsorted_match_result = compare_result["Unsorted Result"].loc[0]
                output_results(target_directory, test_name, content_type, content_a_id, content_b_id, "NA", exact_match_result, unsorted_match_result, error_test_skipped_merge_results, error_query_fails_to_run, error_investigate_output, error_merged_result, error_content_type)

            else:
                error_content_type = True
                output_results(target_directory, test_name, content_type, content_a_id, content_b_id, "NA", "NA", "NA", error_test_skipped_merge_results, error_query_fails_to_run, error_investigate_output, error_merged_result, error_content_type)

    dict_from_csv = csv.DictReader(open(target_directory + '/' + test_summary_file))

    dict_from_csv, summary = add_level(dict_from_csv)

    output_markdown(target_directory, dict_from_csv, summary)


main()
