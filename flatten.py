import json
import pandas as pd
import numpy as np
from collections import deque

test_data = [{"New Customer Indicator (Yes / No)":"No","Average Spend per Customer":{"Traffic Source":{"Display":"46.83","Email":"46.47","Facebook":"45.95","Organic":"46.48","Search":"46.24"}}},
{"New Customer Indicator (Yes / No)":"Yes","Average Spend per Customer":{"Traffic Source":{"Display":"45.61","Email":"45.78","Facebook":"46.34","Organic":"47.15","Search":"44.69"}}}]

test_data_complex = [{"Customers New Customer Indicator (Yes / No)":"No","Customers Country":"UK","Order Items Average Spend per Customer":{"Customers Traffic Source":{"Display":{"Customers Gender":{"Female":"$44.05","Male":"$49.94"}},"Email":{"Customers Gender":{"Female":"$44.46","Male":"$52.31"}},"Facebook":{"Customers Gender":{"Female":"$44.40","Male":"$47.65"}},"Organic":{"Customers Gender":{"Female":"$44.18","Male":"$49.76"}},"Search":{"Customers Gender":{"Female":"$44.21","Male":"$48.05"}}}},"Customers Count of Customers":{"Customers Traffic Source":{"Display":{"Customers Gender":{"Female":"212","Male":"165"}},"Email":{"Customers Gender":{"Female":"407","Male":"262"}},"Facebook":{"Customers Gender":{"Female":"504","Male":"396"}},"Organic":{"Customers Gender":{"Female":"1,363","Male":"1,062"}},"Search":{"Customers Gender":{"Female":"849","Male":"668"}}}}},{"Customers New Customer Indicator (Yes / No)":"No","Customers Country":"USA","Order Items Average Spend per Customer":{"Customers Traffic Source":{"Display":{"Customers Gender":{"Female":"$44.80","Male":"$49.51"}},"Email":{"Customers Gender":{"Female":"$43.48","Male":"$49.49"}},"Facebook":{"Customers Gender":{"Female":"$44.43","Male":"$47.94"}},"Organic":{"Customers Gender":{"Female":"$44.46","Male":"$49.04"}},"Search":{"Customers Gender":{"Female":"$44.25","Male":"$48.90"}}}},"Customers Count of Customers":{"Customers Traffic Source":{"Display":{"Customers Gender":{"Female":"1,141","Male":"875"}},"Email":{"Customers Gender":{"Female":"1,762","Male":"1,504"}},"Facebook":{"Customers Gender":{"Female":"2,453","Male":"1,953"}},"Organic":{"Customers Gender":{"Female":"7,055","Male":"5,393"}},"Search":{"Customers Gender":{"Female":"4,236","Male":"3,335"}}}}},{"Customers New Customer Indicator (Yes / No)":"Yes","Customers Country":"UK","Order Items Average Spend per Customer":{"Customers Traffic Source":{"Display":{"Customers Gender":{"Female":"$43.82","Male":"$49.50"}},"Email":{"Customers Gender":{"Female":"$46.11","Male":"$41.95"}},"Facebook":{"Customers Gender":{"Female":"$42.47","Male":"$43.22"}},"Organic":{"Customers Gender":{"Female":"$49.11","Male":"$49.08"}},"Search":{"Customers Gender":{"Female":"$43.71","Male":"$45.72"}}}},"Customers Count of Customers":{"Customers Traffic Source":{"Display":{"Customers Gender":{"Female":"56","Male":"47"}},"Email":{"Customers Gender":{"Female":"94","Male":"80"}},"Facebook":{"Customers Gender":{"Female":"141","Male":"90"}},"Organic":{"Customers Gender":{"Female":"366","Male":"340"}},"Search":{"Customers Gender":{"Female":"244","Male":"195"}}}}},{"Customers New Customer Indicator (Yes / No)":"Yes","Customers Country":"USA","Order Items Average Spend per Customer":{"Customers Traffic Source":{"Display":{"Customers Gender":{"Female":"$41.68","Male":"$50.32"}},"Email":{"Customers Gender":{"Female":"$43.39","Male":"$49.22"}},"Facebook":{"Customers Gender":{"Female":"$45.24","Male":"$49.19"}},"Organic":{"Customers Gender":{"Female":"$44.39","Male":"$49.78"}},"Search":{"Customers Gender":{"Female":"$42.77","Male":"$47.18"}}}},"Customers Count of Customers":{"Customers Traffic Source":{"Display":{"Customers Gender":{"Female":"319","Male":"247"}},"Email":{"Customers Gender":{"Female":"466","Male":"393"}},"Facebook":{"Customers Gender":{"Female":"653","Male":"550"}},"Organic":{"Customers Gender":{"Female":"1,959","Male":"1,563"}},"Search":{"Customers Gender":{"Female":"1,168","Male":"945"}}}}}]

def depth(d):
    ''' This functions determines the depth
        of nesting in a dictionary. 
    '''
    queue = deque([(id(d), d, 1)])
    memo = set()
    while queue:
        id_, o, level = queue.popleft()
        if id_ in memo:
            continue
        memo.add(id_)
        if isinstance(o, dict):
            queue += ((id(v), v, level + 1) for v in o.values())
    return level

def row(data_row,regular_columns,pivot_columns,measures):
  def pivot(pivoted_section, depth, measure):
    i = depth
    pivot_column = next(iter(pivoted_section))
    if i < len(pivot_columns):
      row[pivot_column] = next(iter(pivoted_section[pivot_column]))
      i += 1
      pivot(pivoted_section[pivot_column],i, measure)
    else:
      lowest = pivoted_section[pivot_column]
      for value in lowest:
        row[pivot_column] = value
        row[measure] = lowest[value]
        table.append(row.copy())
    return(table)
  table = []
  row = {}
  for regular_coloumn in regular_columns:
    row[regular_coloumn] = data_row[regular_coloumn]
  for measure in measures:
    #pivoted_section = data_row[measure]
    table = pivot(data_row[measure],1, measure)
  return(table)

def measures(data_row):
  print("hi")

def regular_columns(data_row):
  print("hi")

def pivot_columns(data_row):
  print("hi")

#test data
table = []
first_row = test_data[0]
depth = depth(first_row) - 1
measures = ['Average Spend per Customer']
regular_columns = ['New Customer Indicator (Yes / No)']
pivot_columns = ['Traffic Source']

for single_row in test_data:
  unpivoted_data = row(single_row,regular_columns,pivot_columns,measures)
  table.extend(unpivoted_data)
print(table)

#test data complex
table = []
first_row = test_data_complex[0]
depth = 4
measures = ['Order Items Average Spend per Customer','Customers Count of Customers']
regular_columns = ['Customers New Customer Indicator (Yes / No)','Customers Country']
pivot_columns = ['Customers Traffic Source','Customers Gender']

for single_row in test_data_complex:
  unpivoted_data = row(single_row,regular_columns,pivot_columns,measures)
  table.extend(unpivoted_data)

print(table)

# This just flattens the line out to a bunch of columns, not really what I need
# https://towardsdatascience.com/flattening-json-objects-in-python-f5343c794b10
sample_object = test_data_complex[0]
capture = pd.json_normalize(test_data_complex)
capture = capture.sort_values(by=capture.columns.tolist()).reset_index(drop=True)
capture = capture.reindex(sorted(capture.columns), axis=1)

capture.to_csv('test.csv')