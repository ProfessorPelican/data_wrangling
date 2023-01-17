# -*- coding: utf-8 -*-
"""
Created on Sat Jan 23 11:10:24 2016

@author: kevinstrickler
Data Wrangling
"""


"""
XML Parser
"""
import xml.etree.ElementTree as ET

ARTICLE_FILE = "/Users/kevinstrickler/Documents/Udacity/data_wrangling/exampleResearchArticle.xml"

def get_root(fname):
    tree = ET.parse(fname)
    return tree.getroot()

def get_authors(root):
    authors = []
    for author in root.findall('./fm/bibl/aug/au'):
        data = {
                "fnm": None,
                "snm": None,
                "email": None,
                "insr": []
        }
        
        data['fnm'] = author.find('fnm').text
        data['snm'] = author.find('snm').text
        data['email'] = author.find('email').text
        insr = author.findall('insr')
        for i in insr:
            data["insr"].append(i.attrib["iid"])    

        authors.append(data)

    return authors

root = get_root(ARTICLE_FILE)
authors = get_authors(root)


"""
Web scraping with BeautifulSoup4
"""
from bs4 import BeautifulSoup
import requests

def options(soup, id):
    # Pulls out a list of option_values from page based on id passed into function.
    option_values = []
    option_list = soup.find(id=id) #Find major element with id passed in.
    for option in option_list.find_all('option'): # Find all options in element.
        option_values.append(option['value']) # Append value for each option.
        # option_values.append(option.text) # Append text for each option.
    return option_values

WEBSITE = '/Users/kevinstrickler/Documents/Udacity/data_wrangling/Data Elements.html'
soup = BeautifulSoup(open(WEBSITE)) # Open file first and then create BeautifulSoup object.

carriers = options(soup, 'CarrierList')
airports = options(soup, 'AirportList')

# Get additional parameter info.
data = {"eventvalidation": "",
        "viewstate": ""}
data['eventvalidation'] = soup.find(id='__EVENTVALIDATION')['value']
data['viewstate'] = soup.find(id='__VIEWSTATE')['value']

#Post requests to website to get data, uses all necessary parameters.
def make_request(data):
    eventvalidation = data["eventvalidation"]
    viewstate = data["viewstate"]
    s = requests.Session() # Session/ cookie data is needed as a parameter.
    r = s.get("http://www.transtats.bts.gov/Data_Elements.aspx?Data=2")
    r = s.post("http://www.transtats.bts.gov/Data_Elements.aspx?Data=2",
                      data={'AirportList': "BOS",
                            'CarrierList': "VX",
                            'Submit': 'Submit',
                            "__EVENTTARGET": "",
                            "__EVENTARGUMENT": "",
                            "__EVENTVALIDATION": eventvalidation,
                            "__VIEWSTATE": viewstate
                    })

    return BeautifulSoup(r.text, 'lxml')


"""
Parsing returned HTML page from web scraping.
"""
from bs4 import BeautifulSoup

f = '/Users/kevinstrickler/Documents/Udacity/data_wrangling/FL-ATL.html'

def process_file(f):
    """This is example of the data structure you should return.
    Each item in the list should be a dictionary containing all the relevant data
    from each row in each file. Note - year, month, and the flight data should be 
    integers. You should skip the rows that contain the TOTAL data for a year
    data = [{"courier": "FL",
            "airport": "ATL",
            "year": 2012,
            "month": 12,
            "flights": {"domestic": 100,
                        "international": 100}
            },
            {"courier": "..."}
    ]
    """
    data = []
    info = {}
    info['flights'] = {}
    # Get courier and airport from file name.
    info["courier"], info["airport"] = f[:-5].split('/')[-1].split('-')
    # Note: create a new dictionary for each entry in the output data list.
    # If you use the info dictionary defined here each element in the list 
    # will be a reference to the same info dictionary.
    with open(f, "r") as html:
        soup = BeautifulSoup(html, 'lxml')
        
        # Obtain table first (with data).
        table = soup.find( "table", id="DataGrid1")        
        
        # Loop through table and get contents of relevant cells.        
        for row in table.findAll('tr'):
            if (row.find_all('td')[0].text != 'Year' and 
                    row.find_all("td")[1].text != 'TOTAL'):    
                info['year'] = int(row.find_all("td")[0].text)
                info['month'] = int(row.find_all("td")[1].text)
                # Use list comprehension to convert list of strings to integers.
                
                info['flights']['domestic'] = int(row.find_all("td")[2].text.split(',')[0] + row.find_all("td")[2].text.split(',')[1])
                info['flights']['international'] = int(row.find_all("td")[3].text.split(',')[0] + row.find_all("td")[3].text.split(',')[1])
                data.append(info)
    return data

def test():
    print("Running a simple test...")
    open_zip(datadir)
    files = process_all(datadir)
    data = []
    for f in files:
        data += process_file(f)
        
    assert len(data) == 399  # Total number of rows
    for entry in data[:3]:
        assert type(entry["year"]) == int
        assert type(entry["month"]) == int
        assert type(entry["flights"]["domestic"]) == int
        assert len(entry["airport"]) == 3
        assert len(entry["courier"]) == 2
    assert data[0]["courier"] == 'FL'
    assert data[0]["month"] == 10
    assert data[-1]["airport"] == "ATL"
    assert data[-1]["flights"] == {'international': 108289, 'domestic': 701425}
    
    print("... success!")


"""
Data auditing and cleaning
"""

"""
Your task is to check the "productionStartYear" of the DBPedia autos datafile for valid values.
The following things should be done:
- check if the field "productionStartYear" contains a year
- check if the year is in range 1886-2014
- convert the value of the field to be just a year (not full datetime)
- the rest of the fields and values should stay the same
- if the value of the field is a valid year in the range as described above,
  write that line to the output_good file
- if the value of the field is not a valid year as described above, 
  write that line to the output_bad file
- discard rows (neither write to good nor bad) if the URI is not from dbpedia.org
- you should use the provided way of reading and writing data (DictReader and DictWriter)
  They will take care of dealing with the header.

You can write helper functions for checking the data and writing the files, but we will call only the 
'process_file' with 3 arguments (inputfile, output_good, output_bad).
"""
import csv
import pprint

INPUT_FILE = '/Users/kevins/Documents/Udacity/data_wrangling/autos.csv'
OUTPUT_GOOD = 'autos-valid.csv'
OUTPUT_BAD = 'FIXME-autos.csv'

def process_file(input_file, output_good, output_bad):

    with open(INPUT_FILE, "rt", encoding='utf8') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        all_data = list(reader)
        
    
    # This is just an example on how you can use csv.DictWriter
    # Remember that you have to output 2 files
    with open(output_good, "w") as g:
        writer = csv.DictWriter(g, delimiter=",", fieldnames= header)
        writer.writeheader()
        for row in all_data:
            if (validyear(row['productionStartYear']) is True and
                row['URI'][7:14] == 'dbpedia'):
                row['productionStartYear'] = convertyear(row['productionStartYear'])
                writer.writerow(row)

    with open(output_bad, "w") as g:
        writer = csv.DictWriter(g, delimiter=",", fieldnames= header)
        writer.writeheader()
        for row in all_data:
            if (validyear(row['productionStartYear']) is False and
                row['URI'][7:14] == 'dbpedia'):
                writer.writerow(row)

def validyear(y):
    if isinstance(y, int):
        if 1886 <= y <= 2014:
            return True
        else:
            return False
    try:
        if 1886 <= int(y[:4]) <= 2014:
            return True
        else:
            return False
    except ValueError:
        return False

def convertyear(y):
    return int(y[:4])

def test():
    process_file(INPUT_FILE, OUTPUT_GOOD, OUTPUT_BAD)


"""
Data Cleaning/ Auditing Examples
"""
CITIES = '/Users/kevins/Documents/Udacity/data_wrangling/cities.csv'

import codecs
import csv
import json
import pprint

FIELDS = ["name", "timeZone_label", "utcOffset", "homepage", "governmentType_label", "isPartOf_label", "areaCode", "populationTotal", 
          "elevation", "maximumElevation", "minimumElevation", "populationDensity", "wgs84_pos#lat", "wgs84_pos#long", 
          "areaLand", "areaMetro", "areaUrban"]

def audit_file(filename, fields):
    fieldtypes = {}

    with open(CITIES, "rt", encoding='utf8') as f:
        reader = csv.DictReader(f)
        header = FIELDS
        all_data = list(reader)
        del all_data[:3]
    
    # Loops through each field and returns set containing different types of 
    # data in the fields.    
    
    # First make a set for each field (use set so no duplicates).    
    for field in FIELDS:
        fieldtypes[field] = set()
        
    # Now loop through to add field types for each field
    for row in all_data:
        for field in FIELDS:
            string_to_type
            fieldtypes[field].add(type(string_to_type(row[field])))
    
    return fieldtypes

def string_to_type(field):
    #Changes strings to integers, floats, or nulls if that is what they represent.
    try:
        return int(field)
    except ValueError:
        pass
    
    try:
        return float(field)
    except ValueError:
        pass
        
    if field == 'NULL':
        return None
    elif field[:1] == '{':
        return field[1:-1].split('|')
    else:
        return field
    
def test():
    fieldtypes = audit_file(CITIES, FIELDS)

    pprint.pprint(fieldtypes)

    assert fieldtypes["areaLand"] == set([type(1.1), type([]), type(None)])
    assert fieldtypes['areaMetro'] == set([type(1.1), type(None)])
    
if __name__ == "__main__":
    test()


"""
Try out MongoDB
"""
from pymongo import MongoClient
import pprint

from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client.examples

query = {'manufacturer':'Porsche'}
autos = db.autos.find(query)
autos.count()
pprint.pprint(query[0])
