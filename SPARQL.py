import pandas as pd 
from bs4 import BeautifulSoup
import re 
import requests 
import string
from SPARQLWrapper import SPARQLWrapper, JSON
pd.options.mode.chained_assignment = None
import numpy as np
from country_list import countries_for_language
import wikipedia
import math
import time
from urllib.error import HTTPError 
from wikipedia import PageError, DisambiguationError
from collections import OrderedDict
import matplotlib.pyplot as plt 
import spacy 
import random
from spacy.util import minibatch, compounding
from pathlib import Path
import nltk
from nltk.sem import extract_rels,rtuple
from nltk.chunk import tree2conlltags
from spacy.symbols import prep, VERB, pobj, PROPN, ADP 
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import pandas as pd
from scipy import interpolate
import os
import os.path
import ssl
import stat
import subprocess
import sys
import pytesseract
import cv2
import warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
import pandas as pd 
import pandas as pd 
pd.options.mode.chained_assignment = None
import copy
ssl._create_default_https_context = ssl._create_unverified_context
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import Normalizer
from sklearn.preprocessing import QuantileTransformer
from sklearn.preprocessing import PowerTransformer
import geopandas as gpd
from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler
pd.options.mode.chained_assignment = None 
import prince 




#General Wrapper functionsto prevent the actual function throwing an error 
def math_wrapper_is_nan(x):
    """
    Function that checks where x is nan without throwing an error

    """
    try: 
        return math.isnan(x)
    except TypeError: 
        return False
    
def soup_find_wrapper(x):
    """
    Function that checks if span in Soup object without throwing an error if the Soup object is None 

    """
    
    try: 
        return x.find("span", itemprop="name").text
    
    except AttributeError:
        return np.nan
    
def extract_wikidata_identifier(wikidata_link):
    
    """
    Function that returns the wikidata identifier from the URL or URI
    
    """
    if math_wrapper_is_nan(wikidata_link) or wikidata_link == None: 
        return np.nan 
    
    identifier = wikidata_link.split('/')[-1]
    
    return identifier 

#Read in dataframes that are needed
index_of_places = pd.read_csv("IPN_GB_2021.csv", encoding = "ISO-8859-1")
countries = dict(countries_for_language('en'))


"""
To make the implementation clear, I have sectioned the code into the following sections
In each section I usually define any relevent functions and follow this with the code that calls these functions 
It does take quite a while to finish
SECTION 1: DATA COLLECTION 

    Subtask 1 : Collect name and wikidata identifier on all MPs
    
        Tasks: 
            1. 
                i) Extract unique identifiers, parlimentary number, and optionally place of birth from Wikidata of MPs from first UK parliament to current UK Parliament    
            2
                i) Extract names and unique identifiers from Wikipedia of MPs from first UK parliament to current UK parliament
            3 
                i) Join these two datasets together
        
    Subtask 2 : Collect place of birth data
    
        Tasks:
            1.
                i) Collect place of birth data from Wikipedia, Wikitree, Geni and Rush Parliamentary Archives
            2. 
                i) Process and standarise this data
            3. 
                i) Take the mode of this data to find historic county 
    
    Subtask 3 : Collection population data 
        
        Tasks: 
            1. 
                i) Extract British population data from Vision of Britian
                ii) Combine this data into historical county 
            
            2. 
                i) Perform OCR and process Northern Ireland population data
    
SECTION 2: Graph and Map Creation

    Subtask 1 : Create choropleth map 
    
    Subtask 2 : Create PCA
    
    Subtask 3 : Create MFA 
    
    Subtask 4 : Create MCA 

"""



##########################################################################
#           SECTION 1 SUBTASK 1 TASK 1)i  - Query Wikidata               #
##########################################################################



#First SPARQL query that gathers all MPS from every parliament and optionally their date of birth, gender and place of birth 
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
sparql.setQuery("""

  SELECT  ?person ?PN ?gender ?dateOfBirth ?personLabel ?PNLabel ?genderLabel ?dateOfBirthLabel ?placeOfBirth ?placeOfBirthLabel 

      
  WHERE
    {
      ?person wdt:P39 ?PN .
      ?PN wdt:P279 wd:Q16707842 .
      OPTIONAL {?person wdt:P21 ?gender . }
      OPTIONAL {?person wdt:P569 ?dateOfBirth . }
      OPTIONAL {?person wdt:P19 ?placeOfBirth . } 
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". } 
    }
  """)


sparql.setReturnFormat(JSON)
wikidata_query_result = sparql.query().convert()
first_wikidata_query = pd.json_normalize(wikidata_query_result['results']['bindings'])

  

def return_party_electory_startdate(column):
    """
    Function that returns the electoral district, the political party and the start time for an MP for a specific parliament they served in
    These queries had to be split up as they were exceeded the quota set out by the sparql python interface

    """

    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    #Create final dataframe results will be appended to 
    columns = ['person.value', 'group.value', 'elect.value', 'PNN.value',  'starttime.value']
    final_results = pd.DataFrame(columns = columns)
    
    nrows = 100
    start = 0
    end = nrows 

    reached_end = False
    #Submit MP wikidata identifier in batches of length nrows
    for i in range(math.ceil((len(column) + 1)/nrows)):
        
        #Avoid too many requests Error 
        time.sleep(3)
        
        values = '{ '
        
        for j in range(start,end):
            #Check if the end of the column has been reached
            if j >= len(df):
              reached_end = True 
              break
            wikidata_identifier = column[j]
            #Append the wikidata identifiers to the values 
            values += ' wd:' + wikidata_identifier + ' '

        values += " }"
       
        
        done = False
        while done == False:
            
          try: 
             #Submit query with specific values 
            sparql.setQuery("""SELECT ?person ?PNN ?starttime ?elect ?group ?electLabel ?groupLabel WHERE {
              VALUES ?person """ + values + """
              ?person p:P39 ?statement . 
              ?statement ps:P39 ?PNN . 
              OPTIONAL{ ?statement pq:P580 ?starttime .}
              OPTIONAL{ ?statement pq:P768 ?elect . }
              OPTIONAL{ ?statement pq:P4100 ?group . }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
            } """)

            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            results_df = pd.json_normalize(results['results']['bindings'])
            done = True
          except HTTPError:
            #If too many requests error try again
            pass
    
        
        
        if 'group.value' not in list(results_df.columns):
          results_df['group.value'] = np.nan

        elif 'elect.value' not in list(results_df.columns):
          results_df['elect.value'] = np.nan

        try:
          final_results = final_results.append(results_df[['person.value', 'group.value', 'elect.value', 'PNN.value',  'starttime.value']])
        
        except KeyError:
          
          pass
        
        start += nrows
        end += nrows
        

        if reached_end == True:
          break

    
    final_results['person.value'] = final_results['person.value'].apply(extract_wikidata_identifier)
    final_results.set_index(final_results['person.value'], inplace = True)
    final_results = final_results.drop(columns=['person.value'])

    return final_results

def retrive_parliament_number(string):
  string = string.replace('Member of the ', '')
  string = string.replace('Parliament of the United Kingdom', '')
  string = string.replace('th ', '')
  string = string.replace('nd', '')
  string = string.replace('st', '')
  string = string.replace('rd', '')
  ParliamentNumber = int(string)

  return ParliamentNumber



#Process the first wikidata query to extract the unique Wikidata identifier from the wikidata URI returned 
first_wikidata_query['ID'] = first_wikidata_query['person.value'].apply(extract_wikidata_identifier)
first_wikidata_query['parliamentNumber'] = first_wikidata_query['PNLabel.value'].apply(retrive_parliament_number)

#Make a section wikidata query that returns parliamentary group , electoral district and start date for each parliament an MP served in 
second_wikidata_query = return_party_electory_startdate(first_wikidata_query['ID'])

#Process the two query datasets so they can be joined
unique_PN = list(first_wikidata_query['PN.value'].unique())
second_wikidata_query = second_wikidata_query[second_wikidata_query['PNN.value'].isin(unique_PN)]
second_wikidata_query.drop_duplicates(inplace=True)
second_wikidata_query = second_wikidata_query.rename(columns={'PNN.value': 'PN.value'})
first_wikidata_query['person.value'] = first_wikidata_query['person.value'].apply(extract_wikidata_identifier)

#Perform left join
wikidataQuery = pd.merge(first_wikidata_query, second_wikidata_query,  how='left', left_on=['person.value','PN.value'], right_on = ['person.value','PN.value'])

#Save dataframe 
wikidataQuery.to_csv('wikidataQuery.csv') # combined_df 

wikidataQuery = pd.read_csv('wikidataQuery.csv')