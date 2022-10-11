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





##########################################################################
#      SECTION 1 SUBTASK 1 TASK 2)i  - Extract Wikipedia Data            #
##########################################################################

    
#Create relevant functions 
def extract_names_for_letter(url, letter):
    """
    Function that extracts the names and a link to the wikipedia page of all MPs that start with a specific letter and in a specific time period. 
    
    """
    page = requests.get(url)

    time.sleep(3)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    names, links = [], []
    target = soup.find('h3',text = str(letter))
    
    try: 
        for sib in target.find_next_siblings():
            if sib.name=="h3":
                break
            else:
                
                lines = sib.find_all('li')
                for line in lines: 
                    link = line.find('a', href = True)['href']
                    link = "https://en.wikipedia.org" + link
                    name = line.text
                    names.append(name)
                    links.append(link)
    
    except AttributeError:
        pass
    
    return names, links 


def construct_url(time_period, letter): 
    """
    Function that creates URL that links to the wikipedia page for that specific time period and letter
    """

    
    if '-' in time_period:
        time = time_period.split('-')
        url = " https://en.wikipedia.org/w/index.php?title=Category:UK_MPs_"+ time[0] + "%E2%80%93" + time[1] + "&from=" + str(letter)
    
    else:
        url = "https://en.wikipedia.org/w/index.php?title=Category:UK_MPs_" + time_period + "&from=" + str(letter)
    
    return url 



def return_wikidata_identifier_from_wikipedia_page(url):   
    """
    Function that returns the wikidata identifier from a wikipedia page 
    
    """
    
    page = requests.get(url)

    time.sleep(3)
    soup = BeautifulSoup(page.text, 'html.parser')
    try:
     link = soup.find('a', {'accesskey':"g"})['href'] 
    except TypeError:
        return None
    
    link = str(link).replace('Special:EntityPage/', '')
    
    return link

 
letters = list(string.ascii_uppercase) 
overall_names = []
overall_links = [] 
time_periods = ['1801-1802', '1802-1806', '1806-1807', '1807-1812', '1812-1818', '1818-1820', '1820-1826', '1826-1830', '1830-1831', '1831-1832', '1832-1835', '1835-1837', '1837-1841', '1841-1847', '1847-1852', '1852-1857', '1857-1859', '1859-1865', '1865-1868', '1868-1874', '1874-1880', '1880-1885', '1885-1886', '1886-1892', '1892-1895', '1895-1900', '1900-1906', '1906-1910', '1910-1918', '1918-1922', '1974', '1910', '2019–present']

## First extract links and names of MPs from all Parliments from 1st-58th: 
for time_period in time_periods: 
    
    for letter in letters: 
        url = construct_url(time_period, letter)
        names, links = extract_names_for_letter(url, letter)
        overall_names.extend(names)
        overall_links.extend(links)
 
        
#Add this to a dataframe
wikipedia_df = pd.DataFrame()
wikipedia_df['names'] = overall_names
wikipedia_df['links'] = overall_links
wikipedia_df.to_csv('wikipedia_names.csv')

wikipedia_df = pd.read_csv('wikipedia_names.csv')
wikipedia_df['person.value'] = wikipedia_df['links'].apply(return_wikidata_identifier_from_wikipedia_page)

wikipedia_df.to_csv('wikipediaQuery.csv')






##########################################################################################################
#  SECTION 1 SUBTASK 1 TASK 3)i  - Join Wikidata and Wikipedia datasets on unique wikidata identifier    #
##########################################################################################################

#Read in data: 
wikidata_query = pd.read_csv('wikidataQuery.csv')
wikipedia_query = pd.read_csv('wikipediaQuery.csv')


#Remove duplicate entries (where a person served in more than one Parliament) to save time on processing
wikidata_query = wikidata_query.drop_duplicates(subset = ['person.value'])
wikipedia_query = wikipedia_query.drop_duplicates(subset = ['person.value'])

#Create wikidata_identifier to join the two datasets on
wikipedia_query = wikipedia_query.rename(columns={'person.value': 'wikidata_link'})
wikipedia_query['wikidata_identifier'] = wikipedia_query['wikidata_link'].apply(extract_wikidata_identifier)
wikidata_query['wikidata_identifier'] = wikidata_query['person.value'].apply(extract_wikidata_identifier)

wikidata_query.set_index('wikidata_identifier', inplace=True) 
wikipedia_query.set_index('wikidata_identifier', inplace=True)


#Join the datasets 
combined_df = pd.concat([wikipedia_query, wikidata_query], axis=1)


combined_df['ID'] = combined_df.index
combined_df.to_csv('combined.csv')

#Drop any people that are included by both the wikipedia and wikidata queries
combined_df_unique = combined_df.drop_duplicates(subset = ['ID'])
#Yields a total of 12209 unique people

combined_df_unique.set_index(combined_df_unique['ID'], inplace = True)


combined_df_unique.to_csv('unique.csv')

###########################################################################################
#      SECTION 1 SUBTASK 2 TASK 1)i  - Collect place of birth from additional sources     #
###########################################################################################

#Define relevant functions 
def returning_attribute_from_wikidata_identifier(column, wdt, nrows):
    """
    General function that finds the wdt relationship passed in with a column of wikidata identifiers
    For example if wdt = 'P19' and the column was a column of wikidata identifiers for MPs 
    it would return a list of the places of birth of these MPs
    """

    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    final_results = pd.DataFrame(columns = ['person.value', 'attribute.value'])
    
    start = 0
    end = nrows
    total_considered = 0

    for i in range(len(column)//nrows):
        time.sleep(5)
        values = '{ '
        
    
        for i in range(start,end):
            wikidata_identifier = column[i]
            values += ' wd:' + wikidata_identifier + ' '
            total_considered += 1
        

        values += " }"

        sparql.setQuery("""
        SELECT ?person ?attribute  {
        VALUES ?person """ + values + """ 
        ?person wdt:""" + wdt +""" ?attribute. 
       }
        """) 
    
        
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        results_df = pd.json_normalize(results['results']['bindings'])
        
        final_results = final_results.append(results_df[['person.value', 'attribute.value']])
        start += 300
        end += 300
        
    
    final_results['person.value'] = final_results['person.value'].apply(extract_wikidata_identifier)
    final_results.set_index(final_results['person.value'], inplace = True)
    final_results = final_results.drop(columns=['person.value'])
    
    return final_results



def pattern_match_wikipedia_place_of_birth(text):
    """
    Function that uses pattern matchig to return the place of birth from a wikipedia infobox 
    given the text in the infobox

    """
    
    location = None
    
    
    if re.search('\d{4}[A-Z]{1}', text) is not None:
        
        start, end = re.search('\d{4}[A-Z]{1}', text).span()
        location = text[end-1:]
    
    elif ')' in text:
        
        location = text.split(')')[-1]
        
    elif '[1]' in text:
        if '[2]' in text: 
            
            location = text.split('[2]')[-1]
        
        else:
            location = text.split('[1]')[-1]
        
    elif re.search('[a-z]{1}[A-Z]{1}', text) is not None:
        start, end = re.search('[a-z]{1}[A-Z]{1}', text).span()
        location = text[:end]
        
        
    return location


def place_of_birth_from_table(url):
    """
    
    Function that returns the place of birth (if listed in infobox on wikipedia page), 
    if there is no place of birth or no info box it return None

    """
    

    if "https://en.wikipedia.org" not in url:
        url = "https://en.wikipedia.org" + url

    done = False
    
    while done == False:
        try:
            page = requests.get(url)
            done = True
        except:
            done = False

    
    soup = BeautifulSoup(page.text, 'html.parser')
        
    table = soup.find('table',{'class':'infobox vcard'})
        
        
    place_of_birth = None
    try:
        for tr in table.find_all('tr'):
    
            content = tr.text
    
            if 'Born' in content:
                #print(url)
                
                content = content.replace('Born', '')
               
                place_of_birth = pattern_match_wikipedia_place_of_birth(content)
        
        if place_of_birth == None:
            return place_of_birth
        
    
        if bool(re.search(r'\d', place_of_birth)):
   
            return None
      
        return place_of_birth
    
    except AttributeError:

        return place_of_birth



def return_wikitree_place_of_birth(wikitree_identifier):
    """
    Function that returns the place of birth of an MP (idenitifed by a unique Wikitree identifier)
    if it is listed on the wikitree website 

    """
    if math_wrapper_is_nan(wikitree_identifier):
        return np.nan 
    
    elif wikitree_identifier == None:
        return np.nan
    
    url = "https://www.wikitree.com/wiki/" + str(wikitree_identifier)    
    page = requests.get(url)

    time.sleep(3)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    subset_soup = soup.find("span", itemprop="birthPlace")
    
    place = soup_find_wrapper(subset_soup)
    
   
    
    
    return place 
 
  


def return_geni_place_of_birth(geni_identifier):
    """
    Function that returns the place of birth of an MP 
    (identified by a unique Geni idenitifer) if listed on the Geni website 

    """
    
    if math_wrapper_is_nan(geni_identifier):
        return np.nan 
    elif geni_identifier == None:
        return np.nan
    
    url = "https://www.geni.com/search?search_type=people&names=" + str(int(geni_identifier))
    page = requests.get(url)

    
    soup = BeautifulSoup(page.text, 'html.parser')
    
    subset_soup = soup.find("td", {"id": "birth_location"})
    
    full_address = soup_find_wrapper(subset_soup)
   
    return full_address
    
    
   
def return_rush_parliamentary_archive_place_of_birth(rush_parliamentary_identifier):
    """
    Function that returns the place of birth of an MP 
    (identified by a unique Rush Parliamentary Idenitifer) if listed on the Rush Parliamentary website

    """
    
    if math_wrapper_is_nan(rush_parliamentary_identifier):
        return np.nan 
    elif rush_parliamentary_identifier == None:
        return np.nan
    
    url = "https://membersafter1832.historyofparliamentonline.org/members/" + str(int(rush_parliamentary_identifier))
    page = requests.get(url)

    time.sleep(3)
    soup = BeautifulSoup(page.text, 'html.parser')
    df = soup.find_all("dl", {'class': 'row'})
    
    for ele in df:
        for (dt, dd) in zip(ele.find_all("dt"), ele.find_all("dd")):
            if 'Birth place' in dt.text:
                
                return dd.text
            
    
    return np.nan 


def return_wikipedia_from_wikidata(wikidata_identifier):
    """
    Function that returns a URL for the English wikipedia page associated
    with the wikidata identifier passed in 
    
    """
    url = "https://www.wikidata.org/wiki/" + wikidata_identifier
    page = requests.get(url, timeout = 5)
    soup = BeautifulSoup(page.text, 'html')
    sub_soup = soup.find_all('span', {'class':'wikibase-sitelinkview-page', 'lang':'en'})
    for soup in sub_soup:
        link = soup.find('a', href= True)['href']
        if 'https://en.wikipedia.org/wiki/' in link:
            return link
        
    return np.nan


def extract_text_from_wikipedia(wikipedia_url):
    """
    Function that extracts the text from a wikipedia article

    """
    
    page = requests.get(wikipedia_url)
    soup = BeautifulSoup(page.text, 'lxml')

    text = ''
    number_of_sentences = 0
    for paragraph in soup.find_all('p'):
        text += paragraph.text
        text += ' '
        if '.' in paragraph.text:
            number_of_sentences += 1
            
        if number_of_sentences > 4:
           
            break
    
    text = re.sub(r'\[.*?\]+', '', text)
    text = text.replace('\n', '')
    
    return text    


nlp = spacy.load('en_core_web_sm')
def parsing_the_dependency_tree(sentence):
    """
    Function that performs POS tagging by parsing the dependency tree of a sentance
    """
    doc = nlp(sentence)
    verbs = []
    for possible_subject in doc:
        if possible_subject.dep == prep and possible_subject.head.pos == VERB:
            if str(possible_subject.head) == 'born':
                verbs.append(possible_subject)
            
    if verbs == []:
        return verbs
    
    for possible_subject in doc:
        if possible_subject.dep == pobj and possible_subject.head.pos == ADP:
            if possible_subject.head == verbs[0]:
                verbs.append(possible_subject)
            
            
            
    return verbs[-1]  



def extract_place_of_birth_from_unstructured_text(wikipedia_url):
    """
    Function that extracts the place of birth from unstructured text

    """
    text = extract_text_from_wikipedia(wikipedia_url)
    
    sentences = re.split('(?<=[\.\?\!])\s*', text)
    born_synonyms = ['born', 'comes from', 'originates', 'originated']
    
    final_sentences = []
    for sentence in sentences: 
        for born_synonym in born_synonyms:
            if born_synonym in sentence:
                final_sentences.append(sentence)
                
    for sentence in final_sentences:
        place_of_birth = parsing_the_dependency_tree(sentence)
        
    return place_of_birth
        

#Now call these functions by first extracting relevant identifiers 

#Retrieve Parliamentary Rush Identifier, this is the entity that has the wikidata Rush Parliamentary Archive ID (P4471) relationship with the person 

df = returning_attribute_from_wikidata_identifier(combined_df_unique['ID'], nrows = 300, wdt = "P4471")
combined_df_unique = combined_df_unique.join(df)
combined_df_unique = combined_df_unique.rename(columns={'attribute.value': 'rush'})


#Retrieve Wikitree Identifier, this is the entity that has the wikidata Wikitree ID (P2929) relationship with the person 
df = returning_attribute_from_wikidata_identifier(combined_df_unique['ID'], nrows = 300, wdt = "P2949")
combined_df_unique = combined_df_unique.join(df)
combined_df_unique = combined_df_unique.rename(columns={'attribute.value': 'wikitree'})

#Retrieve Geni Identifier, this is the entity that has the wikidata Geni ID (P2600) relationship with the person 
df = returning_attribute_from_wikidata_identifier(combined_df_unique['ID'], nrows = 300, wdt = "P2600")
combined_df_unique = combined_df_unique.join(df)
combined_df_unique = combined_df_unique.rename(columns={'attribute.value': 'geni'})

#Return place of birth from these websites:

#Place of birth from Parliamentary Rush Website. The page has been found using the Parliamentary Rush ID found previously
combined_df_unique['rush_place_of_birth'] = combined_df_unique['rush'].apply(return_rush_parliamentary_archive_place_of_birth)


#Place of birth from Wikitree Website. The page has been found using the Wikitree ID found previously
combined_df_unique['wikitree_place_of_birth'] = combined_df_unique['wikitree'].apply(return_wikitree_place_of_birth)


#Place of birth from Geni Website. The page has been found using the Geni ID found previously
combined_df_unique['geni_place_of_birth'] = combined_df_unique['geni'].apply(return_geni_place_of_birth)


#Get wikipedia link associated with the person from the wikidata page. This link will be the unique to the person
combined_df_unique['wikipedia'] = combined_df_unique.apply(lambda x: return_wikipedia_from_wikidata(x['ID']) if(pd.isnull(x['links'])) else np.nan, axis = 1)
combined_df_unique['wikipedia'].fillna(combined_df_unique['links'], inplace=True)


#If the wikipedia page contains an infobox, look for the place of birth there 
combined_df_unique['wikipedia_place_of_birth'] = combined_df_unique.apply(lambda x: place_of_birth_from_table(x['wikipedia']) if(pd.notnull(x['wikipedia'])) else np.nan, axis = 1)


#Need to set wikidata_place_of_birth column 
#birthPlace.value
if 'placeOfBirth.value' in list(combined_df_unique.columns):
    combined_df_unique = combined_df_unique.rename(columns={'placeOfBirth.value': 'wikidata_place_of_birth'})




##########################################################################
#     SECTION 1 SUBTASK 2 TASK 2)i  - Process place of birth data        #
##########################################################################


#Define relevant functions 
def wikidata_search_irish_county(wikidata_identifier):
    """
    Function that returns the Irish historical county that a place is in (if applicable)
    (the county that has the wikidata relationship P7959 with the wikidata identifier passed in)
    
    """
    
    
    query_string = """
        SELECT ?county ?countyLabel 
        { wd:""" + str(wikidata_identifier) + """ wdt:P7959 ?county .  
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". } 
       }
    """
    
    done = False
    
    while done == False:
        try:
            sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
            sparql.setQuery(query_string) 
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            
            
            done = True
        
        except HTTPError:
            time.sleep(10)
            

    results_df = pd.json_normalize(results['results']['bindings'])
    if results_df.empty: 
        
        
        return None
    
    return results_df['countyLabel.value'].iloc[0]




def find_ireland(place_str):
    """ 
    Function that searches wikipedia for a specific Irish loction 
    and returns the tuple (county, whether Ireland or Northern Ireland, UK) if the place is found
    else it return (None, None, None)
    
    """
    
    

    done = False
    while done == False:
        try: 
            results = wikipedia.search(place_str, results = 5)
        
            done = True
        
        except ConnectionError:
            time.sleep(10)
            
    
    #Search for county if not results
    if len(results) == 0: 
        for place in place_str: 
            if 'County' or 'county' in place:
                results = wikipedia.search(place, results = 5)
                break
    
    if len(results) == 0:
        return None, None, None

    try: 
        url = wikipedia.page(results[0]).url
    
    except PageError:
        return None, None, None
    
    except DisambiguationError:
        return None, None, None
    
    except: 
        return None, None, None
    
    wikidata_url = return_wikidata_identifier_from_wikipedia_page(url)
    if wikidata_url == None:
        return None, None, None
    
    place_str = wikidata_url.split('/')[-1]
    county = wikidata_search_irish_county(place_str)
    
    if 'County' in county:
        county = county.replace('County', '')
        county = county.strip()


    northern_Ireland_counties = ['Antrim', 'Armagh', 'Down', 'Fermanagh', 'Londonderry', 'Tyrone', 'Derry']
    Ireland_counties = ['Cork', 'Galway', 'Mayo', 'Donegal', 'Kerry', 'Tipperary', 'Clare', 'Tyrone', 'Antrim', 'Limerick', 'Roscommon', 'Down',
                        'Wexford', 'Meath', 'Londonderry', 'Kilkenny', 'Wicklow', 'Offaly', 'Cavan', 'Waterford', 'Westmeath', 'Sligo', 'Laois', 'Kildare', 'Fermanagh',
                        'Leitrim', 'Armagh', 'Monaghan', 'Longford', 'Dublin', 'Carlow', 'Louth']

    if county in northern_Ireland_counties:
        ireland = 'Northern Ireland'
        country = 'UK'
    
    elif county in Ireland_counties: 
        ireland = None
        country = 'Republic of Ireland'
        county = None
        
    else: 
        return None, None, None
        
    return county, ireland, country



def return_historic_county(place, place_broken_up, col = 'place20nm', df = index_of_places):
    """
    Function that returns the historical county and British country (Wales, England, Scotland) of the place passed in 
    by quering the Index of Places database for the place passed in
    
    """
    
    
    #Find place in the index of places
    filtered_df = df.loc[df[col] == place]
    
   
    #If more than one exact match, then use the other columns and the rest of place to see which match the best
    if len(filtered_df) > 1:
    
        if len(place_broken_up) > 2:
            place_broken_up.remove(place)
            
            if place in place_broken_up:
                print(True)
            
        
            for place in place_broken_up: 
                try: 
                    
                    if place != 'United Kingdom' and place != 'UK': 
                        
                        filtered_df_ = filtered_df[filtered_df.apply(lambda r: r.str.contains(place, case=False, na = None).any(), axis=1)] 
                        if len(filtered_df_) != 0:
                            filtered_df = filtered_df_
                except: 
                
                    return None, None
    
    #If still more, take the one with most information that describes the largest place 
    if len(filtered_df) > 1:
        
        
        filtered_df_ = filtered_df[filtered_df['ctyhistnm'].notna()]
        
        if filtered_df_.empty:
            try: 
                filtered_df = filtered_df[filtered_df['descnm'] == 'CTYHIST']
                return filtered_df['place20nm'].iloc[0], filtered_df['ctry20nm'].iloc[0]
            except IndexError:
                pass
                
        
        df_nan = filtered_df.isnull().sum(axis=1)
        minimum = df_nan.min() 
        
        index = df_nan.loc[df_nan == minimum].index
        filtered_df = filtered_df.loc[index]
        
      
        

    if len(filtered_df) > 1: 

        lst_to_sort_by = ['BUA', 'BUASD', 'CA', 'CED', 'COM', 'CTY', 'CTYHIST', 'CTYLT', 'LOC', 'LONB', 'MD', 'NMD', 'NPARK', 'PAR', 'RGN', 'UA', 'WD']

        filtered_df.descnm = filtered_df.descnm.astype("category")
        filtered_df.descnm.cat.set_categories(lst_to_sort_by, inplace=True)
        filtered_df = filtered_df.iloc[0]


    if len(filtered_df) == 0:
   
        return None, None
        
    try: 
        return filtered_df.iloc[0]['ctyhistnm'], filtered_df.iloc[0]['ctry20nm']
    
    except IndexError:
        return filtered_df['ctyhistnm'], filtered_df['ctry20nm']
    
def search_col(place, col = 'place20nm', df = index_of_places):
    """
    Function that checks whether the place passed in, is in the index of places 
    """
    if df[col].astype(str).str.contains(place).any():
        return True
    
    else:
        return False   
    
def process_places(place):
    """
    Function that returns the tuple: 
        historic county - if applicable else None
        the UK country if applicable (i.e. England, Scotland, Wales, Northern Ireland)  else None 
        the country (e.g. Italy)

    """
    #Returns Index in index of places if applicable (i.e. in Great Britain) else None
    #        the UK country if applicable (i.e. England, Scotland, Wales, Northern Ireland)  else None 
    #       the country (e.g. Italy)
    
    if place == None or math_wrapper_is_nan(place):
        return None, None, None 
    
    #Process the place
    
    place_broken_up = place.split(',')
    for i in range(len(place_broken_up)):
        place_broken_up[i] = place_broken_up[i].strip()

    
    countries = dict(countries_for_language('en'))
    list_of_countries = list(countries.values())
    list_of_countries.remove('United Kingdom')
    
    
    for place in place_broken_up: 
        #Check if international country (so not included in index of places)
        if place in list_of_countries:
            return None, None, place
        
        for count in list_of_countries:
            if count in place:
                return None, None, count
        
        #Check if Ireland (so not included in index of places)
        if place == 'Ireland':
            return find_ireland(" ".join(place_broken_up))
            
    
    if len(place_broken_up) == 1:
        if place_broken_up[0] == 'England' or place_broken_up[0] == 'Scotland' or place_broken_up[0] == 'Wales':
            return None, place_broken_up[0], 'UK'
            
        elif place_broken_up[0] == 'UK' or place_broken_up[0] == 'United Kingdom':
            return None, None, 'UK'
            
        else:
            print('len is 1 and not identified', place_broken_up)
                
                
    if 'England' in place_broken_up[0]:
            return None, 'England', 'UK'
        
    elif 'Scotland' in place_broken_up[0]:
        return None, 'Scotland', 'UK'
    
    elif 'Wales' in place_broken_up[0]:
        return None, 'Wales', 'UK'
            
       
    else:
        
        place_to_look_for = None
        for place in place_broken_up:
                
            #Search the place20nm column to find if in index of places
            if search_col(place):
                
                index = place_broken_up.index(place)
                place_broken_up = place_broken_up[index:]
                place_to_look_for = place
                
                break
        
        #If not found sometimes the place will be in Ireland and unlabelled as Ireland so check for this
        if place_to_look_for == None:
            
            place_to_look_for = find_ireland(" ".join(place_broken_up))
            if place_to_look_for == None:
                return None, None, None
            else: 
                return place_to_look_for
        
        else:
            
            historic, country = return_historic_county(place_to_look_for, place_broken_up)
            
            return historic, country, 'UK'




def apply_function_in_sections(df, f, col_to_apply_to, string, section_size = 200):
    """
    
    Function that applies a function f in sections to a column in a dataframe by breaking the column into sections 

    """
    
    start = 0 
    end = section_size
    columns = list(df.columns)
    
    print(len(df), 'df')
    county_str, country_UK_str, country_str = 'county_' + string, 'country_UK_' + string, 'country_' + string
    columns.append(county_str)
    columns.append(country_UK_str)
    columns.append(country_str)
    
    end_df = pd.DataFrame()
    df.reset_index(drop=True, inplace=True)
    
    for i in range(math.ceil((len(df)+1)/section_size)):
        
        section_of_df = df[start:end]
        section_of_df['temp'] = section_of_df[col_to_apply_to].apply(extract_wikidata_identifier)
        temp_df = section_of_df['temp'].apply(pd.Series)
        temp_df.columns = ['a', 'b', 'c']
        section_of_df['county_' + string], section_of_df['country_UK_' + string], section_of_df['country_' + string] = temp_df['a'], temp_df['b'], temp_df['c']
        section_of_df.drop(columns = ['temp'], inplace = True)
        
        start += section_size
        end += section_size
    
        section_of_df = section_of_df.reset_index(drop = True)
        end_df = end_df.reset_index(drop = True)
      
        end_df = end_df.append(section_of_df)

       
    
    return end_df 




#combined_df_unique.drop(columns = ['birthPlace.value'], inplace=True)

#combined_df_unique = combined_df_unique.rename(columns = {'attribute.value':'birthPlace.value'})

combined_df_unique.drop_duplicates(subset = ['ID'], inplace = True)
combined_df_unique.set_index(combined_df_unique['ID'], inplace= True)


combined_df_unique = apply_function_in_sections(combined_df_unique, process_places, 'wikitree_place_of_birth', 'wikitree')
combined_df_unique =  apply_function_in_sections(combined_df_unique, process_places, 'geni_place_of_birth', 'geni')
combined_df_unique = apply_function_in_sections(combined_df_unique, process_places, 'rush_place_of_birth', 'rush')
combined_df_unique = apply_function_in_sections(combined_df_unique, process_places, 'wikipedia_place_of_birth', 'wikipedia')
combined_df_unique = apply_function_in_sections(combined_df_unique, process_places, 'wikidata_place_of_birth', 'wikidata')
combined_df_unique.to_csv('processed_places_combined_df.csv')



##########################################################################
#   SECTION 1 SUBTASK 3 TASK 1)i  - Take mode of place of birth data     #
##########################################################################

all_data = pd.read_csv('combined.csv')
unique_people = pd.read_csv('processed_places_combined_df.csv')

#Define relevant functions
def mode_county(row):
    """
    Function that returns the most common county in a row (from the Wikitree county, Wikidata county, 
                                                           Wikipedia county, Geni county and 
                                                           Parliamentary Rush county)
    """
    
    subset_of_row = row[['county_wikitree','county_wikidata','county_wikipedia','county_rush','county_geni']]
    mode = list(subset_of_row.mode())
    
    if len(mode) > 1:
        if row['county_wikidata'] in mode: 
            return row['county_wikidata']
        
        elif row['county_wikipedia'] in mode:
            return row['county_wikipedia']
        
        elif row['county_wikitree'] in mode:
            return row['county_wikitree']
        
        
    if mode == []:
        return None
    
    return mode[0]
        


def mode_country(row):
    """
     Function that returns the most common country in a row (from the Wikitree country, Wikidata country, 
                                                           Wikipedia country, Geni country and 
                                                           Parliamentary Rush country)
    """
    
    subset_of_row = row[['country_wikitree','country_wikidata','country_wikipedia','country_rush','country_geni']]
    mode = list(subset_of_row.mode())
    
    if len(mode) > 1:
        if row['country_wikidata'] in mode: 
            return row['country_wikidata']
        
        elif row['country_wikipedia'] in mode:
            return row['country_wikipedia']
        
        elif row['country_wikitree'] in mode:
            return row['country_wikitree']
        
    if mode == []:
        return None   

    return mode[0]
    

def mode_UK_country(row):
    """
    Function that returns the most common UK country in a row (from the Wikitree UK country, Wikidata UK country, 
                                                           Wikipedia UK country, Geni UK country and 
                                                           Parliamentary Rush UK country)
    """
    
    subset_of_row = row[['country_UK_wikitree','country_UK_wikidata','country_UK_wikipedia','country_UK_rush','country_UK_geni']]
    mode = list(subset_of_row.mode())
    
    if len(mode) > 1:
        if row['country_UK_wikidata'] in mode: 
            return row['country_UK_wikidata']
        
        elif row['country_UK_wikipedia'] in mode:
            return row['country_UK_wikipedia']
        
        elif row['country_UK_wikitree'] in mode:
            return row['country_UK_wikitree']
        
    if mode == []:
        return None    
        
    return mode[0]


def return_county(row, unique_people = unique_people):
  """
    Function that returns the place of birth county for an MP (given a unique identifier)

  """
  row_for_person = unique_people[unique_people['ID'] == row['person.value']]
  
      
  if row_for_person.empty:
      return None 
  
  place = row_for_person['county'].iloc[0]
      
  
  
  return place

    

def return_country(row, unique_people = unique_people):
  """
  Function that returns the place of birth country for an MP (given a unique identifier)
  
  """
  row_for_person = unique_people[unique_people['ID'] == row['person.value']]
  
  if row_for_person.empty:
      return None 

  place = row_for_person['country'].iloc[0]
  
  return place



def return_UK_country(row, unique_people = unique_people):
  """
  Function that returns the place of birth country (UK) for an MP (given a unique identifier) 

  """
  
  row_for_person = unique_people[unique_people['ID'] == row['person.value']]
  
  if row_for_person.empty:
      return None 

  place = row_for_person['UK_country'].iloc[0]

  return place


unique_people['county'] = unique_people.apply(mode_county, axis = 1)
unique_people['country'] = unique_people.apply(mode_country, axis = 1) 
unique_people['UK_country'] = unique_people.apply(mode_UK_country, axis =1)  

unique_people.to_csv('unique_people_mode_taken.csv')


#Now use the dataset on unique MPs to fill out all data (the unique people dataset contains data on unique people but the all data contains a list of all MPs in all Parliaments - even if they served more than once) 

all_data['county'] = all_data.apply(return_county, axis = 1)
all_data['country'] = all_data.apply(return_country, axis = 1)
all_data['UK_country'] = all_data.apply(return_UK_country, axis = 1)


all_data.to_csv('all_data_mode_taken.csv')


all_data = pd.read_csv('all_data_mode_taken.csv')
unique_people = pd.read_csv('unique_people_mode_taken.csv')



#####################################################################################################
#     SECTION 1 SUBTASK 3 TASK 1)i  - Extract British Population Data from Vision of Britian        #
#####################################################################################################

#Define relevant functions 
def remove_commas_from_lst(lst):
    """
    Function to remove commas from list 

    """
    
    for i in range(len(lst)):
        if ',' in lst[i]:
            lst[i] = lst[i].replace(',', '')
            lst[i] = float(lst[i])
            
    return lst 

def extract_regions(url):
    """
    Extract the hyperlinks to the different regions included from the Vision of Britain website 

    """
    
    page = requests.get(url, verify=False)
    soup = BeautifulSoup(page.text, "lxml")
    #print(soup)
    area_links = []
    for area in soup.find_all("area"):
        #print(area)
        if area['href'] not in area_links:
            area_links.append(area['href'])
    
    return area_links

def extract_population(url):
    """
    Function that extracts the population listed on the page that the URL passed links to 

    """
    
    page = requests.get(url, verify=False)
    soup = BeautifulSoup(page.text, "lxml")
    
    subsoup = soup.find_all("td", {"class":"number"})
    population = []
    for thi in subsoup:
        processed = str(thi.text).replace('\n', '').replace('\t', '').replace(' ', '')
        population.append(processed)
        
    return population 

def extract_years(url):
    """
    Function that extracts the years the population data has been recorded for 
    which is listed on the page that the URL passed links to 
    """
    page = requests.get(url, verify=False)
    soup = BeautifulSoup(page.text, "lxml")
    years = []
    for td in soup.find_all("td"):
        
        if re.match(r"[0-9]{4}$", td.text) is not None:
            years.append(td.text)
    
    return years 

def construct_url(area_link):
    """
    Function that appends the area link to Vision of Britian link to create a URL that accesses the 
    area page on the Vision of Britian website

    """
    link = "https://www.visionofbritain.org.uk" + str(area_link)
    
    return link 

def append_code(link, code = '/cube/TOT_POP'):
    """
    Function that returns the URL to the population page of a specific area 

    """
    
    link = "https://www.visionofbritain.org.uk" + link + code 
    
    return link 

def convert_list_of_string_to_int(lst):
    """
    Function that converts a list of strings to a list of ints

    """
    for i in range(len(lst)):
        if i != '':
            lst[i] = int(lst[i])
    
    return lst

def return_place(url):
    """
    Function that returns the string of the place of the page that the url links to
    """
    
    page = requests.get(url, verify=False)
    soup = BeautifulSoup(page.text, "lxml")
    
    
    
    subsoup = soup.find("div", {'class':'row', 'id': 'breadcrumbs'})
    
    place = []
    subsoup = subsoup.find_all('li')
    
    for line in subsoup:
        
        try: 
            link =  line.find('a', href = True).text
            
            
            if 'Home' not in link and link != '':
                place.append(link)
        
        except AttributeError:
            pass
        
    
    return '/'.join(place)


    
def check_dates_and_pop(years, pop):
    """
    Function that interpolates or extrapolates any missing data
    """
    
    dates = [1801, 1811, 1821, 1831, 1841, 1851, 1861, 1871, 1881, 1891, 1901, 1911, 1921, 1931, 1941, 1951, 1961, 1971, 1981, 1991, 2001, 2011, 2021]
    
    years_, pop_ = zip(*((x, y) for x, y in zip(years, pop) if y != ''))
    
    years_ = list(years_)
    pop_ = list(pop_)

    years_ = convert_list_of_string_to_int(years_)
    pop_ = convert_list_of_string_to_int(pop_)
    years = list(OrderedDict.fromkeys(years))
    years.append(2021.0)
    pop.append('')
    
    if len(pop) != len(dates):
        for i in range(len(dates)): 
            
            if years[i] != dates[i]:
                pop.insert(i, '')
                years.insert(i, dates[i])
    
    
    #Interpolate function 
    f_i = interpolate.interp1d(years_, pop_, kind='linear')
    
    #Extrapolate function 
    fit = np.polyfit(years_, pop_ , 3) #The use of 1 signifies a linear fit.
    f_e = np.poly1d(fit)
    
    
    
    
    for i in range(len(pop)): 
        if pop[i] == '':
            
            #Try to interpolate
            try:
                
                years[i] = float(years[i])
     
                pop[i] = f_i(years[i])
                if pop[i] < 0: 
                    pop[i] = pop[i-1]
            
            #Else extrapolate    
            except ValueError:
                pop[i] = f_e(years[i])
    
   
    
    population_end, year_end = [], []
    for i in range(1800, 2021):
        if (i - 1)%10 == 0:
            j = int((i - 1)/10 - 180)
            population_end.append(pop[j])
            year_end.append(i)
            
        else:
            try:
                p = f_i(i)
                if p < 0: 
                    p = population_end[i-1800 - 1]
                population_end.append(p)
                year_end.append(i)
                
            #Else extrapolate    
            except ValueError:
                p = f_e(i)
                if p < 0: 
                    p = 0
                population_end.append(p)
                year_end.append(i) 
            
    return population_end, year_end
    

def return_population_from_vision_of_Britain():
    """
    Function that extracts the relevant population data from the Vision of Britian website
    
    """
    regions_lst = []
    
    df = pd.DataFrame(index = list(range(1800, 2021)))
    regions = extract_regions("https://www.visionofbritain.org.uk/")

    for region in regions:
        
        link = construct_url(region)
        sub_regions = extract_regions(link)
        
        
        for sub_region in sub_regions: 
            
            regions_lst.append(sub_region)
            
            url = append_code(sub_region)
            
            population = extract_population(url)
            years = extract_years(url)
            
            years = [float(i) for i in years]
            population = remove_commas_from_lst(population)
            
            population_10_years_earlier = population[::2]
            current_total_population = population[1::2]
            
            url = "https://www.visionofbritain.org.uk" + sub_region
            place = return_place(url)
            

            current_total_population, years = check_dates_and_pop(years, current_total_population)
            
          

            df[place + '_current'] = current_total_population
    url = "https://www.visionofbritain.org.uk" + '/unit/10168326'
    
    
    place = return_place(url)
    current_total_population, years = check_dates_and_pop(years, current_total_population)
    df['Shetland Islands' + '_current'] = current_total_population       
                
    
    df.to_csv('vision_of_britain_pop_linear.csv')
    return regions_lst 





def process_columns(column):
    """
    Function that processes the column names for the Vision Of Britian dataset

    """
    
    if column == 'Britain/England/Herefordshire/Herefordshire UA/County_current':
        return 'Herefordshire'
    
    column = column.split('/')[-1]
    if 'District_current' in column:
        column = column.replace('District_current', '')
    
    if 'UA_current' in column:
        column = column.replace('UA_current', '')
    
    if 'UA' in column:
        column = column.replace('UA', '')
        
    if 'City_current' in column:
        column = column.replace('City_current', '')
        
    column = column.strip()
    
 
        
    return column
 



def historical_county(place):
    """
    Function that finds the historical county of a place using the index of places 
    """
    
    filtered_df = index_of_places.loc[index_of_places['place20nm'] == place] 
    county = filtered_df['ctyhistnm'].mode()
 
    try: 
        return county.values[0]
    
    except IndexError:
        
        return None
    
    


return_population_from_vision_of_Britain()


#Now process the vision of Britain and align with Historical counties 
vision_of_britain = pd.read_csv('vision_of_britain_pop_linear.csv')
vision_of_britain.rename({'Britain/England/Gloucestershire/Bristol/Bristol UA/City_current': 'Britain/England/Gloucestershire/Bristol/Bristol UA/City_current Bristol'}, inplace = True)
lst_of_columns = list(vision_of_britain.columns)     
new_list = [process_columns(x) for x in lst_of_columns]
new_list[174] = 'Bristol'
vision_of_britain.columns = new_list
new_list = ['Arun', 'Rushmoor', 'Oxford', 'Windsor and Maidenhead', 'East Hampshire', 'the Isle of Wight', 'Mid Sussex', 'Chiltern', 'Dover', 'Wokingham', 'Eastbourne', 'Test Valley', 'South Bucks', 'Vale of White Horse', 'Mole Valley', 'Worthing', 'Horsham', 'Crawley', 'Brighton and Hove', 'Swale', 'Rother', 'Reigate and Banstead', 'Basingstoke and Deane', 'Gosport', 'Waverley', 'Portsmouth', 'Aylesbury Vale', 'Hart', 'Slough', 'Maidstone', 'Medway', 'Sevenoaks', 'Havant', 'Thanet', 'Guildford', 'Lewes', 'Reading', 'Eastleigh', 'Tandridge', 'Surrey Heath', 'West Berkshire', 'West Oxfordshire', 'Milton Keynes', 'Fareham', 'Elmbridge', 'Cherwell', 'Wycombe', 'Canterbury', 'Tonbridge and Malling', 'Chichester', 'Wealden', 'Epsom and Ewell', 'Southampton', 'New Forest', 'Shepway', 'Hastings', 'Bracknell Forest', 'Runnymede', 'South Oxfordshire', 'Adur', 'Spelthorne', 'Woking', 'Ashford', 'Winchester', 'Dartford', 'Tunbridge Wells', 'Gravesham', 'Lewisham', 'Barking and Dagenham', 'Harrow', 'Havering', 'Hackney', 'Hillingdon', 'Enfield', 'Islington', 'Barnet', 'Redbridge', 'City of London', 'Ealing', 'Wandsworth', 'Kingston upon Thames', 'Newham', 'Greenwich', 'Southwark', 'Waltham Forest', 'Bexley', 'Camden', 'Lambeth', 'Tower Hamlets', 'Haringey', 'Hammersmith and Fulham', 'Brent', 'Richmond upon Thames', 'Merton', 'Sutton', 'Bromley', 'Hounslow', 'Westminster', 'Croydon', 'Kensington and Chelsea', 'Watford', 'East Hertfordshire', 'Bedford', 'St Edmundsbury', 'Rochford', 'Tendring', 'South Cambridgeshire', 'East Cambridgeshire', 'Harlow', 'Stevenage', 'Broadland', 'Suffolk Coastal', 'Fenland', 'Thurrock', 'Uttlesford', 'Mid Suffolk', 'Braintree', 'Welwyn Hatfield', 'Forest Heath', 'Cambridge', 'Babergh', 'Kings Lynn and West Norfolk', 'South Norfolk', 'North Norfolk', 'Basildon', 'Castle Point', 'Norwich', 'Central Bedfordshire', 'Huntingdonshire', 'St Albans', 'Great Yarmouth', 'Epping Forest', 'Breckland', 'Maldon', 'Dacorum', 'Ipswich', 'Chelmsford', 'Colchester', 'Hertsmere', 'Broxbourne', 'Brentwood', 'Luton', 'Three Rivers', 'Southend on Sea', 'Peterborough', 'Waveney', 'North Hertfordshire', 'North Dorset', 'South Gloucestershire', 'Torridge', 'Purbeck', 'North Somerset', 'Taunton Deane', 'Poole', 'South Hams', 'Cheltenham', 'Torbay', 'Wiltshire', 'Mid Devon', 'Bournemouth', 'Teignbridge', 'Weymouth and Portland', 'Cotswold', 'Gloucester', 'Christchurch', 'Bath and North East Somerset', 'West Devon', 'East Dorset', 'North Devon', 'West Somerset', 'Plymouth', 'South Somerset', 'Forest of Dean', '', 'Swindon', 'East Devon', 'Stroud', 'Sedgemoor', 'Mendip', 'Tewkesbury', 'Cornwall', 'Exeter', 'West Dorset', 'Dundee City', 'North Ayrshire', 'Scottish Borders', 'Glasgow', 'North Lanarkshire', 'Aberdeenshire', 'Orkney Islands', 'Argyll and Bute', 'Fife', 'Aberdeen City', 'Highland', 'East Dunbartonshire', 'Angus', 'Inverclyde', 'East Ayrshire', 'Clackmannanshire', 'Midlothian', 'Edinburgh', 'East Lothian', 'Dumfries and Galloway', 'East Renfrewshire', 'South Ayrshire', 'Na H Eileanan An Iar', 'Stirling', 'Moray', 'West Lothian', 'South Lanarkshire', 'West Dunbartonshire', 'Renfrewshire', 'Perth and Kinross', 'Falkirk', 'Pendle', 'Bury', 'Copeland', 'Lancaster', 'Rossendale', 'Barrow in Furness', 'Warrington', 'Salford', 'Bolton', 'Knowsley', 'Wigan', 'Liverpool', 'Wyre', 'Rochdale', 'Preston', 'Halton', 'Fylde', 'Hyndburn', 'South Lakeland', 'Cheshire East', 'Blackpool', 'Trafford', 'Eden', 'West Lancashire', 'St Helens', 'Tameside', 'Chorley', 'Allerdale', 'Ribble Valley', 'Cheshire West and Chester', 'Oldham', 'Wirral', 'Sefton', 'Manchester', 'Stockport', 'Blackburn With Darwen', 'Burnley', 'South Ribble', 'Carlisle', 'North Tyneside', 'County Durham', 'Sunderland', 'Hartlepool', 'Newcastle upon Tyne', 'Darlington', 'Middlesbrough', 'Redcar and Cleveland', 'Gateshead', 'Stockton on Tees', 'South Tyneside', 'Northumberland', 'Telford and Wrekin', 'Worcester', 'Lichfield', 'North Warwickshire', 'Stafford', 'Sandwell', 'Wyre Forest', 'East Staffordshire', 'Bromsgrove', 'South Staffordshire', 'Wychavon', 'Newcastle under Lyme', 'Wolverhampton', 'Coventry', 'Cannock Chase', 'Warwick', 'Stratford on Avon', 'Birmingham', 'Staffordshire Moorlands', 'Dudley', 'Nuneaton and Bedworth', 'Malvern Hills', 'Redditch', 'Solihull', 'Shropshire', 'County_current', 'Stoke on Trent', 'Tamworth', 'Walsall', 'Rugby', 'Denbighshire', 'Merthyr Tydfil', 'the Isle of Anglesey', 'Conwy', 'Newport', 'Rhondda; Cynon; Taff', 'Bridgend', 'The Vale of Glamorgan', 'Swansea', 'Torfaen', 'Wrexham', 'Carmarthenshire', 'Cardiff', 'Caerphilly', 'Blaenau Gwent', 'Gwynedd', 'Flintshire', 'Powys', 'Neath Port Talbot', 'Ceredigion', 'Monmouthshire', 'Pembrokeshire', 'South Kesteven', 'North West Leicestershire', 'Amber Valley', 'Hinckley and Bosworth', 'Blaby', 'Melton', 'Daventry', 'Northampton', 'Boston', 'Derby', 'South Northamptonshire', 'Newark and Sherwood', 'Chesterfield', 'Rushcliffe', 'East Northamptonshire', 'Rutland', 'West Lindsey', 'Erewash', 'South Holland', 'Lincoln', 'East Lindsey', 'Derbyshire Dales', 'Kettering', 'Bassetlaw', 'Oadby and Wigston', 'Charnwood', 'South Derbyshire', 'Bolsover', 'North East Derbyshire', 'Harborough', 'Leicester', 'Corby', 'North Kesteven', 'High Peak', 'Ashfield', 'Gedling', 'Nottingham', 'Mansfield', 'Wellingborough', 'Broxtowe', 'Craven', 'York', 'Barnsley', 'North Lincolnshire', 'Doncaster', 'Calderdale', 'Kirklees', 'Rotherham', 'Ryedale', 'East Riding of Yorkshire', 'Scarborough', 'Bradford', 'Sheffield', 'Richmondshire', 'Hambleton', '', 'Selby', 'Harrogate', 'North East Lincolnshire', 'Leeds', 'Wakefield']
new_list_ = [historical_county(place) for place in new_list]

#Set specific historical county
new_list_[5] = 'Hampshire'
new_list_[7] = 'Buckinghamshire'
new_list_[12] = 'Buckinghamshire'
new_list_[26] = 'Buckinghamshire'
new_list_[46] = 'Buckinghamshire'
new_list_[103] = 'Suffolk'
new_list_[111] = 'Suffolk'
new_list_[118] = 'Suffolk'
new_list_[121] = 'Norfolk'
new_list_[143] = 'Essex'
new_list_[145] = 'Suffolk'
new_list_[147] = 'Dorset'
new_list_[150] = 'Dorset'
new_list_[152] = 'Somerset'
new_list_[161] = 'Dorset'
new_list_[167] = 'Dorset'
new_list_[169] = 'Somerset'
new_list_[173] = 'Gloucestershire'
new_list_[182] = 'Dorset'
new_list_[205] = 'Ayrshire'
new_list_[219] = 'Lancashire'
new_list_[249] = 'Lancashire'
new_list_[262] = 'Durham'
new_list_[276] = 'Staffordshire'
new_list_[281] = 'Warwickshire'
new_list_[290] = 'Shropshire'
new_list_[291] = 'Staffordshire'
new_list_[297] = 'Anglesey'
new_list_[300] = 'Anglesey'
new_list_[302] = 'Glamorgan'
new_list_[372] = 'Yorkshire'



lst_of_columns = list(vision_of_britain.columns)
if 'Unnamed: 0' in lst_of_columns:
    lst_of_columns.remove('Unnamed: 0')
lst_of_columns[173] = 'Bristol'
list_of_counties = list(index_of_places['ctyhistnm'].unique())
list_of_dates = list(range(1800,2021))
df = pd.DataFrame(0, columns = list_of_counties, index = list_of_dates)
lst_to_remove = ['Moray', 'Stirling', 'Herefordshire', 'Edinburgh', 'West Lothian', 'Dumfries and Galloway', 'Scottish Borders', 'Powys', 'Midlothian', 'Falkirk', 'Na H Eileanan An Iar', 'Renfrewshire', 'Inverclyde', 'Glasgow', 'North Lanarkshire', 'South Lanarkshire',  'Highland', 'Aberdeenshire', 'Aberdeen City', 'Perth and Kinross', 'Argyll and Bute', 'East Dunbartonshire', 'West Dunbartonshire']


#Divide population in regions that do not map naturally to historic counties by surface area
for t in range(len(vision_of_britain)):
    df['Morayshire'].iloc[t] = 1233/(1233 + 1660) * vision_of_britain['Moray'].iloc[t]
    df['Banffshire'].iloc[t] = 1660/(1233 + 1660) * vision_of_britain['Moray'].iloc[t]
    df['Ross-shire'].iloc[t] = 8000/(8000 + 10906) * vision_of_britain['Na H Eileanan An Iar'].iloc[t] + 8000/(10906 + 8000 + 1601 + 581 + 5252)  * vision_of_britain['Highland'].iloc[t] 
    df['Inverness-shire'].iloc[t] = 10906/(8000 + 10906) * vision_of_britain['Na H Eileanan An Iar'].iloc[t] + 10906/(10906 + 8000 + 1601 + 581 + 5252)  * vision_of_britain['Highland'].iloc[t]
    df['Caithness'].iloc[t] = 1601/(10906 + 8000 + 1601 + 581 + 5252 + 958) * vision_of_britain['Highland'].iloc[t]
    df['Nairnshire'].iloc[t] =  581/(10906 + 8000 + 1601 + 581 + 5252 + 958) * vision_of_britain['Highland'].iloc[t]
    df['Sutherland'].iloc[t] = 5252/(10906 + 8000 + 1601 + 581 + 5252 + 958) * vision_of_britain['Highland'].iloc[t]
    df['Cromartyshire'].iloc[t] = 958/(10906 + 8000 + 1601 + 581 + 5252 + 958) * vision_of_britain['Highland'].iloc[t]
    df['West Lothian'].iloc[t] = vision_of_britain['West Lothian'].iloc[t]
    df['Herefordshire'].iloc[t] = vision_of_britain['Herefordshire'].iloc[t]
    df['Aberdeenshire'].iloc[t] = 5050/(5050 + 984) * (vision_of_britain['Aberdeenshire'].iloc[t] + vision_of_britain['Aberdeen City'].iloc[t])
    df['Kincardineshire'].iloc[t] = 984/(5050 + 984) * (vision_of_britain['Aberdeenshire'].iloc[t] + vision_of_britain['Aberdeen City'].iloc[t])
    df['Perthshire'].iloc[t] = 5286/(5286 + 189) * vision_of_britain['Perth and Kinross'].iloc[t]
    df['Kinross-shire'].iloc[t] = 189/(5286 + 189) * vision_of_britain['Perth and Kinross'].iloc[t]
    df['Argyllshire'].iloc[t] = 2924/(2924 + 583) * vision_of_britain['Argyll and Bute'].iloc[t]
    df['Buteshire'].iloc[t] = 584/(2924 + 583) * vision_of_britain['Argyll and Bute'].iloc[t]
    df['Dunbartonshire'].iloc[t] = vision_of_britain['East Dunbartonshire'].iloc[t] + vision_of_britain['West Dunbartonshire'].iloc[t]
    df['Renfrewshire'].iloc[t] = vision_of_britain['Renfrewshire'].iloc[t] + vision_of_britain['Inverclyde'].iloc[t]
    df['Lanarkshire'].iloc[t] = vision_of_britain['Glasgow'].iloc[t] + vision_of_britain['North Lanarkshire'].iloc[t] + vision_of_britain['South Lanarkshire'].iloc[t]
    df['Stirlingshire'].iloc[t] = vision_of_britain['Stirling'].iloc[t] + vision_of_britain['Falkirk'].iloc[t]
    df['Midlothian'].iloc[t] = vision_of_britain['Edinburgh'].iloc[t] + vision_of_britain['Midlothian'].iloc[t]
    df['Dumfriesshire'].iloc[t] = 2753/(2753 + 2328 + 1261) * vision_of_britain['Dumfries and Galloway'].iloc[t]
    df['Kirkcudbrightshire'].iloc[t] = 2328/(2753 + 2328 + 1261) * vision_of_britain['Dumfries and Galloway'].iloc[t]
    df['Wigtownshire'].iloc[t] = 1261/(2753 + 2328 + 1261) * vision_of_britain['Dumfries and Galloway'].iloc[t]
    df['Berwickshire'].iloc[t] = 1184/(1419 + 1725 + 629 + 1184) * vision_of_britain['Scottish Borders'].iloc[t]
    df['Peeblesshire'].iloc[t] = 1419/(1419 + 1725 + 629 + 1184) * vision_of_britain['Scottish Borders'].iloc[t]
    df['Roxburghshire'].iloc[t] = 1725/(1419 + 1725 + 629 + 1184) * vision_of_britain['Scottish Borders'].iloc[t]
    df['Selkirkshire'].iloc[t] = 692/(1419 + 1725 + 629 + 1184) * vision_of_britain['Scottish Borders'].iloc[t]
    df['Montgomeryshire'].iloc[t] = 1955/(1955 + 1862 + 1862) * vision_of_britain['Powys'].iloc[t]
    df['Radnorshire'].iloc[t] = 1862/(1955 + 1862 + 1862) * vision_of_britain['Powys'].iloc[t]
    df['Brecknockshire'].iloc[t] = 1862/(1955 + 1862 + 1862) * vision_of_britain['Powys'].iloc[t]



#Create final dataframe
for i in range(len(new_list_)):
    column = lst_of_columns[i]
    county = new_list_[i]
    if column not in lst_to_remove:

        for k in range(len(vision_of_britain)):
            num = df[county].iloc[k] + vision_of_britain[column].iloc[k]
            df[county].iloc[k] = num

       
df.to_csv('population_by_county_linear.csv')


###############################################################################
#     SECTION 1 SUBTASK 3 TASK 2)i  - Northern Ireland Population Data        #
###############################################################################


#OCR Functions 
def process_image(image):
    """
    Function that pre-processes and image in preparation for OCR

    """

    adaptiveThresholdImage = cv2.adaptiveThreshold(image.astype(np.uint8), 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 9, 41)
    kernal = np.ones((1, 1), np.uint8)
    
    begin = cv2.morphologyEx(adaptiveThresholdImage, cv2.MORPH_OPEN, kernal)
    end = cv2.morphologyEx(begin, cv2.MORPH_CLOSE, kernal)
    
    return cv2.bitwise_or(image, end)
    
#pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
#image = cv2.imread('', 0)
#image = process_image(image)
#imagetext = pytesseract.image_to_string(image, lang='eng', config='--psm 9')

#Read in Irish Population data and interpolate where need
irish_population_data = pd.read_csv('Irish_population_data.csv')
years = remove_commas_from_lst(list(irish_population_data['Year']))
population = remove_commas_from_lst(list(irish_population_data['Population']))
population_end, year_end = check_dates_and_pop(years, population)


population_by_county = pd.read_csv('population_by_county_linear.csv')
population_by_county.set_index(population_by_county['Unnamed: 0'], inplace = True)
index_of_places = pd.read_csv("IPN_GB_2021.csv", encoding = "ISO-8859-1")
columns = list(population_by_county.columns)
if 'Unnamed: 0'  in columns:
    columns.remove('Unnamed: 0')
    
if 'Unnamed: 59' in columns:
    columns.remove('Unnamed: 59')
population_by_region = pd.DataFrame(0, columns = list(index_of_places['eer20nm'].unique()), index = list(range(1800, 2021)))








###############################################################################
#         SECTION 2 SUBTASK 1 TASK 1)i  - Plot Choropleth Map                 #
###############################################################################
#Define relevant functions 
def find_region(place):
    """
    Function that returns the region a historical county (the place passed in) is in 


    """
    northern_ireland_counties = ['Antrim', 'Armagh', 'Down', 'Fermanagh', 'Londonderry', 'Tyrone', 'Derry']
    if place in northern_ireland_counties:
        return 'Northern Ireland'

    
    filtered_df = index_of_places.loc[index_of_places['ctyhistnm'] == place] 
    
    
    
    region = filtered_df['eer20nm'].mode()
    
    try: 
        return region.values[0]
    
    except IndexError:
        
        return None
    
def process_date(date):
    
    """
    Function that returns the year from a date
    """
    
    return date.split('-')[0]

def retrive_parliament_number(string_):
      """
        Function that returns the integer number when passed a string that describes which parliament an MP 
        was a part of 
    
    
      """
      string_ = string_.replace('Member of the ', '')
      string_ = string_.replace('Parliament of the United Kingdom', '')
      string_ = string_.replace('th ', '')
      string_ = string_.replace('nd', '')
      string_ = string_.replace('st', '')
      string_ = string_.replace('rd', '')
      ParliamentNumber = int(string_)
    
      return ParliamentNumber


def return_population_county(parliament_number, county):
    
    """
    Function that returns the population of a county in the specific year of the parliament number
    """
    
    
    row_of_number = parliament_dates[parliament_dates['Parliament_Number'] == parliament_number]
    date = row_of_number['startTime.value'].iloc[0]
   
    row = population_by_county[population_by_county.index == int(date)]
    population = row[county]
    
    return population.iloc[0]

#Create population data by region
for place in columns:

    if place != 'Antrim' and place != 'Armagh' and place != 'Down' and place != 'Fermanagh' and place != 'Londonderry' and place !='Tyrone': 
    
        region = find_region(place)
        
        for i in range(len(population_by_county)): 
            
            population_by_region[region].iloc[i] = population_by_region[region].iloc[i] + population_by_county[place].iloc[i]
            
            
        
population_by_region['Northern Ireland'] = population_end 
population_by_county['Northern Ireland'] = population_end
population_by_region.to_csv('population_by_region_including_NI.csv')


#Plot maps 
parliament_dates = pd.read_csv('parliament_dates.csv')
parliament_dates['startTime.value'] = parliament_dates['startTime.value'].apply(process_date)
parliament_dates['Parliament_Number'] = parliament_dates['parliamentLabel.value'].apply(retrive_parliament_number)
index_of_places = pd.read_csv('IPN_GB_2021.csv', encoding = "ISO-8859-1")




#Read in shapefile
fp = "UKDefinitionA.shx"
map_df_A = gpd.read_file(fp)

#Remove data that does not contain county data
all_data_dropped = all_data[all_data['county'].notna()]
all_data_dropped['region'] = all_data_dropped['county'].apply(find_region)

        
total_sum = 0 
y = []

normalised_df = pd.DataFrame(index = list(map_df_A['NAME'].unique()))
count_by_year = pd.DataFrame(index = list(map_df_A['NAME'].unique()) )
for i in range(1,59): 
    
    #Select data corresponding to the specific Parliament Number 
    df_ = all_data_dropped[all_data_dropped['parliamentNumber'] == i]

    #Create a copy of the shapefile to work with
    temp_map = copy.deepcopy(map_df_A)
    
    #Count number of MPs born in each historical county 
    df_count = df_['county'].value_counts().rename_axis('NAME').reset_index(name='counts')
    df_count.to_csv('CountPN' + str(i) + '.csv')
    
    #Count number of MPs born in each historical county 
    df_count_region = df_['region'].value_counts().rename_axis('NAME').reset_index(name='counts')
    df_count_region.to_csv('CountPN' + str(i) + '.csv')
    
    #Do Northern Ireland separately 
    northern_ireland_count = df_count[(df_count['NAME'] == 'Antrim') | (df_count['NAME'] == 'Armagh') | (df_count['NAME'] == 'Down') | (df_count['NAME'] == 'Fermanagh') | (df_count['NAME'] == 'Londonderry')  | (df_count['NAME'] == 'Derry') | (df_count['NAME'] == 'Tyrone') ]
    northern_ireland_count.to_csv('NI_testing.csv')
    total_NI = northern_ireland_count['counts'].sum()
    NI_population = return_population_county(i, 'Northern Ireland') / 1000
    print(total_NI, NI_population)
    NI = total_NI / NI_population
    
    #Join the datasets to be able to plot them
    df_count.set_index(df_count['NAME'], inplace = True)
    temp_map.set_index(temp_map['NAME'], inplace = True)
    
    #Set counts to 0 initially
    temp_map['count'] = 0
    temp_map['normal_count'] = 0
    total_pop = 0
    
        
    for coun in df_count['NAME'].unique():
        
        if coun == 'Antrim' or coun == 'Armagh' or coun == 'Down' or coun == 'Fermanagh' or coun == 'Londonderry' or coun == 'Tyrone' or coun == 'Derry':
            temp_map['count'].loc[coun] = NI
        
        else:

            population = return_population_county(i, coun) / 1000
            
            if population  == 0:
                population = 1
            
            temp_map['count'].loc[coun] = (df_count['counts'].loc[coun]) / population
            temp_map['normal_count'].loc[coun] = df_count['counts'].loc[coun]
    
    normalised_df[i] = temp_map['count']
    count_by_year[i] = temp_map['normal_count']
        
    
    #Choose between PowerTransformer and MinMaxScaler
    scaler = PowerTransformer(method="yeo-johnson")
    #scaler = MinMaxScaler()
    
    
    temp_map['count'] = scaler.fit_transform(temp_map[['count']])
    
    #Plot the chloropleth
    fig, ax = plt.subplots(1, figsize = (10,6))
    ax.set_title(i)
    ax.axis('off')
    temp_map.plot(column = 'count', cmap='viridis', linewidth=1, ax=ax, edgecolor='0.9', legend = True)
    sub_directory = 'Maps/'
    #Save figure
    #plt.savefig(sub_directory + 'Power' + str(i) +'.png', bbox_inches='tight')
    plt.close(fig)
    
    
    
###############################################################################
#         SECTION 2 SUBTASK 2 TASK 1)i  - Plot MCA, MFA, PCA                  #
###############################################################################


#Define relevant functions 
def return_population_region(parliament_number, region):
    """
    Function that returns the population of a specific region for the time of a given parliament

    """

    row_of_number = parliament_dates[parliament_dates['Parliament_Number'] == parliament_number]
    date = row_of_number['startTime.value'].iloc[0]
   
    row = population_by_region[population_by_region.index == int(date)]

    population = row[region]
    
    return population.iloc[0]

def return_start_date(row):
    """
    Function that returns the start data of a specific parliament 

    """
    
    parliament_number = row['parliamentNumber']
    row_of_number = parliament_dates[parliament_dates['Parliament_Number'] == parliament_number]
    return row_of_number['startTime.value'].iloc[0]

def return_party(party_wiki_identifier):
    """
    Function that returns the party corresponding to the party wikidata identifier passed in
    """
    try: 
        if party_wiki_identifier == None: 
            return None
        
        if party_wiki_identifier == 'http://www.wikidata.org/entity/Q9626': 
            return 'Conservative Party'
        
        elif party_wiki_identifier == 'http://www.wikidata.org/entity/Q9630' :
            return 'Labour Party' 
        
        elif 'http:' in party_wiki_identifier:
            return 'Other Party'
        
        else:
            return None
    
    except TypeError:
        return None


all_data_dropped['startTime'] = all_data_dropped.apply(lambda x: return_start_date(x), axis =1)



#Create list of regions and list of counties
lst_of_regions = list(population_by_region.columns)
if 'Unnamed: 0' in lst_of_regions:
    lst_of_regions.remove('Unnamed: 0')

lst_of_counties = list(population_by_county.columns)
if 'Unnamed: 0' in lst_of_counties:
    lst_of_counties.remove('Unnamed: 0')


#Create csv files containing data for the MCA, MFA and PCA


#First create csv file containing number of MPs born in county per 1000 people of population of county for each county and each Parliament number 
#Save this csv file with the parliament number as index and also save it with the counties as index 
region_as_index = pd.DataFrame(0, index = lst_of_counties, columns = list(range(1,59)))
parliament_number_as_index = pd.DataFrame(0, index = list(range(1,59)), columns = lst_of_counties)

for i in range(1,59):
    
    #Select data corresponding to relevant parliament number
    df_ = all_data_dropped[all_data_dropped['parliamentNumber'] == i]
    
    #Count the number of MPs born in each region
    df_count = df_['county'].value_counts()
    
    for county in lst_of_counties:
        
        #Return the population in the relevant year and region and divide by 1000
        population = return_population_county(i, county) / 1000 
        
        #Avoid dividing by 0
        if population == 0:
            population = 1 

        #Add to dataframe, if the county exisits in df_count (i.e. if the count is not 0) 
        try:
            region_as_index[i].loc[county] = df_count[county] / population
            parliament_number_as_index[county].loc[i] = df_count[county] / population
        
        except KeyError:
            pass


#Save
region_as_index.to_csv('county_as_index.csv')
parliament_number_as_index.to_csv('parliament_number_as_index_county.csv')


#Now do the same thing but regionally, create csv file containing number of MPs born in region per 10000 people of population of county for each county and each Parliament number 
#Save this file with region as index and also save a separate file with Parliament number as index 

#Create dataframes
region_as_index = pd.DataFrame(0, index = lst_of_regions, columns = list(range(1,59)))
parliament_number_as_index = pd.DataFrame(0, index = list(range(1,59)), columns = lst_of_regions)


for i in range(1,59):
    #Select data corresponding to relevant parliament number
    df_ = all_data_dropped[all_data_dropped['parliamentNumber'] == i]
    
    #Count the number of MPs born in each region
    df_count = df_['region'].value_counts()
    for region in lst_of_regions:
        
        #Return the population in the relevant year and region and divide by 10000
        population = return_population_region(i, region) / 10000
        
        
        region_as_index[i].loc[region] = df_count[region] / population
        parliament_number_as_index[region].loc[i] = df_count[region] / population

#Save
region_as_index.to_csv('region_as_index_region.csv')
parliament_number_as_index.to_csv('parliament_number_as_index_region.csv')


###############################################################################
#         SECTION 2 SUBTASK 2 TASK 2)i  - Plot MCA                            #
###############################################################################
    

#Process dataset
all_data_dropped['political_party'] = all_data_dropped['group.value'].apply(return_party)

lst_of_regions = list(population_by_region.columns)
if 'Unnamed: 0' in lst_of_regions:
    lst_of_regions.remove('Unnamed: 0')

#Select relevant columns for gender MCA
MCA_df_gender = all_data_dropped[['genderLabel.value', 'region' , 'parliamentNumber']]
MCA_df_gender = MCA_df_gender[MCA_df_gender['genderLabel.value'].notna()]

df_gender = pd.DataFrame(columns = ['Parliament Number', 'Region', 'Male Sum', 'Female Sum'])
#Create dataset for gender MCA 
for i in range(1,59):
    df_ = MCA_df_gender[MCA_df_gender['parliamentNumber'] == i]
    
    for region in lst_of_regions:
        
        df__ = df_[df_['region'] == region]
        
        sum_of_male = len(df__[df__['genderLabel.value'] == 'male'])
        sum_of_female = len(df__[df__['genderLabel.value'] == 'female'] )
        
    df_gender = df_gender.append({'Parliament Number': i, 'Region': region, 'Male Sum': sum_of_male, 'Female Sum': sum_of_female}, ignore_index = True)
        
        
#Select relevant columns for MCA party
MCA_df_party = all_data_dropped[['political_party', 'region' , 'parliamentNumber']]
MCA_df_party = MCA_df_party[MCA_df_party['political_party'].notna()]
MCA_df_party = MCA_df_party[MCA_df_party['region'].notna()]
MCA_df_party = MCA_df_party[MCA_df_party['parliamentNumber'].notna()]


df_party = pd.DataFrame(columns = ['Parliament Number', 'Region', 'Labour Sum', 'Conservative Sum'])
#Create dataset for party MCA
for i in range(1,59):
    df_ = MCA_df_party[MCA_df_party['parliamentNumber'] == i]
    
    for region in lst_of_regions:
        
        df__ = df_[df_['region'] == region]
        
        sum_of_labour = len(df__[df__['political_party'] == 'Labour Party'])
        sum_of_conservative = len(df__[df__['political_party'] == 'Conservative Party'] )
        
    df_party = df_party.append({'Parliament Number': i, 'Region': region, 'Labour Sum': sum_of_labour, 'Conservative Sum': sum_of_conservative}, ignore_index = True)
        
        
       
        
mca = prince.MCA(n_components=2, n_iter=3, copy=True, check_input=True, engine='auto', random_state=42)
mca = mca.fit(df_party)


mca = prince.MCA(n_components=2, n_iter=3, copy=True, check_input=True, engine='auto', random_state=42)
mca = mca.fit(df_gender)


###############################################################################
#         SECTION 2 SUBTASK 2 TASK 3)i  - Plot MFA                            #
###############################################################################
    
#Read in relevant dataset
df = pd.read_csv('parliament_number_as_index_county.csv')
y = df['Unnamed: 0']
X = df.drop('Unnamed: 0', 1)


columns = list(population_by_county.columns)
if 'Unnamed: 0' in columns:
    columns.remove('Unnamed: 0')
if 'Unnamed: 59' in columns:
    columns.remove('Unnamed: 59')

groups = {'Scotland':[], 'South East':[], 'East Midlands':[], 'West Midlands':[], 'Wales':[], 'South West':[], 'Eastern':[], 'North East':[], 'North West':[], 'London':[], 'Yorkshire and The Humber':[]}
for county in columns: 
    
    if (county == 'Antrim') or (county == 'Armagh') or (county == 'Down') or (county == 'Fermanagh') or (county ==  'Londonderry') or (county == 'Tyrone') or (county == 'Derry') or (county == 'Northern Ireland'):
        pass
    else:
        region = find_region(county)
        groups[region].append(county)



mfa = prince.MFA(groups=groups, n_components=2, n_iter=3, copy=True, check_input=True,engine='auto',random_state=42)
mfa = mfa.fit(X)
ax = mfa.plot_row_coordinates(X,ax=None,figsize=(10, 10),x_component=0,y_component=1,labels=X.index,ellipse_outline=False,ellipse_fill=True,show_points=True)



###############################################################################
#         SECTION 2 SUBTASK 2 TASK 3)i  - Plot PCA                            #
###############################################################################
    

df = pd.read_csv('parliament_number_as_index_region.csv')
lst_of_regions = list(df.columns)
if 'Unnamed: 0' in lst_of_regions:
    lst_of_regions.remove('Unnamed: 0')

df = pd.read_csv('region_as_index_region.csv')
y = df['Unnamed: 0']
X = df.drop('Unnamed: 0', 1)


pca = PCA(n_components=2)
pc = pca.fit_transform(X)
df = pd.DataFrame(data = pc, columns = ['Principal Component One', 'Principal Component Two'])
df['region'] = lst_of_regions 

fig = plt.figure(figsize = (8,8))
ax = fig.add_subplot(1,1,1) 

ax.set_xlabel('Principal Component One', fontsize = 13)
ax.set_ylabel('Principal Component Two', fontsize = 13)
ax.set_title('Two Component Principle Component Analysis', fontsize = 20)

colours = ['peru', 'red', 'orangered', 'yellow', 'lightgreen', 'g', 'blue', 'purple', 'cyan', 'deeppink', 'black']

for region, colour in zip(lst_of_regions,colours):
    indicesToKeep = df['region'] == region
    ax.scatter(df.loc[indicesToKeep, 'principal component 1'], df.loc[indicesToKeep, 'principal component 2'], c = colour, s = 50)
ax.legend(lst_of_regions)
ax.grid()




