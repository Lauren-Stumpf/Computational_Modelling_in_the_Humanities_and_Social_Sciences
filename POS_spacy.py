


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
