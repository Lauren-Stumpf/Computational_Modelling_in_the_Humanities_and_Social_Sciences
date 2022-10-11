
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
