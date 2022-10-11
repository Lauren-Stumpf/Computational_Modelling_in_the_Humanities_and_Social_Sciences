
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



