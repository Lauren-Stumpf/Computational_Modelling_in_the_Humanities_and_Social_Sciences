
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
