
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
