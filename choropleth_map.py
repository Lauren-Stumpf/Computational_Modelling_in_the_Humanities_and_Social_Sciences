

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
    
    