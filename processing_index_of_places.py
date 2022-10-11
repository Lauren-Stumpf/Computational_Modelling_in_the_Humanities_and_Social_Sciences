

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

