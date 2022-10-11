
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



