# Modelling the Geographic Distribution of Places of Birth of UK Members of Parliament over Time

This report causally models the geographical distribution of MP’s place of birth, at a regional and historical county level, temporally from the first Parliament to present day. This analysis facilitates an evaluation of how geographically representative the MPs elected over time have been and highlights areas that are underrepresented or overrepresented.

Fair representation, as asserted by Philips [3], is achieved by there existing a loose correspondence between the experiences of those in government and their electorate [4]. This analysis provides an important dimension in answering how well the UK government has achieved fair representation. It finds specific relevance in understanding whether the historic regional funding bias [5] can be attributed to the geographic representativeness of MPs.


![Generated Images 1](MFA_graph.png?raw=true "MFA")

## Contents
* SPARQL.py - Python file for making queries to SPARQL Endpoint interface
* POS_spacy.py - Python file for POS using Spacy’s pretrained English pipeline ‘en_core_web_sm’
* choropleth_map.py - Python file plotting choropleth map using shapefiles
* combined_submitted_code.py - Python file containing all combined code (how the assignment was submitted - takes a few hours to run)
* combining_wikipedia_and_wikidata.py - Python file to make the data from Wikidata and Wikipedia consistent
* optical_character_recog.py - Python file that uses Optical Character Recognition (using Python-tesseract) to read in Northern Ireland population data
* plotting_MCA_MFA_PCA.py - Python file to plot PCA, MFA and MCA graphs
* processing_index_of_places.py - Python file pre-processing index of places file using pandas
* scrapping_vision_of_britain.py - Python file extracting information from Vision of Britain
* scrapping_wikipedia.py - Python file scrapping wikipedia 
* paper_comp_modelling - Report detailing methodology
