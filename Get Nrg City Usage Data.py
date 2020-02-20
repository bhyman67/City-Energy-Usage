# Note to self, need to at least finish the part that gathers all of the data into all 3 dataframes 

from state_abbrevs import states
from bs4 import BeautifulSoup
from datetime import datetime
from api_key import key
import pandas as pd
import requests
import json
import os


# +++++++++++++++++++++++++++++++
# Retrieve data from wikipedia
# +++++++++++++++++++++++++++++++

# make the request
city_list_url = "https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population"
cities_wiki_html = requests.get(city_list_url).text

# Create the BeautifulSoup obj in order to parse the html
soup = BeautifulSoup(cities_wiki_html, "lxml")

# extract needed data
us_cities = [] 
html_dataTbl = soup.find("table", attrs={"class":"wikitable sortable","style":"text-align:center"})
for index, tr in enumerate(html_dataTbl.find_all("tr")):

    if index > 0:

        # Pull the list of table data cells from the current row
        tblCells = tr.find_all("td")

        # Grab needed data (might have to clean it a little bit too)
        #   -> would love to get more data but will worry about that later
        city = tblCells[1].text.rstrip("\n\r")
        if len(city.split("[")) > 1:
            city = city.split("[")[0]
        state = tblCells[2].text.rstrip("\n\r").lstrip("\xa0")

        # Append data to the list (add row to it...)
        us_cities.append((city,state))

    # this is temp code in order to make api calls to only a small set of cites
    # if index > 5:
    #     break
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# # Make sure everything looks right (put this into a function)
# for city in us_cities:
#     print( city[0] + ' -- ' + city[1] + ' -- ' + states[city[1]])


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Make api calls to NREL in order to retrieve the city electricity 
# and natuaral gas data
#   -> usage, expendatures, and ghg emissions 
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Residential specific cols
resCols = ["housing_units","total_pop"]
# Column used for the commercial and industrial markets
commIndusCol = ["num_establishments"]
# Data columns
dataCols = [
    "elec_1kdollars",
    "elec_mwh",
    "gas_mcf",
    "elec_1kdollars_bin_min",
    "elec_1kdollars_bin_max",
    "elec_mwh_bin_min",
    "elec_mwh_bin_max",
    "gas_1kdollars_bin_min",
    "gas_1kdollars_bin_max",
    "gas_mcf_bin_min",
    "gas_mcf_bin_max",
    "elec_lb_ghg",
    "elec_min_lb_ghg",
    "elec_max_lb_ghg",
    "gas_lb_ghg",
    "gas_min_lb_ghg",
    "gas_max_lb_ghg"
]

# Create the dataframes and map their respective markets to them
dataFrames = {
    "residential" : pd.DataFrame(columns = ["us_city"] + resCols + dataCols),
    "commercial" : pd.DataFrame(columns = ["us_city"] + commIndusCol + dataCols),
    "industrial" : pd.DataFrame(columns = ["us_city"] + commIndusCol + dataCols)
}

# Set up the url of for the api call
baseUrl = "https://developer.nrel.gov"
path = "/api/cleap/v1/energy_expenditures_and_ghg_by_sector"
queryString = {"api_key":key}

# For each city in the list of us cities
print("Retrieving data for:")
for us_city in us_cities:

    queryString["city"] = us_city[0]
    queryString["state_abbr"] = states[us_city[1]]

    # API call 
    r = requests.get( baseUrl+path , params = queryString ) 
    jsonResp = r.json() # create test funct for printing in json

    if len(jsonResp["errors"]) == 0:
        # Retrieve the city w/in the json resp
        #   -> the city maps to a dict with three keys (markets): residential, commercial, and industrial
        crntJsonCityKey = jsonResp["inputs"]["city"] 
        print(f"   -> {crntJsonCityKey},{states[us_city[1]]}") # script fails @ Indianapolis Indiana
        for market in jsonResp["result"][crntJsonCityKey]:

            # Get all of the values for the current market
            values = {"us_city":f"{crntJsonCityKey},{states[us_city[1]]}"}
            crntMarket = jsonResp["result"][crntJsonCityKey][market]
            for measurment in crntMarket:

                # append record to the dataframe
                values[measurment] = crntMarket[measurment]

            dataFrames[market] = dataFrames[market].append(values, ignore_index = True)

# write the dataframes to a csv, so that you'll be able to read in data from them in the future 
xlFileName = str(datetime.now()).replace(":"," ") + ".xlsx"
for index, dFrame in enumerate(dataFrames):

    if index == 0:

        dataFrames[dFrame].to_excel(  os.path.join( os.getcwd() ,"archive" , xlFileName ) , dFrame  )

    else:

        with pd.ExcelWriter(path=os.path.join( os.getcwd() ,"archive" , xlFileName ),mode="a", engine="openpyxl") as writer:
            dataFrames[dFrame].to_excel(writer,dFrame)


# ++++++++++++++++
# The Analysis
# ++++++++++++++++


print("Done")