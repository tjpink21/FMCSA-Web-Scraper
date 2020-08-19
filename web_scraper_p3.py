"""
Created on 06/11/2020

author: Tom Pink
contact: tpink@rff.org

This code was written for a machine running python3. Please see Nick Roy's web scrapper if you are running python2.

It should be noted that due this code is not very generalizable. If the FMCSA site layout has changed signifigantly since
this code has been written, then you may need to make changes to some of the lines of this code.
"""

#Import Libraries
import urllib.request
from urllib.request import urlopen
import shutil
from bs4 import BeautifulSoup
import csv
import pandas as pd
import datetime
import os
import os.path
import requests
import timeit
start = timeit.default_timer()

"""///////////////////////////////////////////////////////////////////////////////////////////////////////////////////"""
Today = datetime.datetime.now()
date = str(Today.month)+'_'+str( Today.day)+'_'+str ( Today.year)
"""DIRECTORY TO CHANGE"""
sourcedata = '~/Downloads/slow_data_2020_TJP.xlsx'

#Note: for extracting FMCSA data for multiple datasets use a simple for loop and make the sourcedata a list

slowdata = pd.read_excel(sourcedata, sheet_name='Sheet1')
observations = slowdata.usdot_num.tolist()
observations = slowdata['usdot_num']
print(observations)
"""This dictionary holds each key in the order in which they will be exported into the csv file. Each key corresponds to a list
which will be appended to with a value for each DOT # provided. This dictionary will later be used to create a list of lists whcih
can be easily written into the csv file. This dictionary is useful as it preserves the order of the keys regardless of the order
in which the values are added to them.
"""
all_values = {
    'Site_DOT_number' : [],'Carrier_Name' : [], 'Address' : [], 'City' : [], 'State': [], 'Zipcode' : [], 'Number_of_Vehicles' : [], 'Number_of_Drivers' : [], 'Number_of_Inspections' : [],
    'Safety_Rating' : [], 'Safety_Rating_Date' : [],'VehicleOOSr' : [],'DriverOOSr' : [], 'BASIC_Status_Date' : [], 'Most_Recent_Investigation' : [], 'Total_Inspections' : [], 'Inspections_with_Violation' : [],
    'Unsafe_Driving_Measure' : [], 'Hours_of_Service_Compliance' : [], 'Vehicle_Maintenance_Measure' : [],
    'Property' : [], 'Passenger' : [], 'Household_Goods' : [], 'Broker' : [],
    'OP_Classification' : [], 'Tractors_Owned' : [],'Tractors_Term_Leased' : [],'Tractors_Trip_Leased' : [], 'Trailers_Owned' : [],'Trailers_Term_Leased' : [], 'Trailers_Trip_Leased' : [], 'Percent_Trailers_Leased' : [], 'Percent_Trip_Leased' : []
}

"""///////////////////////////////////////////////////////////////////////////////////////////////////////////////////"""

"""This function determines the classification of the industries in which the trucking company operaties."""
def getclassification(soup):
    operationsdictionary = { 'AUTHORIZED FOR HIRE' : '1', 'EXEMPT FOR HIRE' : '2', 'PRIVATE PROPERTY' : '3',
                            'PRIVATE PASSENGER, BUSINESS' : '4', 'PRIVATE PASSENGER, NON-BUSINESS' : '5', 'MIGRANT' : '6',
                            'U. S. MAIL' : '7', 'FEDERAL GOVERNMENT' : '8', 'STATE GOVERNMENT' : '9',
                            'LOCAL GOVERNMENT' : '10', 'INDIAN TRIBE' : '12', 'PRIVATE PROPERTY' : '13',
                            'MIGRANT' : '14', 'STATE GOVERNMENT' : '15', 'OTHER' : '16'}
    elements = str(soup.find('ul',attrs={'class':"opClass"})).split('\n')
    operationslist = []
    for operation in elements:
        if ">X<" in operation:
            operation = operation.split('</li>')
            operation = operation[0].split('</span>')
            operation = operation[-1]
            operationslist.append(operation)        
    element = []
    for operation in operationslist:
        try:
            element.append(operationsdictionary[operation])
            #element = concatenate(element)
        except:
            element.append(operationsdictionary['OTHER'])
    return element

def createFolder(directory):
    if os.path.exists(directory):
        print('directory exists')
    else:
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print ('Error: Creating directory. ' +  directory)

def saveURLs(my_url_1, my_url_2, my_url_3, my_url_4, my_url_5, obs):
    #save each url into a file
    #Directory to change:
    direct = '/Users/tompink/Desktop/RFF_internship/Trucking/webfiles/webfiles_for_DOT#_{}_on_{}'.format(str(obs), date)
    createFolder(direct)
    item1 = urllib.request.urlopen(my_url_1)
    item2 = urllib.request.urlopen(my_url_2)
    item3 = urllib.request.urlopen(my_url_3)
    item4 = urllib.request.urlopen(my_url_4)
    item5 = urllib.request.urlopen(my_url_5)

    webContent1 = item1.read()
    webContent2 = item2.read()
    webContent3 = item3.read()
    webContent4 = item4.read()
    webContent5 = item5.read()

    file1 = open(os.path.join(direct, 'Overview_page_for_DOT#_' + str(obs)) + '.html', 'wb')
    file2 = open(os.path.join(direct, 'CarrierRegistration_page_for_DOT#_' + str(obs)) + '.html', 'wb')
    file3 = open(os.path.join(direct, 'UnsafenDriving_page_for_DOT#_' + str(obs)) + '.html', 'wb')
    file4 = open(os.path.join(direct, 'HOSCompliance_page_for_DOT#_' + str(obs)) + '.html', 'wb')
    file5 = open(os.path.join(direct, 'VehicleMaint_page_for_DOT#_' + str(obs)) + '.html', 'wb')

    file1.write(webContent1)
    file2.write(webContent2)
    file3.write(webContent3)
    file4.write(webContent4)
    file5.write(webContent5)

    file1.close
    file2.close
    file3.close
    file4.close
    file5.close
    
"""By looping through each DOT#, this function extracts all of the data from the relevant pages.
For each DOT #, there are 5 different urls from the FMCSA website which are accessed for their data.
Each url contains a different set of useful information. The information is extracted from the page,
then added to the all_values dictionary. Some of the DOT #'s provided may correspond to a trucking 
company which is no longer publicly listed on the FMCSA site. If one of these DOT #'s is passed into the 
loop, the data can not be extracted and a print statement will niotify the user."""
def main(observations, all_values):
    response = " "
    while (response!='Y' and response!='N'):
        response = input("Would you like to save the web files from this scrape? (Y/N): ")
    for place in range(0,(len(observations)-1)):
        obs = str(observations[place])
        #the DOT# values all had ".0" at the end of them, so the following line corrects that problem, this line will need to change depending on the format of the DOT#
        #obs = obs[:-2]
        #1st url:
        my_url_1 = 'https://ai.fmcsa.dot.gov/SMS/Carrier/' + str(obs)+ '/Overview.aspx?FirstView=True'
        #print(my_url_1)
        result = requests.get(my_url_1)
        src = result.content
        soup1 = BeautifulSoup(src, 'lxml')
        pretty_soup = soup1.prettify()

        #Getting basic info from page:
        dot_spot = soup1.find('li',attrs={'id':"dot-num-li"})
        if dot_spot is not None:
            dot_num=dot_spot.text.split('\r\n')[3].strip()
            all_values['Site_DOT_number'].append(dot_num)

            dot_spot = soup1.find('div',attrs={'class':"carrierName"})
            carrier_name=dot_spot.h3.text
            all_values['Carrier_Name'].append(carrier_name)

            spot = soup1.find_all('li')
            for s in spot:
                header=s.label
                if header is not None:
                    if header.text.strip() == 'Address:':
                        temp = s.text.split("\t")
                        address = temp[0].split('\r\n')[3].strip()
                        CSZ = temp[0].split('\r\n')[5].strip()
                    elif header.text.strip() == 'Number of Vehicles:':
                        temp = s.text.split("\t")
                        num_vehichles = temp[0].split('\r\n')[3].strip()
                    elif header.text.strip() == 'Number of Drivers:':
                        temp = s.text.split("\t")
                        num_drivers = temp[0].split('\r\n')[3].strip()
                    elif header.text.strip() == 'Number of Inspections:':
                        temp = s.text.split("\t")
                        num_inspects = temp[0].split('\r\n')[3].strip()   
                
            city = CSZ.split(',')[0]
            stzp = CSZ.split(',')[1]
            state = stzp.split(' ')[1]
            zip_code= stzp.split(' ')[2]
            all_values['Address'].append(address)
            all_values['City'].append(city)
            all_values['State'].append(state)
            all_values['Zipcode'].append(zip_code)
            all_values['Number_of_Vehicles'].append(num_vehichles)
            all_values['Number_of_Drivers'].append(num_drivers)
            all_values['Number_of_Inspections'].append(num_inspects)

            #Safety stuff:
            safe_rating = soup1.find('div',attrs={'id':"Rating"}).text
            all_values['Safety_Rating'].append(safe_rating)
            srd= soup1.find('div',attrs={'id':"RatingDate"})
            if srd is not None:
                safe_rating_date = soup1.find('div',attrs={'id':"RatingDate"}).text.split('\r\n')[2].strip()
                safe_rating_date = safe_rating_date.split(')')[0]
            else:
                safe_rating_date = 'Not Rated'
            all_values['Safety_Rating_Date'].append(safe_rating_date)

            vehichleOOSr = soup1.find('tbody').tr.th.find_next_sibling().text
            all_values['VehicleOOSr'].append(vehichleOOSr)

            driverOOSr = soup1.find('tbody').tr.find_next_sibling().th.find_next_sibling().text
            all_values['DriverOOSr'].append(driverOOSr)

            Property =  soup1.find('div',attrs={'id':"LicensingAndInsurance"}).tbody.tr.td.text
            Passenger = soup1.find('div',attrs={'id':"LicensingAndInsurance"}).tbody.tr.find_next_sibling().td.text
            HHGoods = soup1.find('div',attrs={'id':"LicensingAndInsurance"}).tbody.tr.find_next_sibling().find_next_sibling().td.text
            Broker = soup1.find('div',attrs={'id':"LicensingAndInsurance"}).tbody.tr.find_next_sibling().find_next_sibling().find_next_sibling().td.text
            all_values['Property'].append(Property)
            all_values['Passenger'].append(Passenger)
            all_values['Household_Goods'].append(HHGoods)
            all_values['Broker'].append(Broker)

            BASICdate = soup1.find('p',attrs={'class':"basicSubtitle"}).span.find_next_sibling().text.split('ending')[1].strip()
            all_values['BASIC_Status_Date'].append(BASICdate)
            try:
                MRI = soup1.find('section',attrs={'id':"SummaryOfActivities"}).li.text.split('\r\n')[3].strip().split(' (')[0]
            except:
                MRI = 'no inspection listed'
            all_values['Most_Recent_Investigation'].append(MRI)
            Total_inspections = soup1.find('section',attrs={'id':"SummaryOfActivities"}).li.find_next_sibling().text.split('\r\n')[3].strip()
            all_values['Total_Inspections'].append(Total_inspections)
            Inspections_with_violation = soup1.find('section',attrs={'id':"SummaryOfActivities"}).li.find_next_sibling().ul.li.find_next_sibling().text.split('\r\n')[3].strip()
            all_values['Inspections_with_Violation'].append(Inspections_with_violation)

            #2nd url:
            my_url_2 ='https://ai.fmcsa.dot.gov/SMS/Carrier/' + str(obs)+ '/CarrierRegistration.aspx'
            result2 = requests.get(my_url_2)
            src2 = result2.content
            soup2 = BeautifulSoup(src2, 'lxml')
            
            OPC = getclassification(soup2)
            all_values['OP_Classification'].append(OPC)

            values = soup2.tbody.find_all('tr')
            for v in values:
                if v.find('th',attrs={'class':"vehType"}).text == 'Truck Tractors':  
                    tractors_owned = v.td.text.replace(",", "")
                    tractors_termleased = v.td.find_next_sibling().text.replace(",", "")
                    tractors_tripleased = v.td.find_next_sibling().find_next_sibling().text.replace(",", "")
                elif v.find('th',attrs={'class':"vehType"}).text == 'Trailers*':
                    trailers_owned = v.td.text.replace(",", "")
                    trailers_termleased = v.td.find_next_sibling().text.replace(",", "")
                    trailers_tripleased = v.td.find_next_sibling().find_next_sibling().text.replace(",", "")

            all_values['Tractors_Owned'].append(tractors_owned)
            all_values['Tractors_Term_Leased'].append(tractors_termleased)
            all_values['Tractors_Trip_Leased'].append(tractors_tripleased)
            all_values['Trailers_Owned'].append(trailers_owned)
            all_values['Trailers_Term_Leased'].append(trailers_termleased)
            all_values['Trailers_Trip_Leased'].append(trailers_tripleased)

            #Calculate the percent of trailers leased
            total_trailer = int(trailers_owned) + int(trailers_termleased) + int(trailers_tripleased)
            total_leased = int(trailers_termleased) + int(trailers_tripleased)
            if total_trailer > 0:
                per_trailer_lease = (int(trailers_termleased) + int(trailers_tripleased))/total_trailer
            if total_leased > 0:
                per_tripleased = int(trailers_tripleased)/int(total_leased)
            else:
                per_trailer_lease = 0
                per_tripleased = 0

            all_values['Percent_Trip_Leased'].append(per_tripleased)
            all_values['Percent_Trailers_Leased'].append(per_trailer_lease)
            
            #url3:
            my_url_3 = 'https://ai.fmcsa.dot.gov/SMS/Carrier/' + str(obs)+ '/BASIC/UnsafeDriving.aspx'
            result3 = requests.get(my_url_3)
            src3 = result3.content
            soup3 = BeautifulSoup(src3, 'lxml')
            basic_overview = soup3.find('div',attrs={'id':"BASICOverviewContainer"})
            unsafe_driving = basic_overview.find('td',attrs={'class':"rel159"}).text.strip()

            #url4:
            my_url_4 = 'https://ai.fmcsa.dot.gov/SMS/Carrier/' + str(obs)+ '/BASIC/HOSCompliance.aspx'
            result4 = requests.get(my_url_4)
            src4 = result4.content
            soup4 = BeautifulSoup(src4, 'lxml')
            basic_overview = soup4.find('div',attrs={'id':"BASICOverviewContainer"})
            hrs_service_compliance = basic_overview.find('td',attrs={'class':"rel159"}).text.strip()

            #url5:
            my_url_5 = 'https://ai.fmcsa.dot.gov/SMS/Carrier/' + str(obs)+ '/BASIC/VehicleMaint.aspx'
            result5 = requests.get(my_url_5)
            src5 = result5.content
            soup5 = BeautifulSoup(src5, 'lxml')
            basic_overview = soup5.find('div',attrs={'id':"BASICOverviewContainer"})
            vehichle_maintenance = basic_overview.find('td',attrs={'class':"rel159"}).text.strip()

            all_values['Unsafe_Driving_Measure'].append(unsafe_driving)
            all_values['Hours_of_Service_Compliance'].append(hrs_service_compliance)
            all_values['Vehicle_Maintenance_Measure'].append(vehichle_maintenance)
            #Save URLs if response if yes
            if(response == 'Y'):
                saveURLs(my_url_1, my_url_2, my_url_3, my_url_4, my_url_5, obs)
        else:
            print('Check the following url: ' + '\n' + my_url_1 + '\n' + ' The information for DOT # ' + str(obs) + ' may no longer be available for public display.')
            observations.pop(place)
    return observations, all_values

filled_obs, all_values_filled = main(observations,all_values)
#csv test:

"""These lists include the names of each variable in catergorized lists. These names are used as headers for the csv file."""
Basic_Info = ['Site_DOT_number', 'Carrier_Name', 'Address', 'City', 'State', 'Zipcode', 'Number_of_Vehicles', 'Number_of_Drivers', 'Number_of_Inspections']
Safety_Rating = ['Safety_Rating', 'Safety_Rating_Date','VehicleOOSr','DriverOOSr', 'BASIC_Status_Date', 'Most_Recent_Investigation', 'Total_Inspections', 'Inspections_with_Violation']
Index_Measures = ['Unsafe_Driving_Measure', 'Hours_of_Service_Compliance', 'Vehicle_Maintenance_Measure']
Licensing = ['Property', 'Passenger', 'Household_Goods', 'Broker']
TruckTrailerLeaseOwn = ['OP_Classification', 'Tractors_Owned', 'Tractors_Term_Leased', 'Tractors_Trip_Leased','Trailers_Owned','Trailers_Term_Leased', 'Trailers_Trip_Leased', 'Percent_Trailers_Leased','Percent_Trip_Leased']

"""This nested loop extracts the data from the all_values dictionary and creates a list of values for each DOT #
Each lis of values is then appended to an overall list. This list of lists is then looped through and added into the 
csv file. Each DOT # has a unique row in the csv file with corresponding informatuon specified by the headers."""
temp_list=[]
list_of_lists=[]
for o in range(0,(len(filled_obs)-1)):
    for r in all_values_filled.keys():
        temp_list.append(all_values_filled[r][o])
    list_of_lists.append(temp_list)
    temp_list=[]
last_direct = '/Users/tompink/Desktop/RFF_internship/Trucking/scraped_data_files'    
createFolder(last_direct)
with open('/Users/tompink/Desktop/RFF_internship/Trucking/scraped_data_files/scraped_FMCSA_data_on_'+date+'.csv','w') as csvFile:
    wr = csv.writer(csvFile,dialect='excel')
    wr.writerow(Basic_Info + Safety_Rating + Index_Measures + Licensing + TruckTrailerLeaseOwn)
    for row in list_of_lists:
        wr.writerow(row)
csvFile.close()

stop = timeit.default_timer()

print('Run Time: ', round(stop - start, 2), ' seconds.')  