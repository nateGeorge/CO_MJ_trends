# scraped on 1-6-2017
import requests as req
from bs4 import BeautifulSoup as bs
import pandas as pd
from lxml import html
import urllib
import re
from collections import OrderedDict
import numpy as np
from glob import iglob

URL = 'https://www.colorado.gov/pacific/revenue/colorado-marijuana-tax-data'

def clean_filename(web_link):
    '''
    cleans filenames from web link

    args:
    web_link -- string; link to excel file to download
    returns:
    cleaned_filename -- string; filename that is

    '''
    templink = re.sub('%2\d', '_', web_link.split('/')[-1])
    cleaned_filename = re.sub('_+', '_', templink)
    return cleaned_filename

def scrape_current_special_tax():
    '''
    scrapes the current special 10% tax data from
    https://www.colorado.gov/pacific/revenue/colorado-marijuana-tax-data
    '''
    res = req.get(URL)
    soup = bs(res.content, 'lxml')
    # found 7 of these...not super easy to grab the right ones
    #tables = soup.findAll('div', {'class':'fieldset-wrapper'})
    # abandoning BeautifulSoup in favor of lxml
    tree = html.fromstring(res.content)
    mj_state_tax_current = tree.xpath('//*[@id="collapse-text-dynamic-form-number-1"]/div/div[3]/fieldset/div/ul/li/ul')
    lis = mj_state_tax_current[0].findall('li')
    links = []
    filenames = []
    for l in lis:
        templink = l.find('a').get('href')
        links.append(templink)
        filename = clean_filename(templink)
        filenames.append(filename)

    for l, f in zip(links, filenames):
        testfile = urllib.URLopener()
        testfile.retrieve(l, 'data/' + f)

def scrape_current_sales_tax():
    '''
    scrapes the archived sales tax data from
    https://www.colorado.gov/pacific/revenue/colorado-marijuana-tax-data
    '''
    res = req.get(URL)
    soup = bs(res.content, 'lxml')
    # found 7 of these...not super easy to grab the right ones
    #tables = soup.findAll('div', {'class':'fieldset-wrapper'})
    # abandoning BeautifulSoup in favor of lxml
    tree = html.fromstring(res.content)
    mj_state_tax_current = tree.xpath('//*[@id="collapse-text-dynamic-form-number-1"]/div/div[2]/fieldset/div/ul/li/ul')
    lis = mj_state_tax_current[0].findall('li')
    links = []
    filenames = []
    for l in lis:
        templink = l.find('a').get('href')
        links.append(templink)
        filename = clean_filename(templink)
        filenames.append(filename)

    for l, f in zip(links, filenames):
        testfile = urllib.URLopener()
        testfile.retrieve(l, 'data/' + f)

def scrape_old_sales_tax():
    '''
    scrapes the archived sales tax data from
    https://www.colorado.gov/pacific/revenue/colorado-marijuana-tax-data
    '''
    res = req.get(URL)
    soup = bs(res.content, 'lxml')
    tree = html.fromstring(res.content)
    mj_state_tax_arch = tree.xpath('//*[@id="collapse-text-dynamic-form-number-1"]/div/fieldset[2]/div/div[1]/fieldset[2]/div/ul')
    years = mj_state_tax_arch[0].findall('li')
    links = []
    filenames = []
    for y in years:
        ul = y.findall('ul')[0]
        links_li = ul.findall('li')
        for l in links_li:
            templink = l.find('a').get('href')
            links.append(templink)
            filename = clean_filename(templink)
            filenames.append(filename)

    for l, f in zip(links, filenames):
        testfile = urllib.URLopener()
        testfile.retrieve(l, 'data/' + f)

def scrape_old_special_tax():
    '''
    scrapes the archived sales tax data from
    https://www.colorado.gov/pacific/revenue/colorado-marijuana-tax-data
    '''
    res = req.get(URL)
    soup = bs(res.content, 'lxml')
    tree = html.fromstring(res.content)
    mj_state_tax_arch = tree.xpath('//*[@id="collapse-text-dynamic-form-number-1"]/div/fieldset[2]/div/div[2]/fieldset/div/ul')
    years = mj_state_tax_arch[0].findall('li')
    links = []
    filenames = []
    for y in years:
        ul = y.findall('ul')[0]
        links_li = ul.findall('li')
        for l in links_li:
            templink = l.find('a').get('href')
            links.append(templink)
            filename = clean_filename(templink)
            filenames.append(filename)

    for l, f in zip(links, filenames):
        testfile = urllib.URLopener()
        testfile.retrieve(l, 'data/' + f)

def get_state_tax_filenames():
    '''
    returns list of files with county-level state taxes for marijuana
    '''
    all_files = iglob('data/*.xlsx')
    file_list = []
    for f in all_files:
        if re.search('Marijuana_Sales_Tax_by_County', f) or re.search('Marijuana_Sales_Tax_2.9', f):
            file_list.append(f.split('/')[1])

    return file_list

def get_special_tax_filenames():
    '''
    returns list of files with county-level state taxes for marijuana
    '''
    all_files = iglob('data/*.xlsx')
    file_list = []
    for f in all_files:
        if re.search('Local_Government_Distribution', f) or re.search('Retail_Marijuana_Special', f):
            file_list.append(f.split('/')[1])

    return file_list

def parse_sales_tax(filenames):
    # of course, the data is super sloppy and counties differ in each file.
    # first, go through each file to get list of all counties so we can the build
    # up the dataframe of county/year/tax revenue
    med_months = []
    rec_months = []
    med_years = []
    rec_years = []
    med_dates = []
    rec_dates = []
    all_med_tax = []
    all_med_counties = []
    all_rec_tax = []
    all_rec_counties = []

    for f in filenames:
        month_year = f.split('_')[0]
        print month_year
        # this would convert the date to a timestamp, but not going to do that anymore
        #timestamp = time.strptime(month_year, '%m%y')
        month = int(month_year[:2])
        year = int('20' + month_year[2:])
        month_year = str(month) + '-01' + '-' + str(year)

        data = pd.read_excel('data/' + f, header=None)

        # parse medical county and total 2.9% tax
        # first get row number of county header and last row (with "totals")
        startRow = data[data[0] == 'County'].index.values[0] + 1
        lastRow = data[data[0].str.startswith('Total', na=False)].index.values[0] # up to but not including

        counties_med = []
        counties_rec = []
        tax_med = []
        tax_rec = []
        for i in range(startRow, lastRow):
            if re.search('remainder', data.iloc[i, 0], re.IGNORECASE):
                counties_med.append('Other Counties')
            else:
                counties_med.append(data.iloc[i, 0])

            tax_med.append(data.iloc[i, 1])
            med_months.append(month)
            med_years.append(year)
            med_dates.append(month_year)

        startRow = data[data[3] == 'County'].index.values[0] + 1
        lastRow = data[data[3].str.startswith('Total', na=False)].index.values[0] # up to but not including

        for i in range(startRow, lastRow):
            if re.search('remainder', data.iloc[i, 3], re.IGNORECASE):
                counties_rec.append('Other Counties')
            else:
                counties_rec.append(data.iloc[i, 3])

            tax_rec.append(data.iloc[i, 4])
            rec_months.append(month)
            rec_years.append(year)
            rec_dates.append(month_year)

        all_med_counties.extend(counties_med)
        all_rec_counties.extend(counties_rec)
        all_med_tax.extend(tax_med)
        all_rec_tax.extend(tax_rec)

    unique_med_counties = pd.Series(np.unique(all_med_counties))
    unique_rec_counties = pd.Series(np.unique(all_rec_counties))
    #remove 'remainder' entries since there are multiple with minor diffs
    unique_med_counties = unique_med_counties.drop(unique_med_counties[unique_med_counties.str.startswith('Remainder')].index.values)
    unique_med_counties = unique_med_counties.append(pd.Series(['Other Counties']))
    unique_rec_counties = unique_rec_counties.drop(unique_rec_counties[unique_rec_counties.str.startswith('Remainder')].index.values)
    unique_rec_counties = unique_rec_counties.append(pd.Series(['Other Counties']))
    unique_med_counties = unique_med_counties.sort_values()
    unique_rec_counties = unique_rec_counties.sort_values()

    # blank = np.zeros(shape=(unique_rec_counties.shape[0]*len(filenames)*len(unique()), 3))
    # for i, c, m, y, r in enumerate(zip(all_rec_counties, rec_months, rec_years, all_rec_tax)):
    #     blank[i, 0] = c
    #     blank[i, 1] = y
    #     blank[i, 2] = m
    #     blank[i, 3] = r

    df = pd.DataFrame(OrderedDict({'county':all_med_counties, 'year':med_years, 'month':med_months, 'date':med_dates, '2.9%tax':all_med_tax}))
    # this is 2.9% tax, so divide by 0.029 to get revenue
    df['total_revenue'] = df['2.9%tax'].apply(lambda x: x/0.029)
    df.to_csv('data/med_taxes_2.9.csv')

    df = pd.DataFrame(OrderedDict({'county':all_rec_counties, 'year':rec_years, 'month':rec_months, 'date':rec_dates, '2.9%tax':all_rec_tax}))
    df['total_revenue'] = df['2.9%tax'].apply(lambda x: x/0.029)
    df.to_csv('data/rec_taxes_2.9.csv')

def parse_special_tax(filenames):
    # of course, the data is super sloppy and counties differ in each file.
    # first, go through each file to get list of all counties so we can the build
    # up the dataframe of county/year/tax revenue
    rec_months = []
    rec_years = []
    rec_dates = []
    all_rec_tax = []
    all_rec_cities = []

    for f in filenames:
        print f
        month_year = f.split('_')[0]
        print month_year
        # this would convert the date to a timestamp, but not going to do that anymore
        #timestamp = time.strptime(month_year, '%m%y')
        month = int(month_year[:2])
        year = int('20' + month_year[2:])
        month_year = str(month) + '-01' + '-' + str(year)

        data = pd.read_excel('data/' + f, header=None)

        # parse medical county and total 2.9% tax
        # first get row number of county header and last row (with "totals")

        if year < 2016:
            startRow = data[data[1].str.startswith('Distrib', na=False)].index.values[0] + 1
            lastRow = data[data[1].str.startswith('Total', na=False)].index.values[0] # up to but not including
            for i in range(startRow, lastRow):
                if re.search('combined', data.iloc[i, 1], re.IGNORECASE):
                    all_rec_cities.append('Other Cities')
                else:
                    all_rec_cities.append(data.iloc[i, 1])
                all_rec_tax.append(data.iloc[i, 2])
                rec_months.append(month)
                rec_years.append(year)
                rec_dates.append(month_year)
        else:
            # format was changed in 2016, so have to parse it in a different way
            startRow = data[data[0].str.startswith('Local', na=False)].index.values[0] + 1
            lastRow = data[data[0].str.startswith('Total', na=False)].index.values[0] # up to but not including
            for i in range(startRow, lastRow):
                if re.search('combined', data.iloc[i, 0], re.IGNORECASE):
                    all_rec_cities.append('Other Cities')
                else:
                    all_rec_cities.append(data.iloc[i, 0])

                all_rec_tax.append(data.iloc[i, 1])
                rec_months.append(month)
                rec_years.append(year)
                rec_dates.append(month_year)


    df = pd.DataFrame(OrderedDict({'city':all_rec_cities, 'year':rec_years, 'month':rec_months, 'date':rec_dates, 'special_tax':all_rec_tax}))
    # this is 15% of the 10% tax, so to get total revenue, divide by 0.15 then by 0.1
    df['total_revenue'] = df['special_tax'].apply(lambda x: x/0.15/0.1)
    df.to_csv('data/rec_special_taxes.csv')

if __name__ == "__main__":
    pass
# this is here because I was trying to think of the best way to organize the data
# for c in unique_med_counties:
#     med_rev_dict[c] = []
#
# for c, r in zip(all_med_counties, all_med_tax):
#     med_rev_dict[c] =
#
# all_med_tax = np.array(all_med_tax)
# all_ret_tax = np.array(all_ret_tax)

    # medical = pd.DataFrame(OrderedDict({'County':counties, '2.9%tax':med_tax}))
    # retail = pd.DataFrame(OrderedDict({'County':counties_ret, '2.9%tax':tax_ret}))
