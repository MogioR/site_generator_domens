import random
import os
import json
import re
import math
from multiprocessing import Pool
from functools import partial

import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from russian_names import RussianNames

from Modules.google_sheets_api import GoogleSheetsApi


"""List names for downloading"""
CONTAINER_LIST = 'Container'
MASTER_DATA_LIST = 'MasterData'
MASTER_ABOUT_LIST = 'MasterAbout'
SELECTION_MASTER_LIST = 'SectionMaster'
MASTER_EDUCATION_LIST = 'MasterEducation'
SECTION_LIST = 'Section'
DOMAIN_LIST = 'Domain'

SITE_TEMPLATE_ROOT = '\\Assets\\template.html'
MASTER_ITEM_TEMPLATE_ROOT = '\\Assets\\master_item.html'
QUESTION_SCRIPT_TEMPLATE_ROOT = '\\Assets\\question_script_template.json'

MASTER_MINIMUM_COUNT = 6
MASTER_MAXIMUM_COUNT = 14
MASTER_ABOUT_COUNT = 4

# Num thread for task
NUM_THREADS = 8
# If true mark generated sites in google_sheets
MAKE_REPORT = True
# Google sheets send packet size
GOOGLE_BLOCK_SIZE = 250
DEBUG = False
DEBUG_SITE_COUNT = 500


class SitesGenerator:
    def __init__(self, master_about_csv_file, reviews_csv_file, root_path):
        self.container_df = pd.DataFrame()
        self.master_data_df = pd.DataFrame()
        self.selection_master_df = pd.DataFrame()
        self.master_education_df = pd.DataFrame()
        self.review_df = pd.DataFrame()
        self.section_df = pd.DataFrame()

        self.domains = {}

        self.master_maximum_count = MASTER_MAXIMUM_COUNT
        self.master_minimum_count = MASTER_MINIMUM_COUNT

        self.root_path = root_path

        # Load master_about
        if not os.path.exists(reviews_csv_file):
            print('Bad path to goods_bd file!')
            raise Exception('Bad path to goods_bd file!')

        self.master_about_csv_file = master_about_csv_file
        self.master_about_df = pd.read_csv(master_about_csv_file, sep='\t')
        self.master_about_df = self.master_about_df.sample(frac=1).reset_index(drop=True)
        self.master_about_df['used'] = 0.0

        # Load reviews
        if not os.path.exists(reviews_csv_file):
            print('Bad path to goods_bd file!')
            raise Exception('Bad path to goods_bd file!')

        self.reviews_csv_file = reviews_csv_file
        self.review_df = pd.read_csv(reviews_csv_file, sep='\t')
        self.review_df = self.review_df.sample(frac=1).reset_index(drop=True)
        self.review_df['used'] = 0.0

    def download_data(self, token, table_id):
        # Downloading data from sheets
        sheets = GoogleSheetsApi(token)
        container_data = sheets.get_data_from_sheets(table_id, CONTAINER_LIST, 'A2',
                                'P' + str(sheets.get_list_size(table_id, CONTAINER_LIST)[1]), 'COLUMNS')
        master_data_data = sheets.get_data_from_sheets(table_id, MASTER_DATA_LIST, 'A2',
                                'O' + str(sheets.get_list_size(table_id, MASTER_DATA_LIST)[1]), 'COLUMNS')
        selection_master_data = sheets.get_data_from_sheets(table_id, SELECTION_MASTER_LIST, 'A2',
                                'B' + str(sheets.get_list_size(table_id, SELECTION_MASTER_LIST)[1]), 'COLUMNS')
        master_education_data = sheets.get_data_from_sheets(table_id, MASTER_EDUCATION_LIST, 'A2',
                                'B' + str(sheets.get_list_size(table_id, MASTER_EDUCATION_LIST)[1]), 'COLUMNS')
        section_data = sheets.get_data_from_sheets(table_id, SECTION_LIST, 'B2',
                                'E' + str(sheets.get_list_size(table_id, SECTION_LIST)[1]), 'COLUMNS')
        domains_data = sheets.get_data_from_sheets(table_id, DOMAIN_LIST, 'A2',
                                'E' + str(sheets.get_list_size(table_id, DOMAIN_LIST)[1]), 'ROWS')

        # Data frame filling
        if not DEBUG:
            elements_count = len(container_data[0])
        else:
            elements_count = len(container_data[0][:DEBUG_SITE_COUNT])

        container_data = self.normalize_list(container_data, 16, [])
        self.container_df['sectionId'] = self.normalize_list(container_data[0], elements_count)
        self.container_df['domain'] = [_.strip() for _ in self.normalize_list(container_data[1], elements_count)]
        self.container_df['location'] = self.normalize_list(container_data[2], elements_count)
        self.container_df['urlPath'] = self.normalize_list(container_data[3], elements_count)
        self.container_df['name'] = self.normalize_list(container_data[4], elements_count)
        self.container_df['masterList'] = self.normalize_list(container_data[5], elements_count)
        self.container_df['title'] = self.normalize_list(container_data[6], elements_count)
        self.container_df['description'] = self.normalize_list(container_data[7], elements_count)
        self.container_df['question_1'] = self.normalize_list(container_data[8], elements_count)
        self.container_df['answer_1'] = self.normalize_list(container_data[9], elements_count)
        self.container_df['question_2'] = self.normalize_list(container_data[10], elements_count)
        self.container_df['answer_2'] = self.normalize_list(container_data[11], elements_count)
        self.container_df['question_3'] = self.normalize_list(container_data[12], elements_count)
        self.container_df['answer_3'] = self.normalize_list(container_data[13], elements_count)
        self.container_df['add'] = self.to_bool_list(self.normalize_list(container_data[14], elements_count))
        self.container_df['First_add'] = self.to_bool_list(self.normalize_list(container_data[15], elements_count))
        self.container_df['generated'] = self.normalize_list([False], elements_count)

        if DEBUG:
            print(self.container_df.head(5).values)

        elements_count = len(master_data_data[0])
        self.master_data_df['path'] = master_data_data[0]
        self.master_data_df['initials'] = self.normalize_list(master_data_data[1], elements_count)
        self.master_data_df['logoPath'] = self.normalize_list(master_data_data[2], elements_count)
        self.master_data_df['rate'] = self.normalize_list(master_data_data[3], elements_count)
        self.master_data_df['amount_reviews'] = self.normalize_list(master_data_data[4], elements_count)

        elements_count = len(selection_master_data[0])
        self.selection_master_df['sectionId'] = selection_master_data[0]
        self.selection_master_df['pathMaster'] = self.normalize_list(selection_master_data[1], elements_count)

        if len(master_education_data) > 0:
            elements_count = len(master_education_data[0])
            self.master_education_df['masterDataId'] = master_education_data[0]
            self.master_education_df['education'] = self.normalize_list(master_education_data[1], elements_count)
        else:
            self.master_education_df['masterDataId'] = ''
            self.master_education_df['education'] = ''

        elements_count = len(section_data[0])
        self.section_df['name'] = section_data[0]
        self.section_df['cost_service_from'] = self.normalize_list(section_data[2], elements_count)
        self.section_df['cost_service_to'] = self.normalize_list(section_data[3], elements_count)

        # self.domains
        for domain in domains_data:
            self.domains[domain[0].strip()] = [domain[3], float(domain[4]), domain[1], domain[2]]

        if DEBUG:
            print(self.domains.keys())

    @staticmethod
    def normalize_list(arr, size, placeholder=''):
        if len(arr) <= size:
            return arr + ([placeholder]*(size-len(arr)))
        else:
            return arr[:size]

    @staticmethod
    def to_bool_list(arr):
        return [True if len(i) > 0 else False for i in arr]

    def gen_sites(self, token, table_id, out_directory):
        # Gen folders
        for domain in self.domains.keys():
            if not os.path.exists(out_directory+self.domain_to_root(domain)):
                os.mkdir(out_directory+self.domain_to_root(domain))

        # Generate sites
        print('Gen first')
        firsts_sites = self.container_df[self.container_df['First_add'] == True].values
        self.gen_sites_by_list_fast(out_directory, firsts_sites)

        print('Gen next')
        not_firsts_sites = self.container_df[self.container_df['First_add'] == False].values
        self.gen_sites_by_list_fast(out_directory, not_firsts_sites)

        # Link sites
        print('Link sites')
        generated_sites = self.container_df[self.container_df['generated'] == True].values
        for site in tqdm(generated_sites):
            self.link_site(out_directory, site)
            self.container_df.loc[(self.container_df.domain == site[1]) &\
                                  (self.container_df.urlPath == site[3]), 'add'] = True

        # Mapping sites
        print('Mapping sites')
        sites_maps = {}
        for domain in self.domains.keys():
            map_text = '<?xml version="1.0" encoding="UTF-8"?>\n' \
                       '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
            map_text += self.gen_site_map_block(domain)
            sites_maps[domain] = map_text

        for site in tqdm(generated_sites):
            sites_maps[site[1]] += self.gen_site_map_block(site[1]+'/'+site[3])

        for domain in self.domains.keys():
            sites_maps[domain] += '</urlset>'
            domain_root = self.domain_to_root(domain) + '\\'
            with open(out_directory + domain_root + 'sitemap.xml', 'w', encoding='utf-8') as f:
                f.write(sites_maps[domain])

        # Save added sites in google table
        if MAKE_REPORT:
            print('Make report')
            sheets = GoogleSheetsApi(token)
            add_list = self.container_df['add'].tolist()
            add_list = ['add' if item else '' for item in add_list]
            sheets.put_column_to_sheets_packets(table_id, CONTAINER_LIST, 'O', 2, add_list, GOOGLE_BLOCK_SIZE)

    def gen_sites_by_list(self, out_directory, sites: list):
        for site in tqdm(sites):
            masters = self.get_masters_paths(self.master_minimum_count, self.master_maximum_count, site)
            if len(masters) > 0:
                content = self.get_content(self.master_minimum_count, len(masters), site)
                if len(content) > 0:
                    # Generate site text
                    site_text = self.gen_site_page(site, masters, content)
                    # Save site
                    domain_root = self.domain_to_root(site[1]) + '\\'
                    with open(out_directory+domain_root+site[3]+'.html', 'w', encoding='utf-8') as f:
                        f.write(site_text)
                    # Mark generated
                    self.container_df.loc[self.container_df['urlPath'] == site[3], 'generated'] = True

    def gen_sites_by_list_fast(self, out_directory, sites):
        selection_id_black_list = []

        # Gen master paths parallel
        pool = Pool(NUM_THREADS)
        func = partial(self.get_masters_paths, self.master_minimum_count, self.master_maximum_count)
        sites_masters = []
        for masters in tqdm(pool.imap_unordered(func, sites), total=len(sites)):
            sites_masters.append(masters)

        # Gen reviews one-thread
        sites_content = []
        for i in tqdm(range(len(sites))):
            if len(sites_masters[i]) != 0 and sites[i][0] not in selection_id_black_list:
                content_buf = self.get_content(self.master_minimum_count, len(sites_masters[i]), sites[i])
                sites_content.append(content_buf)
                if len(content_buf) == 0:
                    selection_id_black_list.append(sites[i][0])
            else:
                sites_content.append([])

        # Gen sites parallel
        func = partial(self.gen_site_fast, out_directory)
        func_args = []
        for i in range(len(sites)):
            func_args.append([sites[i], sites_masters[i], sites_content[i]])

        for generated, site in tqdm(pool.imap_unordered(func, func_args), total=len(func_args)):
            if generated:
                self.container_df.loc[(self.container_df.domain == site[1]) &
                                      (self.container_df.urlPath == site[3]), 'generated'] = True

    def gen_site_fast(self, out_directory: str, args: list):
        site = args[0]
        masters = args[1]
        content = args[2]
        site_generated = False

        if len(content) > 0:
            # Generate site text
            site_text = self.gen_site_page(site, masters, content)
            # Save site
            domain_root = self.domain_to_root(site[1]) + '\\'
            with open(out_directory + domain_root + site[3] + '.html', 'w', encoding='utf-8') as f:
                f.write(site_text)
            site_generated = True

        return site_generated, site

    def link_site(self, out_directory, site):
        # Open site
        domain_root = self.domain_to_root(site[1]) + '\\'
        with open(out_directory + domain_root + site[3]+'.html', 'r', encoding='utf-8') as f:
            site_text = f.read()
        site_item = BeautifulSoup(site_text, "html.parser")

        # Local block
        local_list = list(self.container_df.loc[(self.container_df.generated == True) &\
            (self.container_df.location != 'online') & (self.container_df.urlPath != site[3]) &\
            (self.container_df.domain == site[1])][['urlPath', 'name']].values)

        if len(local_list) > 10:
            local_list = random.sample(local_list, 10)

        locale_block = site_item.find('div', {'data-mark': "Container.linksBlock_2"}).parent.ul

        for i in range(len(local_list)):
            new_tag = site_item.new_tag('li')
            new_tag2 = site_item.new_tag('a', href='/' + local_list[i][0])
            if site[1] == 'https://redsale-pro.kz':
                new_tag2['href'] += '.html'
            locale_block.append(new_tag)
            locale_block.find_all('li')[-1].append(new_tag2)
            locale_block.find_all('a')[-1].string = local_list[i][1]

        # Save site
        domain_root = self.domain_to_root(site[1]) + '\\'
        with open(out_directory + domain_root + site[3] + '.html', 'w', encoding='utf-8') as f:
            f.write(str(site_item))

    def gen_site_page(self, site_data, masters, content):
        # Get template of site
        with open(self.root_path+SITE_TEMPLATE_ROOT, 'r', encoding='utf-8') as f:
            site_text = f.read()

        site_item = BeautifulSoup(site_text, "html.parser")

        # Filling template
        # Info
        site_item.head.title.string = site_data[6]
        site_item.find('meta', {'data-mark': 'Container.description'}).attrs['content'] = site_data[7]
        site_item.find('h1', {'data-mark': 'Container.name'}).string = site_data[4]

        # Master list
        site_item.find('h2', {'data-mark': 'Container.masterList'}).string = site_data[5]
        masters_block = site_item.find('h2', {'data-mark': 'Container.masterList'}).parent.div

        for i in range(len(content)):
            master_item = self.gen_master_item(masters[i], content[i][0], content[i][1], site_data, i)
            if master_item is not None:
                masters_block.insert(i, master_item)

        # Questions
        site_item.find('h3', {'data-mark': 'Container.question_1'}).string = site_data[8]
        site_item.find('h3', {'data-mark': 'Container.question_2'}).string = site_data[10]
        site_item.find('h3', {'data-mark': 'Container.question_3'}).string = site_data[12]
        site_item.find('div', {'data-mark': 'Container.answer_1'}).string = site_data[9]
        site_item.find('div', {'data-mark': 'Container.answer_2'}).string = site_data[11]
        site_item.find('div', {'data-mark': 'Container.answer_3'}).string = site_data[13]
        site_item.find('script', {'type': 'application/ld+json'}).string = self.get_questions_script(site_data)

        site_text = self.get_html(site_item)
        site_text = site_text.replace('{GTM_1}', self.domains[site_data[1]][2])
        site_text = site_text.replace('{GTM_2}', self.domains[site_data[1]][3])
        return site_text

    # Get content for masters
    # Return [['review', '...'], ['about', '...'], ...]
    def get_content(self, minimum_entities: int, maximum_entities: int, site):
        reviews_df = self.review_df[(self.review_df.sectionId == site[0]) &
                                    (self.review_df.used == 0.0)].head(maximum_entities)
        master_about_df = self.master_about_df[(self.master_about_df.sectionId == site[0]) &
                                               (self.master_about_df.used == 0.0)]\
            .head(maximum_entities*MASTER_ABOUT_COUNT)

        count = len(reviews_df.index) + math.floor(float(len(master_about_df))/MASTER_ABOUT_COUNT)

        if count < minimum_entities:
            content_list = []
        else:
            content_list = []
            # 0 is review, 1 is about
            random_chance = [0 for i in range(len(reviews_df.index))] + \
                            [1 for i in range(math.floor(float(len(master_about_df))/MASTER_ABOUT_COUNT))]
            random_sample = random.sample(random_chance, min(maximum_entities, count))

            reviews_index = 0
            abouts_index = 0

            for content in random_sample:
                if content == 0:
                    self.review_df.at[reviews_df.index[reviews_index], 'used'] = 1.0
                    content_list.append(['review', self.review_df.iloc[reviews_df.index[reviews_index]]['review']])
                    reviews_index += 1

                elif content == 1:
                    abouts_buf = []
                    for i in range(MASTER_ABOUT_COUNT):
                        self.master_about_df.at[master_about_df.index[abouts_index], 'used'] = 1.0
                        abouts_buf.append(self.master_about_df.iloc[master_about_df.index[abouts_index]]['aboutText'])
                        abouts_index += 1

                    content_list.append(['about', abouts_buf])

        return content_list

    # Getting masters paths equal site selection_id from selection_master_df
    # if masters count less then minimum_masters return [],
    # if count more then maximum_masters return maximum_masters masters
    def get_masters_paths(self, minimum_masters, maximum_masters, site):
        master_paths = self.selection_master_df[self.selection_master_df['sectionId'] == site[0]]['pathMaster']\
            .values
        master_paths = list(filter(lambda x: self.valid_master_path(x), master_paths))

        if len(master_paths) > maximum_masters:
            master_paths = random.sample(master_paths, maximum_masters)
        if len(master_paths) < minimum_masters:
            master_paths = []

        return master_paths

    # Validate master_path
    # If count masters with master_path equal 1 return True, else False
    def valid_master_path(self, master_path):
        masters_count = self.master_data_df[self.master_data_df['path'] == master_path].values
        return len(masters_count) == 1

    def gen_site_map_block(self, site_path):
        return '<url>\n<loc>'+site_path+'</loc>\n<changefreq>weekly</changefreq>\n<priority>1.00</priority>\n</url>'

    def get_html(self, soup):
        site_text = str(soup)
        site_text = site_text.replace('&lt;', '<')
        site_text = site_text.replace('&gt;', '>')
        return site_text

    def get_questions_script(self, site_data):
        with open(self.root_path+QUESTION_SCRIPT_TEMPLATE_ROOT, 'r', encoding='utf-8') as f:
            data = json.load(f)

        data['mainEntity'][0]['name'] = site_data[8]
        data['mainEntity'][0]['acceptedAnswer']['text'] = site_data[9]
        data['mainEntity'][1]['name'] = site_data[10]
        data['mainEntity'][1]['acceptedAnswer']['text'] = site_data[11]
        data['mainEntity'][2]['name'] = site_data[12]
        data['mainEntity'][2]['acceptedAnswer']['text'] = site_data[13]

        return str(json.dumps(data, ensure_ascii=False))

    @staticmethod
    def get_end_script(container_name, url):
        with open('../Assets/end_script_template.json', 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Gen random price
        min_price = random.randint(6, 8) * 100 + random.randint(0, 1) * 50
        if min_price > 800:
            min_price = 800
        max_price = random.randint(12, 15) * 100 + random.randint(0, 1) * 50
        if max_price > 1500:
            max_price = 1500
        random_price = '???? ' + str(min_price) + ' ???? ' + str(max_price) + ' ???'

        data['name'] = container_name
        data['url'] = url
        data['priceRange'] = random_price
        data['aggregateRating']['reviewCount'] = str(random.randint(400, 2500))
        data['aggregateRating']['ratingValue'] = str(random.randint(47, 50) / 10)

        return str(json.dumps(data, ensure_ascii=False))

    def gen_master_item(self, master, info_type, info, site_data, num):
        # Get author data
        masters = self.master_data_df[self.master_data_df['path'] == master].values
        master_data = masters[0]

        # Get template of master item
        with open(self.root_path+MASTER_ITEM_TEMPLATE_ROOT, 'r', encoding='utf-8') as f:
            master_item_text = f.read()

        # Filling template
        # Info
        master_item = BeautifulSoup(master_item_text, "html.parser")
        avatar_src = 'master/' + master_data[2]
        if avatar_src.split('.')[-1] != 'svg':
            avatar_src += '.jpg'
        master_item.find('div', {'data-mark': 'MasterData.logoPath'}).find('img').attrs['src'] = avatar_src
        master_item.find('h4', {'data-mark': 'MasterData.initials'}).string.replace_with(master_data[1])
        master_item.find('div', {'data-mark': 'MasterData.rate'}).find('span').string.replace_with(master_data[3])
        master_item.find('span', {'data-mark': 'MasterData.amount_reviews'})\
            .string.replace_with('????????????: ' + master_data[4])

        # About, review
        if info_type == 'about':
            # Delete review block
            master_item.find('div', {'class': 'master__reviews-text'}).decompose()

            # Filling about block
            about_block = master_item.find('div', {'data-mark': 'MasterAbout.aboutText'}).parent
            about_block.div.p.decompose()
            new_tag = master_item.new_tag('p')
            about_block.div.insert(0, new_tag)
            about_block.find_all('p')[0].attrs['class'] = 'hide-item'
            info_str = '<br>'.join(info)
            about_block.find_all('p')[0].string = info_str

        elif info_type == 'review':
            # Delete about block
            master_item.find('div', {'data-mark': 'MasterAbout.aboutText'}).parent.decompose()

            # Filling review block
            master_item.find('p', {'data-mark': 'ReviewData.review'}).string.replace_with(info)
            reviewers_name = RussianNames().get_person().split(' ')[0]
            master_item.find('div', {'data-mark': 'ReviewData.review_customerName_date'}) \
                .string = '{0} <span>{1}</span>'.format(reviewers_name, self.gen_rand_review_date())

        # Education
        master_education = self.master_education_df[self.master_education_df['masterDataId'] == master]\
            ['education'].values

        education_block = master_item.find('div', {'data-mark': 'MasterEducation.education'}).parent
        education_block.div.p.decompose()
        if len(master_education) > 0:
            for i in range(len(master_education)):
                new_tag = master_item.new_tag('p')
                education_block.div.insert(i, new_tag)
                education_block.find_all('p')[-1].attrs['class'] = 'hide-item'
                education_block.find_all('p')[-1].string = master_education[i]
        else:
            education_block.decompose()

        # Price
        price_range = self.section_df[self.section_df['name'] == site_data[0]]\
            [['cost_service_from', 'cost_service_to']].values[0]
        price_value = float(random.randint(int(price_range[0]), int(price_range[1]))) * self.domains[site_data[1]][1]
        price_currency = self.domains[site_data[1]][0]
        master_item.find('div', {'data-mark': 'MasterData.cost_time'})\
            .string = "<span>" + str(price_value) + ' ' + price_currency + "</span>"

        # Paste nums
        master_item.find('div', {'class': 'master__left _master-1'})['class'] = 'master__left _master-' + str(num + 1)
        master_item.find('div', {'class': 'spollers _spollers-1'})['class'] = 'spollers _spollers-'+str(num+1)
        master_item.find('div', {'data-da': '_master-1,1,991'})['data-da'] = '_master-'+str(num+1)+',1,991'
        master_item.find('div', {'data-da': '_spollers-1,2,991'})['data-da'] = '_spollers-'+str(num+1)+',2,991'

        return self.get_html(master_item)

    # Return random post date
    @staticmethod
    def gen_rand_review_date():
        dates = (
            '??????????????',
            '??????????',
            '2 ?????? ??????????',
            '3 ?????? ??????????',
            '4 ?????? ??????????',
            '5 ???????? ??????????',
            '6 ???????? ??????????',
            '7 ???????? ??????????',
            '10 ???????? ??????????',
            '20 ???????? ??????????',
            '???????????? ???????????? ??????????'
        )
        return dates[random.randint(0, len(dates)-1)]

    # Return string domain with replaced all non letters symbols to _
    @staticmethod
    def domain_to_root(domain: str):
        return re.sub(r'\W', '_', domain)
