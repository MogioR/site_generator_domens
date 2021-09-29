import pandas as pd
from google_sheets_api import GoogleSheetsApi
from bs4 import BeautifulSoup
import random

"""List names for downloading"""
CONTAINER_LIST = 'Container'
MASTER_DATA_LIST = 'MasterData'
MASTER_ABOUT_LIST = 'MasterAbout'
SELECTION_MASTER_LIST = 'SectionMaster'
MASTER_EDUCATION = 'MasterEducation'

MASTER_MINIMUM_COUNT = 6
MASTER_MAXIMUM_COUNT = 14

OUT_DIRECTORY = 'S:\\sites\\'

class SitesGenerator:
    def __init__(self):
        self.container_df = pd.DataFrame()
        self.master_data_df = pd.DataFrame()
        self.master_about_df = pd.DataFrame()
        self.selection_master_df = pd.DataFrame()
        self.master_education_df = pd.DataFrame()
        random.seed()

    def download_data(self, token, table_id):
        # Downloading data from sheets
        sheets = GoogleSheetsApi(token)
        container_data = sheets.get_data_from_sheets(table_id, CONTAINER_LIST, 'A2',
                                'O' + str(sheets.get_list_size(table_id, CONTAINER_LIST)[1]), 'COLUMNS')
        master_data_data = sheets.get_data_from_sheets(table_id, MASTER_DATA_LIST, 'A2',
                                'O' + str(sheets.get_list_size(table_id, MASTER_DATA_LIST)[1]), 'COLUMNS')
        master_about_data = sheets.get_data_from_sheets(table_id, MASTER_ABOUT_LIST, 'A2',
                                'C' + str(sheets.get_list_size(table_id, MASTER_ABOUT_LIST)[1]), 'COLUMNS')
        selection_master_data = sheets.get_data_from_sheets(table_id, SELECTION_MASTER_LIST, 'A2',
                                'B' + str(sheets.get_list_size(table_id, SELECTION_MASTER_LIST)[1]), 'COLUMNS')
        master_education_data = sheets.get_data_from_sheets(table_id, MASTER_EDUCATION, 'A2',
                                'B' + str(sheets.get_list_size(table_id, MASTER_EDUCATION)[1]), 'COLUMNS')

        # Data frame filling
        elements_count = len(container_data[0])
        self.container_df['sectionId'] = container_data[0]
        self.container_df['location'] = self.expand_list(container_data[1], elements_count)
        self.container_df['urlPath'] = self.expand_list(container_data[2], elements_count)
        self.container_df['name'] = self.expand_list(container_data[3], elements_count)
        self.container_df['masterList'] = self.expand_list(container_data[4], elements_count)
        self.container_df['title'] = self.expand_list(container_data[5], elements_count)
        self.container_df['description'] = self.expand_list(container_data[6], elements_count)
        self.container_df['question_1'] = self.expand_list(container_data[7], elements_count)
        self.container_df['answer_1'] = self.expand_list(container_data[8], elements_count)
        self.container_df['question_2'] = self.expand_list(container_data[9], elements_count)
        self.container_df['answer_2'] = self.expand_list(container_data[10], elements_count)
        self.container_df['question_3'] = self.expand_list(container_data[11], elements_count)
        self.container_df['answer_3'] = self.expand_list(container_data[12], elements_count)
        self.container_df['add'] = self.to_bool_list(self.expand_list(container_data[13], elements_count))
        self.container_df['First_add'] = self.to_bool_list(self.expand_list(container_data[14], elements_count))
        self.container_df['generated'] = self.expand_list([False], elements_count)

        elements_count = len(master_data_data[0])
        self.master_data_df['path'] = master_data_data[0]
        self.master_data_df['initials'] = self.expand_list(master_data_data[1], elements_count)
        self.master_data_df['logoPath'] = self.expand_list(master_data_data[2], elements_count)
        self.master_data_df['rate'] = self.expand_list(master_data_data[3], elements_count)
        self.master_data_df['amount_reviews'] = self.expand_list(master_data_data[4], elements_count)
        self.master_data_df['amount_lessons'] = self.expand_list(master_data_data[5], elements_count)
        self.master_data_df['cost_time'] = self.expand_list(master_data_data[6], elements_count)
        self.master_data_df['work_online'] = self.to_bool_list(self.expand_list(master_data_data[7], elements_count))
        self.master_data_df['consultation'] = self.to_bool_list(self.expand_list(master_data_data[8], elements_count))
        self.master_data_df['under_school'] = self.to_bool_list(self.expand_list(master_data_data[9], elements_count))
        self.master_data_df['junior_school'] = self.to_bool_list(self.expand_list(master_data_data[10], elements_count))
        self.master_data_df['middle_school'] = self.to_bool_list(self.expand_list(master_data_data[11], elements_count))
        self.master_data_df['high_school'] = self.to_bool_list(self.expand_list(master_data_data[12], elements_count))
        self.master_data_df['students'] = self.to_bool_list(self.expand_list(master_data_data[13], elements_count))
        self.master_data_df['adults'] = self.to_bool_list(self.expand_list(master_data_data[14], elements_count))

        elements_count = len(master_about_data[0])
        self.master_about_df['id'] = master_about_data[0]
        self.master_about_df['masterDataId'] = self.expand_list(master_about_data[1], elements_count)
        self.master_about_df['aboutText'] = self.expand_list(master_about_data[2], elements_count)

        elements_count = len(selection_master_data[0])
        self.selection_master_df['sectionId'] = selection_master_data[0]
        self.selection_master_df['pathMaster'] = self.expand_list(selection_master_data[1], elements_count)

        elements_count = len(master_education_data[0])
        self.master_education_df['masterDataId'] = master_education_data[0]
        self.master_education_df['education'] = self.expand_list(master_education_data[1], elements_count)

    def expand_list(self, arr, size, placeholder=''):
        return arr + ([placeholder]*(size-len(arr)))

    def to_bool_list(self, arr):
        return [True if len(i) > 0 else False for i in arr]

    def gen_sites(self):
        # Generate sites
        firsts_sites = self.container_df[self.container_df['First_add'] == True].values
        for site in firsts_sites:
            masters = list(self.get_masters(site[0]))
            masters = list(filter(lambda x : self.master_check(x), masters))
            if len(masters) >= MASTER_MINIMUM_COUNT:
                if len(masters) > MASTER_MAXIMUM_COUNT:
                    masters = random.sample(masters, MASTER_MAXIMUM_COUNT)
                    site_text = self.gen_site(site, masters)
                    with open(OUT_DIRECTORY+site[2]+'.html', 'w', encoding='utf-8') as f:
                        f.write(site_text)
                    self.container_df.loc[self.container_df['urlPath'] == site[2], 'generated'] = True
            else:
                pass

        # not_firsts_sites = self.container_df[self.container_df['First_add'] == False].values
        # for site in not_firsts_sites:
        #     masters = list(self.get_masters(site[0]))
        #     masters = list(filter(lambda x : self.master_check(x), masters))
        #     if len(masters) >= MASTER_MINIMUM_COUNT:
        #         if len(masters) > MASTER_MAXIMUM_COUNT:
        #             masters = random.sample(masters, MASTER_MAXIMUM_COUNT)
        #             site_text = self.gen_site(site, masters)
        #             with open(OUT_DIRECTORY+site[2]+'.html', 'w', encoding='utf-8') as f:
        #                 f.write(site_text)
        #             self.container_df.loc[self.container_df['urlPath'] == site[2], 'generated'] = True
        #     else:
        #         pass

        # Link sites
        generated_sites = self.container_df[self.container_df['generated'] == True].values
        for site in generated_sites:
            self.link_site(site)
            self.container_df.loc[self.container_df['urlPath'] == site[2], 'add'] = True

    def link_site(self, site):
        # Open site
        with open(OUT_DIRECTORY+site[2]+'.html', 'r', encoding='utf-8') as f:
            site_text = f.read()
        site_item = BeautifulSoup(site_text, "html.parser")

        # Online block
        online_list = list(self.container_df.loc[(self.container_df.generated == True) &\
            (self.container_df.location == 'online') & (self.container_df.urlPath != site[2])]\
            [['urlPath', 'name']].values)

        if len(online_list) > 10:
            online_list = random.sample(online_list, 10)

        online_block = site_item.find('div', {'data-mark': "Container.linksBlock_1"}).parent.ul

        for i in range(len(online_list)):
            new_tag = site_item.new_tag('li')
            new_tag2 = site_item.new_tag('a', href='/'+online_list[i][0])
            online_block.append(new_tag)
            online_block.find_all('li')[-1].append(new_tag2)
            online_block.find_all('a')[-1].string = online_list[i][1]

        # Local block
        local_list = list(self.container_df.loc[(self.container_df.generated == True) &\
            (self.container_df.location != 'online') & (self.container_df.urlPath != site[2])]\
            [['urlPath', 'name']].values)

        if len(local_list) > 10:
            local_list = random.sample(local_list, 10)

        locale_block = site_item.find('div', {'data-mark': "Container.linksBlock_2"}).parent.ul

        for i in range(len(local_list)):
            new_tag = site_item.new_tag('li')
            new_tag2 = site_item.new_tag('a', href='/' + local_list[i][0])
            locale_block.append(new_tag)
            locale_block.find_all('li')[-1].append(new_tag2)
            locale_block.find_all('a')[-1].string = local_list[i][1]


        # Save site
        with open(OUT_DIRECTORY + site[2] + '.html', 'w', encoding='utf-8') as f:
            f.write(str(site_item))

    def gen_site(self, site_data, masters):
        # Get template of site
        with open('template.html', 'r', encoding='utf-8') as f:
            site_text = f.read()
        site_item = BeautifulSoup(site_text, "html.parser")

        # Filling template
        # Info
        site_item.head.title.string = site_data[5]
        site_item.find('meta', {'data-mark': 'Container.description'}).attrs['content'] = site_data[6]
        site_item.find('h1', {'data-mark': 'Container.name'}).string = site_data[3]

        # Master list
        site_item.find('h2', {'data-mark': 'Container.masterList'}).string = site_data[4]
        masters_block = site_item.find('h2', {'data-mark': 'Container.masterList'}).parent.div
        for i in range(len(masters)):
            master_item = self.gen_master_item(masters[i])
            if master_item is not None:
                masters_block.insert(i, master_item)

        return self.get_html(site_item)

    def get_html(self, soup):
        site_text = str(soup)
        site_text = site_text.replace('&lt;', '<')
        site_text = site_text.replace('&gt;', '>')
        return site_text

    def master_check(self, master):
        masters = self.master_data_df[self.master_data_df['path'] == master].values
        return len(masters) == 1

    def get_masters(self, sectionid):
        return self.selection_master_df[self.selection_master_df['sectionId'] == sectionid]['pathMaster'].values

    def gen_master_item(self, master):
        # Get author data
        masters = self.master_data_df[self.master_data_df['path'] == master].values
        master_data = masters[0]

        # Get template of master item
        with open('master_item.html', 'r', encoding='utf-8') as f:
            master_item_text = f.read()

        # Filling template
        # Info
        master_item = BeautifulSoup(master_item_text, "html.parser")
        master_item.find('div', {'data-mark': 'MasterData.logoPath'}).find('img').attrs['src']\
            = 'master/' + master_data[2]
        master_item.find('h4', {'data-mark': 'MasterData.initials'}).string.replace_with(master_data[1])
        master_item.find('div', {'data-mark': 'MasterData.rate'}).find('span').string.replace_with(master_data[3])
        master_item.find('span', {'data-mark': 'MasterData.amount_reviews'})\
            .string.replace_with('Отзывы: ' + master_data[4])
        master_item.find('div', {'data-mark': 'MasterData.amount_lessons'}) \
            .string.replace_with('Уроки: ' + master_data[5])

        # Reviews
        # TODO FILLING NAMES AND REVIEWS
        master_item.find('div', {'data-mark': 'ReviewData.review_customerName_date'}).find('span')\
            .string.replace_with(self.gen_rand_review_date())

        # About
        master_about = self.master_about_df[self.master_about_df['masterDataId'] == master].sort_values(['id'])\
            ['aboutText'].values

        about_block = master_item.find('div', {'data-mark': 'MasterAbout.aboutText'}).parent
        about_block.div.p.decompose()
        if len(master_about) > 0:
            for i in range(len(master_about)):
                new_tag = master_item.new_tag('p')
                about_block.div.insert(i, new_tag)
                about_block.find_all('p')[-1].attrs['class'] = 'hide-item'
                about_block.find_all('p')[-1].string = master_about[i]
        else:
            about_block.decompose()

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

        # Price, work_online, consultation
        master_item.find('div', {'data-mark': 'MasterData.cost_time'}).find('span')\
            .string.replace_with(master_data[6] + ' ₽')
        if not master_data[7]:
            master_item.find('div', {'data-mark': 'MasterData.work_online'}).decompose()
        if not master_data[8]:
            master_item.find('div', {'data-mark': 'MasterData.consultation'}).decompose()

        # Characteristic
        characteristic_block = master_item.find('div', {'data-mark': 'MasterData.characteristic'}).ul
        characteristic_block.li.decompose()

        if master_data[9]:
            new_tag = master_item.new_tag('li')
            characteristic_block.append(new_tag)
            characteristic_block.find_all('li')[-1].string = 'Дошкольники'

        if master_data[10]:
            new_tag = master_item.new_tag('li')
            characteristic_block.append(new_tag)
            characteristic_block.find_all('li')[-1].string = 'Начальные классы'

        if master_data[11]:
            new_tag = master_item.new_tag('li')
            characteristic_block.append(new_tag)
            characteristic_block.find_all('li')[-1].string = '5-9 классы'

        if master_data[12]:
            new_tag = master_item.new_tag('li')
            characteristic_block.append(new_tag)
            characteristic_block.find_all('li')[-1].string = '10-11 классы'

        if master_data[13]:
            new_tag = master_item.new_tag('li')
            characteristic_block.append(new_tag)
            characteristic_block.find_all('li')[-1].string = 'Студенты'

        if master_data[14]:
            new_tag = master_item.new_tag('li')
            characteristic_block.append(new_tag)
            characteristic_block.find_all('li')[-1].string = 'Взрослые'

        return str(master_item)

    def gen_rand_review_date(self):
        dates = (
            'сегодня',
            'вчера',
            '2 дня назад',
            '3 дня назад',
            '4 дня назад',
            '5 дней назад',
            '6 дней назад',
            '7 дней назад',
            '10 дней назад',
            '20 дней назад',
            'больше месяца назад'
        )
        return dates[random.randint(0, len(dates)-1)]