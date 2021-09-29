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
        self.container_df['add'] = self.expand_list(container_data[13], elements_count)
        self.container_df['First_add'] = self.expand_list(container_data[14], elements_count)

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
        # Get sites with first_add
        firsts_sites = self.container_df[self.container_df['First_add'] != '']
        print(firsts_sites)

    def gen_master_item(self, master):
        # Get author data
        masters = self.master_data_df[self.master_data_df['path'] == master].values
        if len(masters) != 1:
            print('Error master_data: ', master)
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