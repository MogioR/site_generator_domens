from google_sheets_api import GoogleSheetsApi
from sites_generator import SitesGenerator
import pandas as pd

"""File with google service auth token."""
TOKEN_FILE = 'token.json'
"""ID Google Sheets document"""
TABLE_ID = '1mZb-JiEzSSqyxXsQeuo1NpentUqDSJLkzDkgqLE3Vy4'
TABLE_TEST_ID = '18CSD7sNaJWQ4DDOv6omd0J2jSYuT7xjlKCyAxSdz-QQ'

REVIEWS_CSV_FILE = 'goods.csv'
OUT_DIRECTORY = 'S:\\sites\\'

generator = SitesGenerator(REVIEWS_CSV_FILE)
generator.download_data(TOKEN_FILE, TABLE_ID)
generator.gen_sites(OUT_DIRECTORY)

