from google_sheets_api import GoogleSheetsApi
from sites_generator import SitesGenerator
import pandas as pd

"""File with google service auth token."""
TOKEN_FILE = 'token.json'
"""ID Google Sheets document"""
TABLE_ID = '1mZb-JiEzSSqyxXsQeuo1NpentUqDSJLkzDkgqLE3Vy4'
TABLE_TEST_ID = '18CSD7sNaJWQ4DDOv6omd0J2jSYuT7xjlKCyAxSdz-QQ'

generator = SitesGenerator()
generator.download_data(TOKEN_FILE, TABLE_ID)
# generator.gen_sites()
print(generator.gen_master_item("agibalov-dl"))




