import os
from Modules.sites_generator import SitesGenerator
from multiprocessing import freeze_support

"""Options"""
TOKEN_FILE = 'Environment/google_token.json'                 # File with google service auth token.
TABLE_ID = '1ru0qAafWrT7BplWb-TY_hprwfLOVbnfYTX3lfPxLEVE'    # ID Google Sheets document
REVIEWS_CSV_FILE = 'Data/reviews.tsv'                        # Path/name.csv of goods file
MASTER_ABOUTS_CSV_FILE = 'Data/master_abouts.tsv'            # Path/name.csv of goods file
OUT_DIRECTORY = 'D:\\#sites\\'                                # Path of output directory
ASSETS_ROOT = os.path.dirname(os.path.abspath(__file__))


if __name__ == '__main__':
    freeze_support()
    generator = SitesGenerator(MASTER_ABOUTS_CSV_FILE, REVIEWS_CSV_FILE, ASSETS_ROOT)
    generator.download_data(TOKEN_FILE, TABLE_ID)
    generator.gen_sites(TOKEN_FILE, TABLE_ID, OUT_DIRECTORY)

