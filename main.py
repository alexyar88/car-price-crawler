import requests
import json
import gzip
import logging
from datetime import datetime
import boto3
import os

log_format = '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)


def get_page_asd(page_num):
    url = f'https://ab.onliner.by/sdapi/ab.api/search/vehicles?page={page_num}&extended=true&limit=50'
    ads = requests.get(url).json()['adverts']
    for ad in ads:
        del ad['images']
    return ads


def get_all_ads():
    logger.info('Start parsing')
    all_ads = []
    for i in range(1, 2000):
        ads = get_page_asd(i)
        if not ads:
            logger.info(f'Noa ads at page {i}. Stop parsing')
            break
        all_ads.extend(ads)
        if i % 10 == 0:
            logger.info(f'Parsing page {i}')
    return all_ads


def send_tg_message(text):
    bot_token = os.environ['TG_BOT_TOKEN']
    chat_id = os.environ['TG_CHAT_ID']

    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    params = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'HTML',
    }

    requests.get(url, params=params)


def main():
    all_ads = get_all_ads()
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_name_json = f'all_ads_{date_str}.json'
    file_name_gz = file_name_json + '.gz'
    logger.info(f'Saving ads dump to {file_name_json}')
    with open(file_name_json, 'w') as f:
        json.dump(all_ads, f)

    logger.info(f'Gzip ads json to {file_name_gz}')
    with open(file_name_json, 'rb') as src, gzip.open(file_name_gz, 'wb') as dst:
        dst.writelines(src)

    client = boto3.client('s3')
    client.upload_file(file_name_gz, 'auto-price', f'parsed-jsons/{file_name_gz}')
    logger.info(f'Parsed and saved to s3 auto ads: {len(all_ads)}')
    send_tg_message(f'Parsed and saved to s3 auto ads: {len(all_ads)}')


if __name__ == '__main__':
    main()
