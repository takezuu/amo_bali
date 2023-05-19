import json
import requests
from pw import UserData
import logging

from paths import my_log

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename=my_log, encoding='utf-8', level=logging.DEBUG)


def api_get_request(tokens: tuple, api_name: str, page_num: int = 1) -> json:
    """Возвращает json запрошенного api"""
    access_token = tokens[0]
    api_call_header = {'Authorization': 'Bearer ' + access_token}

    if api_name == 'pipelines':
        api_name = 'leads/' + api_name
    try:
        logging.info(f'Делаю get запрос {api_name}')
        leads_request = requests.get(f'{UserData.CLIENT_URL}/api/v4/{api_name}?page={page_num}&limit=250',
                                     headers=api_call_header, verify=True)
        return leads_request.json()
    except Exception as error:
        logging.error(f'api_get_request: {error}')


def api_filter_get_request(tokens: tuple, api_name: str, page_num: int = 1) -> json:
    """Возвращает json запрошенного api"""
    access_token = tokens[0]
    api_call_header = {'Authorization': 'Bearer ' + access_token}
    try:
        logging.info(f'Делаю get запрос {api_name}')
        leads_request = requests.get(
            f'{UserData.CLIENT_URL}/api/v4/events?filter[type]={api_name}&page={page_num}&limit=100',
            headers=api_call_header, verify=True)
        return leads_request.json()
    except Exception as error:
        logging.error(f'api_filter_get_request: {error}')


def api_get_deleted_leads(tokens: tuple, page_num: int = 1) -> json:
    """Возвращает json запрошенного api"""
    access_token = tokens[0]
    api_call_header = {'Authorization': 'Bearer ' + access_token}
    try:
        logging.info(f'Делаю get запрос deleted leads {page_num}')
        leads_request = requests.get(
            f'{UserData.CLIENT_URL}/api/v4/leads?with=only_deleted&page={page_num}',
            headers=api_call_header, verify=True)
        return leads_request.json()
    except Exception as error:
        logging.error(f'api_get_deleted_leads: {error}')
