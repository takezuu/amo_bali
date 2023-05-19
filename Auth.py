import json
import requests
from pw import UserData
import logging

from paths import my_log

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename=my_log, encoding='utf-8', level=logging.DEBUG)


def authorization() -> json:
    """Первая авторизация и получение токенов"""
    data = {
        "client_id": f'{UserData.CLIENT_ID}',
        "client_secret": f'{UserData.CLIENT_SECRET}',
        "grant_type": "authorization_code",
        "code": f'{UserData.CODE}',
        "redirect_uri": f'{UserData.URI}'
    }

    url_for_token = f'{UserData.CLIENT_URL}/oauth2/access_token'

    try:
        logging.info('Делаю post запрос на первую авторизацию')
        request = requests.post(url_for_token, data=data)
        return request.json()
    except Exception as error:
        logging.error(f'authorization: {error}')


def get_refresh_token(refresh_token: str) -> json:
    """Получение новых токенов"""
    data = {'client_id': f'{UserData.CLIENT_ID}',
            'client_secret': f'{UserData.CLIENT_SECRET}',
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'redirect_uri': f'{UserData.URI}'}

    url_for_token = f'{UserData.CLIENT_URL}/oauth2/access_token'

    try:
        logging.info('Делаю post запрос для получения нового токена')
        request = requests.post(url_for_token, data=data)
        return request.json()
    except Exception as error:
        logging.error(f'get_refresh_token: {error}')
