import json
import requests
from pw import UserData


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
        request = requests.post(url_for_token, data=data)
        return request.json()
    except Exception as error:
        print(f'authorization: {error}')



def get_refresh_token(refresh_token: str) -> json:
    """Получение новых токенов"""
    data = {'client_id': f'{UserData.CLIENT_ID}',
            'client_secret': f'{UserData.CLIENT_SECRET}',
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'redirect_uri': f'{UserData.URI}'}

    url_for_token = f'{UserData.CLIENT_URL}/oauth2/access_token'
    try:
        request = requests.post(url_for_token, data=data)
        return request.json()
    except Exception as error:
        print(f'get_refresh_token: {error}')

