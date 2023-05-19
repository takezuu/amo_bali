import os
import DB_Operations
import DataFunc
from Auth import get_refresh_token, authorization
from api_methods import api_requests
import logging

from paths import my_log, my_f

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename=my_log, encoding='utf-8', level=logging.DEBUG)


# декораторы
def api_decorator(func):
    """Цикл while для api функций"""

    def inner(api_name: str):
        try:
            logging.info('Запускаю api_decorator')
            tokens = DB_Operations.read_tokens()
            page_num = 1
            req = True
            while req:
                req = func(page_num, api_name, tokens)
                page_num += 1

        except Exception as error:
            logging.error(f'api_decorator: {error}')

    return inner


def general_decorator(func):
    """Цикл while для функций"""

    def inner(*args, **kwargs):
        try:
            logging.info('Запускаю general_decorator')
            page_num = 1
            req = True
            while req:
                req = func(page_num, *args, **kwargs)
                page_num += 1
        except Exception as error:
            logging.error(f'general_decorator: {error}')

    return inner


def insert_decorator_reverse(func):
    """Цикл while для функций, которые записывают данные в базу"""

    def inner(*args, **kwargs):
        try:
            logging.info('Запускаю insert_decorator_reverse')
            count_files = len(os.listdir('Lead_status_changed'))
            logging.info(f'count files: {count_files}')
            page_num = count_files
            req = True
            while req:
                req = func(page_num, *args, **kwargs)
                page_num -= 1

        except Exception as error:
            logging.error(f'insert_decorator_reverse: {error}')

    return inner


@insert_decorator_reverse
def update_insert_reverse(page_num: int, funcc, insert_funcc,
                          name_of_data: str = None, extra_prefix: str = None) -> bool:
    """Обновление записей в базу"""
    try:
        logging.info(f'Работает update_insert_reverse, page {page_num}')
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=name_of_data, extra_prefix=extra_prefix)
    except FileNotFoundError:
        return False
    records_to_insert = funcc(data=file_data)
    insert_funcc(records_to_insert)
    return True


@insert_decorator_reverse
def first_insert_reverse(page_num: int, funcc, insert_funcc,
                         name_of_data: str = None, extra_prefix: str = None) -> bool:
    """Первая запись в базу"""
    try:
        logging.info(f'Работает first_insert_reverse, page {page_num}')
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=name_of_data, extra_prefix=extra_prefix)
    except FileNotFoundError:
        return False
    records_to_insert = funcc(data=file_data)
    insert_funcc(records_to_insert)
    return True


# функции
@api_decorator
def create_api(page_num: int, api_name: str, tokens: tuple) -> bool:
    """Формирование файлов с данными из api"""
    try:
        logging.info(f'Работает create_api, page {page_num}')
        data = api_requests.api_get_request(tokens=tokens, page_num=page_num, api_name=api_name)
        DataFunc.write_data(data=data, page_num=page_num, name_of_data=api_name)
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=api_name)
        return DataFunc.check_next_api_page(file_data=file_data)
    except Exception as error:
        logging.error(f'create_api: {error}')


@api_decorator
def create_api_filter(page_num: int, api_name: str, tokens: tuple) -> bool:
    """Формирование файлов с данными из api"""
    try:
        logging.info(f'Работает create_api_filter, page {page_num}')
        data = api_requests.api_filter_get_request(tokens=tokens, page_num=page_num, api_name=api_name)
        DataFunc.write_data(data=data, page_num=page_num, name_of_data=api_name)
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=api_name)
        return DataFunc.check_next_api_page(file_data=file_data)
    except Exception as error:
        logging.error(f'create_api_filter: {error}')


@general_decorator
def create_dict(page_num: int, funcc, dict_name: str, second_dict_name: str = None,
                prefix: str = None, extra_prefix: str = None) -> bool:
    """Формирование словарей"""
    try:
        logging.info(f'Работает create_dict, page {page_num}')
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=dict_name, extra_prefix=extra_prefix)
    except FileNotFoundError:
        return False
    created_dict = funcc(file_data)
    DataFunc.write_data(created_dict, page_num=page_num, name_of_data=dict_name, prefix=prefix,
                        second_dict_name=second_dict_name)
    return True


@general_decorator
def first_insert(page_num: int, funcc, insert_funcc, name_of_data: str = None, extra_prefix: str = None) -> bool:
    """Первая запись в базу"""
    try:
        logging.info(f'Работает first_insert, page {page_num}')
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=name_of_data, extra_prefix=extra_prefix)
    except FileNotFoundError:
        return False
    records_to_insert = funcc(data=file_data)
    insert_funcc(records_to_insert)
    return True


@general_decorator
def update_insert(page_num: int, funcc, insert_funcc, name_of_data: str = None, extra_prefix: str = None) -> bool:
    """Обновление записей в базу"""
    try:
        logging.info(f'Работает update_insert, page {page_num}')
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=name_of_data, extra_prefix=extra_prefix)
    except FileNotFoundError:
        return False
    records_to_insert = funcc(data=file_data)
    insert_funcc(records_to_insert)
    return True


@api_decorator
def get_deleated_lead(page_num: int, api_name: str, tokens: tuple) -> bool:
    try:
        logging.info(f'Работает get_deleated_lead, page {page_num}')
        deleted_leads = api_requests.api_get_deleted_leads(tokens=tokens, page_num=page_num)
        DataFunc.write_data(data=deleted_leads, name_of_data='Deleted_leads', page_num=page_num)
        file_data = DataFunc.read_data_file(name_of_data='Deleted_leads', page_num=page_num)
        return DataFunc.check_next_api_page(file_data=file_data)
    except Exception as error:
        logging.error(f'get_deleated_lead: {error}')


@general_decorator
def delete_deleted_leads(page_num: int, funcc, delete_funcc, name_of_data: str = None) -> bool:
    try:
        logging.info(f'Работает delete_deleted_leads, page {page_num}')
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=name_of_data)
        logging.info(f'page {page_num}')
    except FileNotFoundError:
        return False
    records_to_delete = funcc(data=file_data)
    delete_funcc(records_to_delete)
    return True


def get_token() -> None:
    """Получает первый токен и записывает в базу"""
    try:
        logging.info('Получаю первый токен')
        new_tokens = authorization()
        # DataFunc.write_tokens(tokens=new_tokens)
        DB_Operations.insert_tk_table(records_to_insert=new_tokens)
        logging.info('Получил access_token')
    except Exception as error:
        logging.error(f'get_token: {error}')


def update_token() -> None:
    """Обновляет токен в базе"""
    try:
        logging.info('Обновляю токен')
        tokens = DB_Operations.read_tokens()
        # tokens = DataFunc.read_token()
        # new_tokens = get_refresh_token(refresh_token=tokens['refresh_token'])
        new_tokens = get_refresh_token(refresh_token=tokens[1])
        # DataFunc.write_tokens(tokens=new_tokens)
        DB_Operations.insert_tk_table(records_to_insert=new_tokens)
        logging.info('Получил новый access_token')
    except Exception as error:
        logging.error(f'update_token: {error}')


def delete_all_files() -> None:
    """Удаляет все загруженные файлы"""
    try:
        logging.info('Удаляю файлы')
        user_dirs = ['Leads/', 'Users/', 'Pipelines/', 'Lead_status_changed/', 'Dict/', 'Deleted_leads/']

        for user_dir in user_dirs:
            for file in os.listdir(my_f + user_dir):
                logging.info(f'delete {file}')
                if file != 'group_dict1':
                    if file != 'block_pipelines_dict1':
                        os.remove(os.path.join(my_f + user_dir, file))

    except Exception as error:
        logging.error(f'delete_all_files: {error}')
