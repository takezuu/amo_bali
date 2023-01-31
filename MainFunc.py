import datetime
import os

import DB_Operations
import DataFunc
from Auth import get_refresh_token, authorization
from api_methods import api_requests


# декораторы
def api_decorator(func):
    """Цикл while для api функций"""
    def inner(api_name: str):
        try:
            tokens = DB_Operations.read_tokens()
            page_num = 1
            req = True
            while req:
                req = func(page_num, api_name, tokens)
                page_num += 1

        except Exception as error:
            print(f'api_decorator: {error}')
    return inner



def dict_decorator(func):
    """Цикл while для функций создающие словари"""
    def inner(*args, **kwargs):
        try:
            page_num = 1
            req = True
            while req:
                req = func(page_num, *args, **kwargs)
                page_num += 1
        except Exception as error:
            print(f'dict_decorator: {error}')

    return inner



def insert_decorator(func):
    """Цикл while для функций, которые записывают данные в базу"""
    def inner(*args, **kwargs):
        try:
            page_num = 1
            req = True
            while req:
                req = func(page_num, *args, **kwargs)
                page_num += 1
        except Exception as error:
            print(f'insert_decorator: {error}')
    return inner



def delete_decorator(func):
    """Цикл while для функций, которые записывают данные в базу"""
    def inner(*args, **kwargs):
        try:

            page_num = 1
            req = True
            while req:
                req = func(page_num, *args, **kwargs)
                page_num += 1

        except Exception as error:
            print(f'delete_decorator: {error}')

    return inner



def insert_decorator_reverse(func):
    """Цикл while для функций, которые записывают данные в базу"""

    def inner(*args, **kwargs):
        try:
            count_files = len(os.listdir('Lead_status_changed'))
            print(count_files)
            page_num = count_files
            req = True
            while req:
                req = func(page_num, *args, **kwargs)
                page_num -= 1

        except Exception as error:
            print(f'insert_decorator_reverse: {error}')

    return inner

@insert_decorator_reverse
def update_insert_reverse(page_num: int, funcc, insert_funcc, name_of_data=None, extra_prefix=None, **kwargs):
    """Обновление записей в базу"""
    try:
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=name_of_data, extra_prefix=extra_prefix)
        print(page_num, 'page')
    except FileNotFoundError:
        return False
    records_to_insert = funcc(data=file_data, **kwargs)
    insert_funcc(records_to_insert)
    return True


@insert_decorator_reverse
def first_insert_reverse(page_num: int, funcc, insert_funcc, name_of_data=None, extra_prefix=None, **kwargs):
    """Первая запись в базу"""
    try:
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=name_of_data, extra_prefix=extra_prefix)
        print(page_num, 'page')
    except FileNotFoundError:
        return False
    records_to_insert = funcc(data=file_data, **kwargs)
    insert_funcc(records_to_insert)
    return True
# функции
@api_decorator
def create_api(page_num: int, api_name: str, tokens):
    """Формирование файлов с данными из api"""
    try:
        data = api_requests.api_get_request(tokens=tokens, page_num=page_num, api_name=api_name)
        DataFunc.write_data(data=data, page_num=page_num, name_of_data=api_name)
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=api_name)
        return DataFunc.check_next_api_page(file_data=file_data)
    except Exception as error:
        print(f'create_api: {error}')


@api_decorator
def create_api_filter(page_num: int, api_name: str, tokens):
    """Формирование файлов с данными из api"""
    try:
        data = api_requests.api_filter_get_request(tokens=tokens, page_num=page_num, api_name=api_name)
        DataFunc.write_data(data=data, page_num=page_num, name_of_data=api_name)
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=api_name)
        return DataFunc.check_next_api_page(file_data=file_data)
    except Exception as error:
        print(f'create_api_filter: {error}')


@dict_decorator
def create_dict(page_num: int, funcc, dict_name: str, second_dict_name=None, prefix=None, extra_prefix=None):
    """Формирование словарей"""
    try:
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=dict_name, extra_prefix=extra_prefix)
    except FileNotFoundError:
        return False
    created_dict = funcc(file_data)
    DataFunc.write_data(created_dict, page_num=page_num, name_of_data=dict_name, prefix=prefix,
                        second_dict_name=second_dict_name)
    return True


@insert_decorator
def first_insert(page_num: int, funcc, insert_funcc, name_of_data=None, extra_prefix=None, **kwargs):
    """Первая запись в базу"""
    try:
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=name_of_data, extra_prefix=extra_prefix)
        print(page_num, 'page')
    except FileNotFoundError:
        return False
    records_to_insert = funcc(data=file_data, **kwargs)
    insert_funcc(records_to_insert)
    return True


@insert_decorator
def update_insert(page_num: int, funcc, insert_funcc, name_of_data=None, extra_prefix=None, **kwargs):
    """Обновление записей в базу"""
    try:
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=name_of_data, extra_prefix=extra_prefix)
        print(page_num, 'page')
    except FileNotFoundError:
        return False
    records_to_insert = funcc(data=file_data, **kwargs)
    insert_funcc(records_to_insert)
    return True


@api_decorator
def get_deleated_lead(page_num: int, api_name, tokens):
    try:
        deleted_leads = api_requests.api_get_deleted_leads(tokens=tokens, page_num=page_num)
        DataFunc.write_data(data=deleted_leads, name_of_data='Deleted_leads', page_num=page_num)
        file_data = DataFunc.read_data_file(name_of_data='Deleted_leads', page_num=page_num)
        return DataFunc.check_next_api_page(file_data=file_data)
    except Exception as error:
        print(f'get_deleated_lead: {error}')


@delete_decorator
def delete_deleted_leads(page_num: int, funcc, delete_funcc, name_of_data=None, extra_prefix=None, **kwargs):
    try:
        file_data = DataFunc.read_data_file(page_num=page_num, name_of_data=name_of_data)
        print(page_num, 'page')
    except FileNotFoundError:
        return False
    records_to_delete = funcc(data=file_data, **kwargs)
    delete_funcc(records_to_delete)
    return True


def get_token():
    """Получает первый токен"""
    try:
        new_tokens = authorization()
        DataFunc.write_tokens(tokens=new_tokens)
        DB_Operations.insert_tk_table(records_to_insert=new_tokens)
        print('Got access_token')
    except Exception as error:
        print(f'get_token: {error}')


def update_token():
    """Обновляет токен"""
    try:
        tokens = DB_Operations.read_tokens()
        # tokens = DataFunc.read_token()
        # new_tokens = get_refresh_token(refresh_token=tokens['refresh_token'])
        new_tokens = get_refresh_token(refresh_token=tokens[1])
        DataFunc.write_tokens(tokens=new_tokens)
        DB_Operations.insert_tk_table(records_to_insert=new_tokens)
        print('Got new access_token')
    except Exception as error:
        print(f'update_token: {error}')


def pipeline_statuses_count():
    """Собирает кол-во сделок на каждом этапе и записывает их в базу"""
    try:
        statuses = ['Неразобранное', 'Получена новая заявка', 'Заявка_взята в работу', 'Клиент квалифицирован',
                    'Демонстрация назначена', 'Демонстрация проведена', 'КП отправлено', 'Оплата согласована',
                    'Договор отправлен', 'Счет выставлен', 'Внесена предоплата', 'Успешно реализовано',
                    'Закрыто и не реализовано']
        count_list = [datetime.datetime.today().date()]
        for status in statuses:
            count = DB_Operations.select_pipeline_status_count(status)
            count_list.append(*count)

        if count_list:
            print(count_list)
            DB_Operations.insert_pipeline_table(count_list)
    except Exception as error:
        print(f'pipeline_statuses_count: {error}')


def delete_all_files():
    try:
        dirs = ['Leads/', 'Users/', 'Pipelines/', 'Lead_status_changed/', 'Dict/', 'Deleted_leads/']

        for dir in dirs:
            for f in os.listdir(dir):
                print(f)
                if f != 'group_dict1':
                    if f != 'block_pipelines_dict1':
                        os.remove(os.path.join(dir, f))
    except Exception as error:
        print(f'delete_all_files: {error}')