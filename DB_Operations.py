import psycopg2
from pw import DataBase
import logging

from paths import my_log

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename=my_log, encoding='utf-8', level=logging.DEBUG)


def db_decorator(func):
    """Декоратор для записи и подключения к базе"""
    def inner(records_to_insert):
        try:
            if len(records_to_insert) > 0:
                logging.info('Запускаю db_decorator')
                connection = psycopg2.connect(database=DataBase.DATABASE, user=DataBase.USER,
                                              password=DataBase.PASSWORD,
                                              host=DataBase.HOST, port=DataBase.PORT)
                cursor = connection.cursor()
                logging.info('Connection success!')

                func(connection, cursor, records_to_insert)

                connection.close()
                logging.info('Connection close!')
        except TypeError:
            logging.info(f'db_decorator: пришел None')
        except Exception as error:
            logging.error(f'db_decorator: {error}')

    return inner


def db_create_decorator(func):
    """Декоратор для создания сущностей в базе"""
    def inner():
        try:
            logging.info('Запускаю db_create_decorator')
            connection = psycopg2.connect(database=DataBase.DATABASE, user=DataBase.USER, password=DataBase.PASSWORD,
                                          host=DataBase.HOST, port=DataBase.PORT)
            cursor = connection.cursor()
            logging.info('Connection success!')

            func(connection, cursor)

            connection.close()
            logging.info('Connection close!')
        except Exception as error:
            logging.error(f'db_create_decorator: {error}')

    return inner


def db_select_decorator(func):
    """Декоратор для выбора из базы"""
    def inner():
        try:
            logging.info('Запускаю db_select_decorator')
            connection = psycopg2.connect(database=DataBase.DATABASE, user=DataBase.USER, password=DataBase.PASSWORD,
                                          host=DataBase.HOST, port=DataBase.PORT)
            cursor = connection.cursor()
            logging.info('Connection success!')

            tokens = func(cursor)

            connection.close()
            logging.info('Connection close!')
            return tokens
        except Exception as error:
            logging.error(f'db_select_decorator: {error}')

    return inner


def db_delete_decorator(func):
    """Декоратор для удаления из базы"""
    def inner(name_of_table):
        try:
            logging.info('Запускаю db_delete_decorator')
            connection = psycopg2.connect(database=DataBase.DATABASE, user=DataBase.USER, password=DataBase.PASSWORD,
                                          host=DataBase.HOST, port=DataBase.PORT)
            cursor = connection.cursor()
            logging.info('Connection success!')

            func(connection, cursor, name_of_table)

            connection.close()
            logging.info('Connection close!')
        except Exception as error:
            logging.error(f'db_delete_decorator: {error}')

    return inner


def db_delete_leads_decorator(func):
    """Декарот для удаления сделок"""
    def inner(delete_list):
        try:
            logging.info('Запускаю db_delete_decorator_new')
            connection = psycopg2.connect(database=DataBase.DATABASE, user=DataBase.USER, password=DataBase.PASSWORD,
                                          host=DataBase.HOST, port=DataBase.PORT)
            cursor = connection.cursor()
            logging.info('Connection success!')

            func(connection, cursor, delete_list)

            connection.close()
            logging.info('Connection close!')
        except Exception as error:
            logging.error(f'db_delete_decorator_new: {error}')

    return inner


@db_create_decorator
def create_table_leads_table(connection, cursor) -> None:
    """Cоздает таблицу сделок"""
    try:
        logging.info('create_table_leads_table')
        create_query = """CREATE TABLE leads_table(
        ID INT PRIMARY KEY NOT NULL ,
        Название TEXT,
        Бюджет INT,
        Состояние TEXT,
        Ответственный TEXT,
        Группа TEXT,
        Воронка TEXT,
        Этап_воронки TEXT,
        Дата_перехода_на_этап TIMESTAMP,
        Дата_создания TIMESTAMP,
        Дата_изменения TIMESTAMP, 
        Дата_закрытия TIMESTAMP,
        Ближайшая_задача TIMESTAMP,
        Наличие_задачи TEXT,
        Просрочена_задача TEXT);"""
        cursor.execute(create_query)
        connection.commit()
        logging.info('CREATE TABLE LEADS TABLE')
    except Exception as error:
        logging.error(f'create_table_leads_table: {error}')


@db_create_decorator
def create_table_custom_fields_table(connection, cursor) -> None:
    """Cоздает таблицу кастомных полей"""
    try:
        logging.info('create_table_custom_fields_table')
        create_query = """CREATE TABLE custom_fields_table(
        ID INT PRIMARY KEY NOT NULL ,
        Был_в_Новая_заявка TEXT,
        Был_в_Менеджер_назначен TEXT,
        Был_в_Взята_в_работу TEXT,
        Был_в_Попытка_связаться TEXT,
        Был_в_Презентация_отправлена TEXT,
        Был_в_Контакт_состоялся TEXT,
        Был_в_Встреча_назначена TEXT,
        Был_в_Встреча_отменена TEXT,
        Был_в_Встреча_проведена TEXT,
        Был_в_Ожидаем_предоплату TEXT,
        Был_в_Получена_предоплата TEXT,
        Был_в_Реквизиты_получены TEXT,
        Был_в_Договор_у_юриста TEXT,
        Был_в_Подготовка_договора TEXT,
        Был_в_Договор_подготовлен TEXT,
        Был_в_Юристом_согласован TEXT,
        Был_в_Договор_клиенту TEXT,
        Был_в_Договор_согласован TEXT,
        Был_в_Договор_подписан TEXT,
        Был_в_Первый_платеж TEXT,
        Был_в_Второй_платеж TEXT,
        Был_в_Третий_платеж TEXT,
        Был_в_Выиграно TEXT,
        Был_в_Проиграно TEXT,
        Дата_Новая_заявка TIMESTAMP,
        Дата_Менеджер_назначен TIMESTAMP,
        Дата_Взята_в_работу TIMESTAMP,
        Дата_Попытка_связаться TIMESTAMP,
        Дата_Презентация_отправлена TIMESTAMP,
        Дата_Контакт_состоялся TIMESTAMP,
        Дата_Встреча_назначена TIMESTAMP,
        Дата_Встреча_отменена TIMESTAMP, 
        Дата_Встреча_проведена TIMESTAMP,
        Дата_Ожидаем_предоплату TIMESTAMP,
        Дата_Получена_предоплата TIMESTAMP,
        Дата_Реквизиты_получены TIMESTAMP,
        Дата_Подготовка_договора TIMESTAMP,
        Дата_Договор_подготовлен TIMESTAMP,
        Дата_Договор_у_юриста TIMESTAMP,
        Дата_Юристом_согласован TIMESTAMP,
        Дата_Договор_клиенту TIMESTAMP,
        Дата_Договор_согласован TIMESTAMP,
        Дата_Договор_подписан TIMESTAMP,
        Дата_Первый_платеж TIMESTAMP,
        Дата_Второй_платеж TIMESTAMP,
        Дата_Третий_платеж TIMESTAMP,
        Дата_Выиграно TIMESTAMP,
        Дата_Проиграно TIMESTAMP,
        Источник_заявки TEXT,
        Взята TEXT,
        Скорость_взятия TIMESTAMP,
        Этап_отказа TEXT,
        Причина_отказа TEXT,
        Отказ_подробно TEXT,
        Партнер_Агент TEXT,
        Проект TEXT,
        Язык TEXT,
        formname TEXT,
        Классификация_сделки TEXT);"""
        cursor.execute(create_query)
        connection.commit()
        logging.info('CREATE TABLE CUSTOM FIELDS')
    except Exception as error:
        logging.error(f'create_table_custom_fields_table: {error}')


@db_create_decorator
def create_table_utm(connection, cursor) -> None:
    """Cоздает таблицу utm меток"""
    try:
        logging.info('create_table_utm')
        create_query = """CREATE TABLE utm_table(
        ID INT PRIMARY KEY NOT NULL,
        fbclid TEXT,
        yclid TEXT,
        gclid TEXT,
        gclientid TEXT,
        utm_from TEXT,
        utm_source TEXT,
        utm_medium TEXT,
        utm_campaign TEXT,
        utm_term TEXT,
        utm_content TEXT,
        utm_referrer TEXT,
        ym_uid TEXT,
        ym_counter TEXT,
        roistat TEXT);"""
        cursor.execute(create_query)
        connection.commit()
        logging.info('CREATE TABLE UTM')
    except Exception as error:
        logging.error(f'create_table_utm: {error}')


@db_create_decorator
def create_table_pipeline(connection, cursor) -> None:
    """Cоздает таблицу воронок"""
    try:
        logging.info('create create_pipeline_table')
        create_query = """CREATE TABLE pipeline_table(
        Дата DATE PRIMARY KEY NOT NULL,
        Неразобранное INT,
        Получена_новая_заявка INT,
        Заявка_взята_в_работу INT,
        Клиент_квалифицирован INT,
        Демонстрация_назначена INT,
        Демонстрация_проведена INT,
        КП_отправлено INT,
        Оплата_согласована INT,
        Договор_отправлен INT,
        Счет_выставлен INT,
        Внесена_предоплата INT,
        Успешно_реализовано INT,
        Закрыто_и_не_реализовано INT);"""
        cursor.execute(create_query)
        connection.commit()
        logging.info('CREATE TABLE PIPELINE')
    except Exception as error:
        logging.error(f'create_table_pipeline: {error}')


@db_create_decorator
def create_table_object_table(connection, cursor) -> None:
    """Cоздает таблицу объекта"""
    try:
        logging.info('create_table_object_table')
        create_query = """CREATE TABLE object_table(
        ID INT PRIMARY KEY NOT NULL ,
        Проект TEXT,
        Квартал TEXT,
        Название_объекта TEXT,
        Тип_помещения TEXT,
        Цена_объекта INT,
        S_м2 TEXT,
        Цена_за_м2 INT,
        Дом TEXT);"""
        cursor.execute(create_query)
        connection.commit()
        logging.info('CREATE TABLE OBJECT TABLE')
    except Exception as error:
        logging.error(f'create_table_object_table: {error}')

@db_create_decorator
def create_table_finance_table(connection, cursor) -> None:
    """Cоздает таблицу финансы"""
    try:
        logging.info('create_table_finance_table')
        create_query = """CREATE TABLE finance_table(
        ID INT PRIMARY KEY NOT NULL,
        Депозит_сделан TEXT,
        Способ_оплаты_депозита TEXT,
        Сумма_депозита INT,
        Дата_оплаты_депозита TIMESTAMP,
        Номер_договора TEXT,
        Дата_подписания_договора TIMESTAMP,
        Дата_завершения_строительства TIMESTAMP,
        Источник_платежа TEXT,
        Система_оплаты TEXT,
        Сумма_первого_платежа INT,
        Дата_первого_платежа TIMESTAMP,
        Сумма_второго_платежа INT,
        Дата_второго_платежа TIMESTAMP,
        Сумма_третьего_платежа INT,
        Дата_третьего_платежа TIMESTAMP,
        Сумма_четвертого_платежа INT,
        Дата_четвертого_платежа TIMESTAMP);"""
        cursor.execute(create_query)
        connection.commit()
        logging.info('CREATE TABLE FINANCE TABLE')
    except Exception as error:
        logging.error(f'create_table_finance_table: {error}')

@db_create_decorator
def create_table_tk(connection, cursor) -> None:
    """Cоздает таблицу тк"""
    try:
        logging.info('create tk_table')
        create_query = """CREATE TABLE tk_table(
        ID INT PRIMARY KEY NOT NULL,
        access_token TEXT, 
        refresh_token TEXT);"""
        cursor.execute(create_query)
        connection.commit()
        logging.info('CREATE TABLE TK')
    except Exception as error:
        logging.error(f'create_table_tk: {error}')


@db_decorator
def insert_leads(connection, cursor, records_to_insert: list) -> None:
    """Записывает сделки в базу"""
    try:
        logging.info(f'insert_leads {len(records_to_insert)}')
        insert_query = """INSERT INTO leads_table (
                ID, Название, Бюджет, Состояние, Ответственный, Группа, Воронка, Этап_воронки,  Дата_перехода_на_этап, 
                Дата_создания, Дата_изменения, Дата_закрытия,
                Ближайшая_задача, Наличие_задачи, Просрочена_задача) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cursor.executemany(insert_query, records_to_insert)
        connection.commit()
    except Exception as error:
        logging.error(f'insert_leads: {error}')


@db_decorator
def insert_custom_fields(connection, cursor, records_to_insert: list) -> None:
    """Записывает дополнительные поля в базу"""
    try:
        logging.info(f'insert_custom_fields {len(records_to_insert)}')
        insert_query = """INSERT INTO custom_fields_table (
                ID, Был_в_Новая_заявка, Был_в_Менеджер_назначен, Был_в_Взята_в_работу, Был_в_Попытка_связаться, 
                Был_в_Презентация_отправлена, Был_в_Контакт_состоялся, Был_в_Встреча_назначена, 
                Был_в_Встреча_отменена, Был_в_Встреча_проведена, Был_в_Ожидаем_предоплату, 
                Был_в_Получена_предоплата, Был_в_Реквизиты_получены, Был_в_Подготовка_договора,
                Был_в_Договор_подготовлен, Был_в_Договор_у_юриста, Был_в_Юристом_согласован,
                Был_в_Договор_клиенту, Был_в_Договор_согласован,  Был_в_Договор_подписан, Был_в_Первый_платеж, 
                Был_в_Второй_платеж, Был_в_Третий_платеж, Был_в_Выиграно, Был_в_Проиграно, 
                Дата_Новая_заявка, Дата_Менеджер_назначен, Дата_Взята_в_работу, Дата_Попытка_связаться, 
                Дата_Презентация_отправлена, Дата_Контакт_состоялся, Дата_Встреча_назначена, Дата_Встреча_отменена, 
                Дата_Встреча_проведена, Дата_Ожидаем_предоплату, Дата_Получена_предоплата, Дата_Реквизиты_получены, 
                Дата_Подготовка_договора, Дата_Договор_подготовлен, Дата_Договор_у_юриста, Дата_Юристом_согласован,
                Дата_Договор_клиенту, Дата_Договор_cогласован,
                Дата_Договор_подписан, Дата_Первый_платеж, Дата_Второй_платеж, Дата_Третий_платеж, Дата_Выиграно, 
                Дата_Проиграно, Источник_заявки, Взята, Скорость_взятия, Причина_отказа, Отказ_подробно, 
                Партнер_Агент, Проект, Язык, formname, Классификация_сделки) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cursor.executemany(insert_query, records_to_insert)
        connection.commit()
    except Exception as error:
        logging.error(f'insert_custom_fields: {error}')


@db_decorator
def insert_utm_table(connection, cursor, records_to_insert: list) -> None:
    """Записывает utm поля в базу"""
    try:
        logging.info(f'insert_utm_table {len(records_to_insert)}')
        insert_query = """INSERT INTO utm_table (
                ID, fbclid, yclid, gclid, gclientid, utm_from, utm_source, utm_medium,
                utm_campaign, utm_term, utm_content, utm_referrer, ym_uid, ym_counter, roistat) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cursor.executemany(insert_query, records_to_insert)
        connection.commit()
    except Exception as error:
        logging.error(f'insert_utm_table: {error}')


@db_decorator
def insert_tk_table(connection, cursor, records_to_insert: dict) -> None:
    """Обновляет и записывает токены в базу"""
    try:
        logging.info('insert_tk_table')
        insert_query = f"""INSERT INTO tk_table (
        ID, access_token, refresh_token)
        VALUES (1, '{records_to_insert["access_token"]}', '{records_to_insert["refresh_token"]}')
        ON CONFLICT (id) DO UPDATE SET
        access_token = EXCLUDED.access_token,
        refresh_token = EXCLUDED.refresh_token"""
        cursor.execute(insert_query)
        connection.commit()
    except Exception as error:
        logging.error(f'insert_tk_table: {error}, {records_to_insert}')


@db_decorator
def insert_pipeline_table(connection, cursor, records_to_insert: list) -> None:
    """Записывает сделки в базу"""
    try:
        logging.info(f'insert_pipeline_table {len(records_to_insert)}')
        insert_query = """INSERT INTO pipeline_table (
                Дата, Неразобранное, Получена_новая_заявка, Заявка_взята_в_работу, Клиент_квалифицирован, 
                Демонстрация_назначена, Демонстрация_проведена, КП_отправлено, Оплата_согласована,
                Договор_отправлен, Счет_выставлен, Внесена_предоплата, Успешно_реализовано, Закрыто_и_не_реализовано) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (Дата) DO UPDATE SET
                Дата = EXCLUDED.Дата,
                Неразобранное = EXCLUDED.Неразобранное,
                Получена_новая_заявка = EXCLUDED.Получена_новая_заявка,
                Заявка_взята_в_работу = EXCLUDED.Заявка_взята_в_работу,
                Клиент_квалифицирован = EXCLUDED.Клиент_квалифицирован, 
                Демонстрация_назначена = EXCLUDED.Демонстрация_назначена,
                Демонстрация_проведена = EXCLUDED.Демонстрация_проведена,
                КП_отправлено = EXCLUDED.КП_отправлено,
                Оплата_согласована = EXCLUDED.Оплата_согласована,
                Договор_отправлен = EXCLUDED.Договор_отправлен,
                Счет_выставлен = EXCLUDED.Счет_выставлен,
                Внесена_предоплата = EXCLUDED.Внесена_предоплата,
                Успешно_реализовано = EXCLUDED.Успешно_реализовано,
                Закрыто_и_не_реализовано = EXCLUDED.Закрыто_и_не_реализовано"""
        cursor.execute(insert_query, records_to_insert)
        connection.commit()
    except Exception as error:
        logging.error(f'insert_pipeline_table: {error}')


@db_decorator
def update_leads(connection, cursor, records_to_insert: list) -> None:
    """Обновляет сделки в базе"""
    try:
        logging.info(f'update_leads {len(records_to_insert)}')
        update_query = """INSERT INTO leads_table (
                ID, Название, Бюджет, Состояние, Ответственный, Группа, Воронка, Этап_воронки,
                Дата_создания, Дата_изменения, Дата_закрытия,
                Ближайшая_задача, Наличие_задачи, Просрочена_задача) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                Название = EXCLUDED.Название, Бюджет = EXCLUDED.Бюджет, Состояние = EXCLUDED.Состояние, 
                Ответственный = EXCLUDED.Ответственный, Группа = EXCLUDED.Группа, Воронка = EXCLUDED.Воронка, 
                Этап_воронки = EXCLUDED.Этап_воронки,
                Дата_создания = EXCLUDED.Дата_создания, Дата_изменения = EXCLUDED.Дата_изменения, 
                Дата_закрытия = EXCLUDED.Дата_закрытия, Ближайшая_задача = EXCLUDED.Ближайшая_задача, 
                Наличие_задачи = EXCLUDED.Наличие_задачи, Просрочена_задача = EXCLUDED.Просрочена_задача"""
        cursor.executemany(update_query, records_to_insert)
        connection.commit()
    except Exception as error:
        logging.error(f'update_leads: {error}')


@db_decorator
def full_update_leads(connection, cursor, records_to_insert: list) -> None:
    """Обновляет сделки в базе"""
    try:
        logging.info(f'full_update_leads {len(records_to_insert)}')
        update_query = """INSERT INTO leads_table (
                ID, Название, Бюджет, Состояние, Ответственный, Группа, Воронка, Этап_воронки, Дата_перехода_на_этап,
                Дата_создания, Дата_изменения, Дата_закрытия,
                Ближайшая_задача, Наличие_задачи, Просрочена_задача) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                Название = EXCLUDED.Название, Бюджет = EXCLUDED.Бюджет, Состояние = EXCLUDED.Состояние, 
                Ответственный = EXCLUDED.Ответственный, Группа = EXCLUDED.Группа, Воронка = EXCLUDED.Воронка, 
                Этап_воронки = EXCLUDED.Этап_воронки, Дата_перехода_на_этап = EXCLUDED.Дата_перехода_на_этап,
                Дата_создания = EXCLUDED.Дата_создания, Дата_изменения = EXCLUDED.Дата_изменения, 
                Дата_закрытия = EXCLUDED.Дата_закрытия, Ближайшая_задача = EXCLUDED.Ближайшая_задача, 
                Наличие_задачи = EXCLUDED.Наличие_задачи, Просрочена_задача = EXCLUDED.Просрочена_задача"""
        cursor.executemany(update_query, records_to_insert)
        connection.commit()
    except Exception as error:
        logging.error(f'full_update_leads: {error}')


@db_decorator
def update_leads_pipelines_status_date(connection, cursor, records_to_insert: list) -> None:
    """Записывает дату перехода в этап воронки"""
    try:
        logging.info(f'update_leads_pipelines_status_date {len(records_to_insert)}')
        insert_query = """UPDATE leads_table SET
                Дата_перехода_на_этап = %s WHERE ID = %s"""

        cursor.executemany(insert_query, records_to_insert)
        connection.commit()
    except Exception as error:
        logging.error(f'update_leads_pipelines_status_date: {error}')


@db_decorator
def update_custom_fields(connection, cursor, records_to_insert: list) -> None:
    """Обновляет дополнительные поля в базе"""
    try:
        logging.info(f'update_custom_fields {len(records_to_insert)}')
        update_query = """INSERT INTO custom_fields_table (
                ID, Был_в_Новая_заявка, Был_в_Менеджер_назначен, Был_в_Взята_в_работу, Был_в_Попытка_связаться, 
                Был_в_Презентация_отправлена, Был_в_Контакт_состоялся, Был_в_Встреча_назначена, 
                Был_в_Встреча_отменена, Был_в_Встреча_проведена, Был_в_Ожидаем_предоплату, 
                Был_в_Получена_предоплата, Был_в_Реквизиты_получены, Был_в_Подготовка_договора,
                Был_в_Договор_подготовлен, Был_в_Договор_у_юриста, Был_в_Юристом_согласован,
                Был_в_Договор_клиенту, Был_в_Договор_согласован,  Был_в_Договор_подписан, Был_в_Первый_платеж, 
                Был_в_Второй_платеж, Был_в_Третий_платеж, Был_в_Выиграно, Был_в_Проиграно, 
                Дата_Новая_заявка, Дата_Менеджер_назначен, Дата_Взята_в_работу, Дата_Попытка_связаться, 
                Дата_Презентация_отправлена, Дата_Контакт_состоялся, Дата_Встреча_назначена, Дата_Встреча_отменена, 
                Дата_Встреча_проведена, Дата_Ожидаем_предоплату, Дата_Получена_предоплата, Дата_Реквизиты_получены, 
                Дата_Подготовка_договора, Дата_Договор_подготовлен, Дата_Договор_у_юриста, Дата_Юристом_согласован,
                Дата_Договор_клиенту, Дата_Договор_cогласован,
                Дата_Договор_подписан, Дата_Первый_платеж, Дата_Второй_платеж, Дата_Третий_платеж, Дата_Выиграно, 
                Дата_Проиграно, Источник_заявки, Взята, Скорость_взятия, Причина_отказа, Отказ_подробно, 
                Партнер_Агент, Проект, Язык, formname, Классификация_сделки) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                Был_в_Новая_заявка = EXCLUDED.Был_в_Новая_заявка,
                Был_в_Менеджер_назначен = EXCLUDED.Был_в_Менеджер_назначен,
                Был_в_Взята_в_работу = EXCLUDED.Был_в_Взята_в_работу,
                Был_в_Попытка_связаться = EXCLUDED.Был_в_Попытка_связаться,
                Был_в_Презентация_отправлена = EXCLUDED.Был_в_Презентация_отправлена,
                Был_в_Контакт_состоялся = EXCLUDED.Был_в_Контакт_состоялся,
                Был_в_Встреча_назначена = EXCLUDED.Был_в_Встреча_назначена,
                Был_в_Встреча_отменена = EXCLUDED.Был_в_Встреча_отменена,
                Был_в_Встреча_проведена = EXCLUDED.Был_в_Встреча_проведена,
                Был_в_Ожидаем_предоплату = EXCLUDED.Был_в_Ожидаем_предоплату,
                Был_в_Получена_предоплата = EXCLUDED.Был_в_Получена_предоплата,
                Был_в_Реквизиты_получены = EXCLUDED.Был_в_Реквизиты_получены,
                Был_в_Договор_подготовлен = EXCLUDED.Был_в_Договор_подготовлен, 
                Был_в_Договор_у_юриста = EXCLUDED.Был_в_Договор_у_юриста, 
                Был_в_Юристом_согласован = EXCLUDED.Был_в_Юристом_согласован,
                Был_в_Договор_клиенту = EXCLUDED.Был_в_Договор_клиенту, 
                Был_в_Договор_согласован = EXCLUDED.Был_в_Договор_согласован,
                Был_в_Договор_согласован = EXCLUDED.Был_в_Договор_согласован,
                Был_в_Договор_подписан = EXCLUDED.Был_в_Договор_подписан,
                Был_в_Первый_платеж = EXCLUDED.Был_в_Первый_платеж,
                Был_в_Второй_платеж = EXCLUDED.Был_в_Второй_платеж,
                Был_в_Третий_платеж = EXCLUDED.Был_в_Третий_платеж,
                Был_в_Выиграно = EXCLUDED.Был_в_Выиграно,
                Был_в_Проиграно = EXCLUDED.Был_в_Проиграно,
                Дата_Новая_заявка = EXCLUDED.Дата_Новая_заявка,
                Дата_Менеджер_назначен = EXCLUDED.Дата_Менеджер_назначен,
                Дата_Взята_в_работу = EXCLUDED.Дата_Взята_в_работу,
                Дата_Попытка_связаться = EXCLUDED.Дата_Попытка_связаться,
                Дата_Презентация_отправлена = EXCLUDED.Дата_Презентация_отправлена,
                Дата_Контакт_состоялся = EXCLUDED.Дата_Контакт_состоялся,
                Дата_Встреча_назначена = EXCLUDED.Дата_Встреча_назначена,
                Дата_Встреча_отменена = EXCLUDED.Дата_Встреча_отменена,
                Дата_Встреча_проведена = EXCLUDED.Дата_Встреча_проведена,
                Дата_Ожидаем_предоплату = EXCLUDED.Дата_Ожидаем_предоплату,
                Дата_Получена_предоплата = EXCLUDED.Дата_Получена_предоплата,
                Дата_Реквизиты_получены = EXCLUDED.Дата_Реквизиты_получены,
                Дата_Подготовка_договора = EXCLUDED.Дата_Подготовка_договора, 
                Дата_Договор_подготовлен = EXCLUDED.Дата_Договор_подготовлен , 
                Дата_Договор_у_юриста = EXCLUDED.Дата_Договор_у_юриста, 
                Дата_Юристом_согласован, = EXCLUDED.Дата_Юристом_согласован,
                Дата_Договор_клиенту = EXCLUDED.Дата_Договор_клиенту,
                Дата_Договор_согласован = EXCLUDED.Дата_Договор_согласован,
                Дата_Договор_подписан = EXCLUDED.Дата_Договор_подписан,
                Дата_Первый_платеж = EXCLUDED.Дата_Первый_платеж,
                Дата_Второй_платеж = EXCLUDED.Дата_Второй_платеж,
                Дата_Третий_платеж = EXCLUDED.Дата_Третий_платеж,
                Дата_Выиграно = EXCLUDED.Дата_Выиграно,
                Дата_Проиграно = EXCLUDED.Дата_Проиграно,
                Источник_заявки = EXCLUDED.Источник_заявки,
                Взята = EXCLUDED.Взята,
                Скорость_взятия = EXCLUDED.Скорость_взятия,
                Причина_отказа = EXCLUDED.Причина_отказа,
                Отказ_подробно = EXCLUDED.Отказ_подробно,
                Партнер_Агент = EXCLUDED.Партнер_Агент,
                Проект = EXCLUDED.Проект,
                Язык = EXCLUDED.Язык,
                formname = EXCLUDED.formname, 
                Классификация_сделки = EXCLUDED.Классификация_сделки
                """
        cursor.executemany(update_query, records_to_insert)
        connection.commit()
    except Exception as error:
        logging.error(f'update_custom_fields: {error}')


@db_decorator
def update_utm_table(connection, cursor, records_to_insert: list) -> None:
    """Обновляет utm в базе"""
    try:
        logging.info(f'update_utm_table {len(records_to_insert)}')
        update_query = """INSERT INTO utm_table (
                ID, fbclid, yclid, gclid, gclientid, utm_from, utm_source, utm_medium,
                utm_campaign, utm_term, utm_content, utm_referrer, ym_uid, ym_counter, roistat) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                fbclid = EXCLUDED.fbclid, yclid = EXCLUDED.yclid, gclid = EXCLUDED.gclid, 
                gclientid = EXCLUDED.gclientid, 
                utm_from = EXCLUDED.utm_from, utm_source = EXCLUDED.utm_source, utm_medium = EXCLUDED.utm_medium,
                utm_campaign = EXCLUDED.utm_campaign, utm_term = EXCLUDED.utm_term, utm_content = EXCLUDED.utm_content, 
                utm_referrer = EXCLUDED.utm_referrer, ym_uid = EXCLUDED.ym_uid, ym_counter = EXCLUDED.ym_counter, 
                roistat = EXCLUDED.roistat"""
        cursor.executemany(update_query, records_to_insert)
        connection.commit()
    except Exception as error:
        logging.error(f'update_utm_table: {error}')


@db_decorator
def update_lost_stage(connection, cursor, records_to_insert: list) -> None:
    """Записывает проигранный этап в сделку"""
    try:
        logging.info(f'update lost stage in lead_table {len(records_to_insert)}')
        insert_query = """UPDATE custom_fields_table SET Этап_отказа = %s WHERE ID = %s"""
        cursor.executemany(insert_query, records_to_insert)
        connection.commit()
    except Exception as error:
        logging.error(f'update_lost_stage: {error}')


@db_select_decorator
def read_tokens(cursor) -> tuple:
    """Читает токены из базы"""
    try:
        logging.info('read_tokens')
        select_query = """SELECT access_token, refresh_token FROM tk_table"""
        cursor.execute(select_query)
        return cursor.fetchone()
    except Exception as error:
        logging.error(f'read_tokens: {error}')


@db_delete_decorator
def delete_from_table(connection, cursor, name_of_table: str) -> None:
    """Удаляет все данные из таблицы"""
    try:
        logging.info(f'delete_from_table {name_of_table}')
        delete_query = f"""DELETE FROM {name_of_table}"""
        cursor.execute(delete_query)
        connection.commit()
        logging.info('DELETE')
    except Exception as error:
        logging.error(f'delete_from_table: {error}')


@db_delete_decorator
def delete_table(connection, cursor, name_of_table: str) -> None:
    """Удаляет таблицу"""
    try:
        logging.info('delete_table')
        delete_query = f"""DROP TABLE {name_of_table}"""
        cursor.execute(delete_query)
        connection.commit()
        logging.info('DELETE')
    except Exception as error:
        logging.error(f'delete_table: {error}')


@db_delete_leads_decorator
def delete_leads(connection, cursor, delete_list) -> None:
    try:
        logging.info(f'delete_leads {len(delete_list)}')
        delete_query = """DELETE FROM leads_table WHERE ID = %s"""
        cursor.executemany(delete_query, delete_list)
        connection.commit()
        logging.info('DELETED LEADS')
    except Exception as error:
        logging.error(f'delete_leads: {error}')
