import psycopg2
from pw import DataBase


def db_decorator(func):
    def inner(records_to_insert):
        if len(records_to_insert) > 0:
            connection = psycopg2.connect(database=DataBase.DATABASE, user=DataBase.USER, password=DataBase.PASSWORD,
                                          host=DataBase.HOST, port=DataBase.PORT)
            cursor = connection.cursor()
            print('Connection success!')
            func(records_to_insert, connection, cursor)

            connection.close()
            print('Connection close!')

    return inner


def db_create_decorator(func):
    def inner():
        connection = psycopg2.connect(database=DataBase.DATABASE, user=DataBase.USER, password=DataBase.PASSWORD,
                                      host=DataBase.HOST, port=DataBase.PORT)
        cursor = connection.cursor()
        print('Connection success!')
        func(connection, cursor)

        connection.close()
        print('Connection close!')

    return inner


def db_select_decorator(func):
    def inner(*args):
        connection = psycopg2.connect(database=DataBase.DATABASE, user=DataBase.USER, password=DataBase.PASSWORD,
                                      host=DataBase.HOST, port=DataBase.PORT)
        cursor = connection.cursor()
        print('Connection success!')
        tokens = func(cursor, *args)
        connection.close()
        print('Connection close!')
        return tokens

    return inner


def db_delete_decorator(func):
    def inner(name_of_table):
        connection = psycopg2.connect(database=DataBase.DATABASE, user=DataBase.USER, password=DataBase.PASSWORD,
                                      host=DataBase.HOST, port=DataBase.PORT)
        cursor = connection.cursor()
        print('Connection success!')
        func(name_of_table, connection, cursor)

        connection.close()
        print('Connection close!')

    return inner


@db_create_decorator
def create_table_leads_table(connection, cursor) -> None:
    """Cоздает таблицу сделок"""
    create_query = """CREATE TABLE leads_table(
    ID INT PRIMARY KEY NOT NULL ,
    Название TEXT,
    Бюджет INT,
    Состояние TEXT,
    Ответственный TEXT,
    Группа TEXT,
    Воронка TEXT,
    Этап_воронки TEXT,
    Дата_перехода_на_этап DATE,
    Дата_создания DATE,
    Дата_изменения DATE,
    Дата_закрытия DATE,
    Ближайшая_задача DATE,
    Наличие_задачи TEXT,
    Просрочена_задача TEXT);"""
    cursor.execute(create_query)
    connection.commit()
    print('CREATE TABLE LEADS TABLE')


@db_create_decorator
def create_table_custom_fields_table(connection, cursor) -> None:
    """Cоздает таблицу кастомных полей"""
    create_query = """CREATE TABLE custom_fields_table(
    ID INT PRIMARY KEY NOT NULL ,
    Источник_заявки TEXT,
    Форма_заявки TEXT,
    Канал_рекламы TEXT,
    Приоритет_клиента TEXT,
    Условия_оплаты TEXT,
    Первая_сумма_оплаты DECIMAL,
    Первая_дата_платежа DATE,
    Вторая_сумма_оплаты DECIMAL,
    Вторая_дата_платежа DATE,
    Остаток_платежа DECIMAL,
    Дата_подписания DATE,
    Причина_отказа TEXT);"""
    cursor.execute(create_query)
    connection.commit()
    print('CREATE TABLE CUSTOM FIELDS')


@db_create_decorator
def create_table_utm(connection, cursor) -> None:
    """Cоздает таблицу utm меток"""
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
    print('CREATE TABLE UTM')


@db_create_decorator
def create_pipeline_table(connection, cursor) -> None:
    """Cоздает таблицу utm меток"""
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
    print('CREATE TABLE PIPELINE')


@db_create_decorator
def create_table_tk(connection, cursor) -> None:
    """Cоздает таблицу тк"""
    create_query = """CREATE TABLE tk_table(
    ID INT PRIMARY KEY NOT NULL,
    access_token TEXT, 
    refresh_token TEXT);"""
    cursor.execute(create_query)
    connection.commit()
    print('CREATE TABLE TK')


@db_decorator
def insert_leads(records_to_insert: list, connection, cursor) -> None:
    """Записывает сделки в базу"""
    print(len(records_to_insert))
    insert_query = """INSERT INTO leads_table (
            ID, Название, Бюджет, Состояние, Ответственный, Группа, Воронка, Этап_воронки,  Дата_перехода_на_этап, 
            Дата_создания, Дата_изменения, Дата_закрытия,
            Ближайшая_задача, Наличие_задачи, Просрочена_задача) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(insert_query, records_to_insert)
    connection.commit()


@db_decorator
def insert_custom_fields(records_to_insert: list, connection, cursor) -> None:
    """Записывает дополнительные поля в базу"""
    print(len(records_to_insert))
    insert_query = """INSERT INTO custom_fields_table (
            ID, Источник_заявки,  Форма_заявки, Канал_рекламы, Приоритет_клиента, Условия_оплаты ,
            Первая_сумма_оплаты, Первая_дата_платежа, Вторая_сумма_оплаты,  Вторая_дата_платежа, Остаток_платежа,
            Дата_подписания, Причина_отказа) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(insert_query, records_to_insert)
    connection.commit()


@db_decorator
def insert_utm_table(records_to_insert: list, connection, cursor) -> None:
    """Записывает utm поля в базу"""
    print(len(records_to_insert))
    insert_query = """INSERT INTO utm_table (
            ID, fbclid, yclid, gclid, gclientid, utm_from, utm_source, utm_medium,
            utm_campaign, utm_term, utm_content, utm_referrer, ym_uid, ym_counter, roistat) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(insert_query, records_to_insert)
    connection.commit()


@db_decorator
def insert_tk_table(records_to_insert, connection, cursor) -> None:
    """Обновляет и записывает токены в базу"""
    print(len(records_to_insert))
    insert_query = f"""INSERT INTO tk_table (
    ID, access_token, refresh_token)
    VALUES (1, '{records_to_insert["access_token"]}', '{records_to_insert["refresh_token"]}')
    ON CONFLICT (id) DO UPDATE SET
    access_token = EXCLUDED.access_token,
    refresh_token = EXCLUDED.refresh_token"""
    cursor.execute(insert_query)
    connection.commit()


@db_decorator
def insert_pipeline_table(record_to_insert: list, connection, cursor) -> None:
    """Записывает сделки в базу"""
    print(len(record_to_insert))
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
    cursor.execute(insert_query, record_to_insert)
    connection.commit()


@db_decorator
def update_leads(records_to_insert: list, connection, cursor) -> None:
    """Обновляет сделки в базе"""
    print(len(records_to_insert))
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


@db_decorator
def update_leads_pipelines_status_date(records_to_insert: list, connection, cursor) -> None:
    """Записывает дату перехода в этап воронки"""
    print(len(records_to_insert))
    insert_query = """UPDATE leads_table SET
            Дата_перехода_на_этап = %s WHERE ID = %s"""

    cursor.executemany(insert_query, records_to_insert)
    connection.commit()


@db_decorator
def update_custom_fields(records_to_insert: list, connection, cursor) -> None:
    """Обновляет дополнительные поля в базе"""
    print(len(records_to_insert))
    update_query = """INSERT INTO custom_fields_table (
            ID, Источник_заявки,  Форма_заявки, Канал_рекламы, Приоритет_клиента, Условия_оплаты,
            Первая_сумма_оплаты, Первая_дата_платежа, Вторая_сумма_оплаты, Вторая_дата_платежа, Остаток_платежа,
            Дата_подписания, Причина_отказа) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
            Источник_заявки = EXCLUDED.Источник_заявки, Форма_заявки = EXCLUDED.Форма_заявки, 
            Канал_рекламы = EXCLUDED.Канал_рекламы, Приоритет_клиента = EXCLUDED.Приоритет_клиента, 
            Условия_оплаты = EXCLUDED.Условия_оплаты, Первая_сумма_оплаты = EXCLUDED.Первая_сумма_оплаты,
            Первая_дата_платежа = EXCLUDED.Первая_дата_платежа, Вторая_сумма_оплаты = EXCLUDED.Вторая_сумма_оплаты, 
            Вторая_дата_платежа = EXCLUDED.Вторая_дата_платежа, Остаток_платежа = EXCLUDED.Остаток_платежа,
            Дата_подписания = EXCLUDED.Дата_подписания, Причина_отказа = EXCLUDED.Причина_отказа"""
    cursor.executemany(update_query, records_to_insert)
    connection.commit()


@db_decorator
def update_utm_table(records_to_insert: list, connection, cursor) -> None:
    """Обновляет utm в базе"""
    print(len(records_to_insert))
    update_query = """INSERT INTO utm_table (
            ID, fbclid, yclid, gclid, gclientid, utm_from, utm_source, utm_medium,
            utm_campaign, utm_term, utm_content, utm_referrer, ym_uid, ym_counter, roistat) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
            fbclid = EXCLUDED.fbclid, yclid = EXCLUDED.yclid, gclid = EXCLUDED.gclid, gclientid = EXCLUDED.gclientid, 
            utm_from = EXCLUDED.utm_from, utm_source = EXCLUDED.utm_source, utm_medium = EXCLUDED.utm_medium,
            utm_campaign = EXCLUDED.utm_campaign, utm_term = EXCLUDED.utm_term, utm_content = EXCLUDED.utm_content, 
            utm_referrer = EXCLUDED.utm_referrer, ym_uid = EXCLUDED.ym_uid, ym_counter = EXCLUDED.ym_counter, 
            roistat = EXCLUDED.roistat"""
    cursor.executemany(update_query, records_to_insert)
    connection.commit()


@db_select_decorator
def read_tokens(cursor) -> tuple:
    """Читает токены из базы"""
    select_query = """SELECT access_token, refresh_token FROM tk_table"""
    cursor.execute(select_query)
    return cursor.fetchone()


@db_select_decorator
def select_pipeline_status_count(cursor, pipeline_status=None) -> tuple:
    """Возвращает кол-во сделок на этапе"""
    print('Забираю count')
    select_query = f"""SELECT COUNT(pipeline_status) FROM leads_table WHERE pipeline= 'Продажи CRM | ClickCRM' 
                    and pipeline_status= '{pipeline_status}'"""
    cursor.execute(select_query)
    return cursor.fetchone()


@db_delete_decorator
def delete_from_table(name_of_table: str, connection, cursor) -> None:
    """Удаляет все данные из таблицы"""
    delete_query = f"""DELETE FROM {name_of_table}"""
    cursor.execute(delete_query)
    connection.commit()
    print('DELETE')


@db_delete_decorator
def delete_table(name_of_table: str, connection, cursor) -> None:
    """Удаляет таблицу"""
    delete_query = f"""DROP TABLE {name_of_table}"""
    cursor.execute(delete_query)
    connection.commit()
    print('DELETE')
