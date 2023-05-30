import json
import datetime
import logging

from paths import my_log, my_f

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename=my_log, encoding='utf-8', level=logging.DEBUG)


def write_tokens(tokens: dict) -> None:
    """Записывает токены в файл"""
    try:
        logging.info('Записываю токены в файл')
        with open(my_f + 'tokens', 'w', encoding='utf-8') as file:
            json.dump(tokens, file, indent=4)
    except Exception as error:
        logging.error(f'write_tokens: {error}')


def write_data(data, name_of_data: str, page_num: int = None, prefix: str = None, second_dict_name: str = None) -> None:
    """Записывает данные в файл"""
    try:
        logging.info('Записываю сделки в файл')
        if second_dict_name is not None:
            name_of_data = second_dict_name
        if prefix is not None:
            file_path = f'{prefix}'.capitalize() + '/' f'{name_of_data}' + '_dict' + f'{page_num}'
        else:
            file_path = f'{name_of_data}'.capitalize() + '/' f'{name_of_data}' + f'{page_num}'

        with open(my_f + file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, sort_keys=False, ensure_ascii=False, indent=4)
            logging.info(f'Json получен страница: {page_num} данные: {name_of_data}')
    except Exception as error:
        logging.error(f'write_data: {error}')


def read_token() -> json:
    """Читает токен из файла"""
    try:
        logging.info('Читаю токен из файла')
        with open(my_f + 'tokens', 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as error:
        logging.error(f'read_token: {error}')


def read_data_file(name_of_data: str, page_num: int = 1, extra_prefix: str = None) -> json:
    """Читает файл"""
    if extra_prefix is not None:
        file_path = f'{extra_prefix}'.capitalize() + '/' f'{name_of_data}' + '_dict' f'{page_num}'
    else:
        file_path = f'{name_of_data}'.capitalize() + '/' f'{name_of_data}' + f'{page_num}'
    with open(my_f + file_path, 'r', encoding='utf-8') as file:
        return json.load(file)


def check_next_api_page(file_data: json) -> bool:
    """Возвращает True/False наличие след. ссылки"""
    try:
        if file_data['_links']['next']:
            return True
    except KeyError:
        logging.info('Получил все файлы!')
        return False
    except Exception as error:
        logging.error(f'check_next_api_page: {error}')


def convert_unix_to_date(unix_time: int) -> datetime or None:
    """Конвертирует время из unix в обычный формат возвращает None либо дату"""
    try:
        time = datetime.datetime.fromtimestamp(unix_time).date()
    except TypeError:
        return None
    except Exception as error:
        logging.info(f'convert_unix_to_date: {error}')
    else:
        return time


def convert_unix_to_date_time(unix_time: int) -> datetime or None:
    """Конвертирует время из unix в обычный формат возвращает None или дату и время"""
    try:
        time = datetime.datetime.fromtimestamp(unix_time)
    except TypeError:
        return None
    except Exception as error:
        logging.error(f'convert_unix_to_date_time: {error}')
    else:
        logging.info(f'Возвращаю convert_unix_to_date_time: {time}')
        return time


def convert_group_id(group_id: int, group_dict) -> str:
    """Конвертирует group id"""
    try:
        if group_dict[str(group_id)]:
            return group_dict[str(group_id)]
        else:
            return 'Нет группы'
    except Exception as error:
        logging.error(f'convert_group_id: {error}')


def convert_status_id(status_id: str, statuses_dict: dict, pipeline_id: int) -> str:
    """Конвертирует status id в этап воронки'"""
    try:
        return list((status[1] for status in statuses_dict[str(pipeline_id)] if status[0] == status_id))[0]
    except Exception as error:
        logging.error(f'convert_status_id: {error}')


def convert_responsible_id(responsible_id: int, users_dict: dict) -> str or None:
    """Конвертирует responsible_id в пользователя"""
    responsible_id_str = str(responsible_id)
    try:
        if users_dict[responsible_id_str] is not None:
            return users_dict[responsible_id_str]
        else:
            return None
    except Exception as error:
        logging.error(f'convert_responsible_id: {error}')


def convert_pipeline_id(user_pipeline_id: int, pipelines_dict: dict) -> str or None:
    """Конвертирует user_pipeline_id в статус в воронке"""
    user_pipeline_id_str = str(user_pipeline_id)
    try:
        if pipelines_dict[user_pipeline_id_str] is not None:
            return pipelines_dict[user_pipeline_id_str]
        else:
            return None
    except Exception as error:
        logging.error(f'convert_pipeline_id: {error}')


def status_of_lead(status_id: int) -> str:
    """Возвращает статус сделки"""
    try:
        if status_id == 143:
            return 'Проиграна'
        elif status_id == 142:
            return 'Выиграна'
        else:
            return 'Активная'
    except Exception as error:
        logging.error(f'status_of_lead: {error}')


def convert_have_task(time) -> str:
    """Возвращает наличие задачи"""
    try:
        if time is None:
            return 'No'
        else:
            return 'Yes'
    except Exception as error:
        logging.error(f'convert_have_task: {error}')


def convert_task_time(closest_task_at) -> str or None:
    """Возвращает просрочена задача или нет"""
    try:
        if closest_task_at is not None:
            if closest_task_at.date() < datetime.date.today():
                return 'Yes'
            else:
                return 'No'
        else:
            return None
    except Exception as error:
        logging.error(f'convert_task_time: {error}')


def get_lead_record(data: json) -> list:
    """Подготовливает строки для записи в базу"""
    pipelines_dict = read_data_file(name_of_data='pipelines', extra_prefix='Dict')
    statuses_dict = read_data_file(name_of_data='statuses', extra_prefix='Dict')
    users_dict = read_data_file(name_of_data='users', extra_prefix='Dict')
    group_dict = read_data_file(name_of_data='group', extra_prefix='Dict')
    archive_pipelines = read_data_file(name_of_data='archive_pipelines', extra_prefix='Dict')
    block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
    records_to_insert = []
    leads = data['_embedded']['leads']
    logging.info('Подготовливаю строки для записи в базу')
    try:
        for lead in leads:
            if str(lead['pipeline_id']) not in archive_pipelines:
                if str(lead['pipeline_id']) not in block_pipelines:
                    lead_id = lead['id']
                    name = lead['name']
                    price = lead['price']
                    responsible = convert_responsible_id(lead['responsible_user_id'], users_dict=users_dict)
                    group_id = convert_group_id(lead['group_id'], group_dict=group_dict)
                    pipeline = convert_pipeline_id(lead['pipeline_id'], pipelines_dict=pipelines_dict)
                    pipeline_status = convert_status_id(lead['status_id'], statuses_dict=statuses_dict,
                                                        pipeline_id=lead['pipeline_id'])
                    pipeline_date = None
                    created_at = convert_unix_to_date_time(lead['created_at'])
                    updated_at = convert_unix_to_date_time(lead['updated_at'])
                    closed_at = convert_unix_to_date_time(lead['closed_at'])
                    closest_task_at = convert_unix_to_date_time(lead['closest_task_at'])
                    lead_status = status_of_lead(lead['status_id'])
                    have_task = convert_have_task(closest_task_at)
                    overdue_task = convert_task_time(closest_task_at)

                    record_to_insert = (lead_id, name, price, lead_status, responsible, group_id,
                                        pipeline, pipeline_status, pipeline_date, created_at, updated_at,
                                        closed_at, closest_task_at, have_task, overdue_task)

                    records_to_insert.append(record_to_insert)

        return records_to_insert
    except Exception as error:
        logging.error(f'get_lead_record: {error}')


def get_lead_update_record(data: json) -> list:
    """Подготовливает строки для записи в базу"""

    pipelines_dict = read_data_file(name_of_data='pipelines', extra_prefix='Dict')
    statuses_dict = read_data_file(name_of_data='statuses', extra_prefix='Dict')
    users_dict = read_data_file(name_of_data='users', extra_prefix='Dict')
    group_dict = read_data_file(name_of_data='group', extra_prefix='Dict')
    archive_pipelines = read_data_file(name_of_data='archive_pipelines', extra_prefix='Dict')
    block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
    records_to_insert = []
    leads = data['_embedded']['leads']
    logging.info('Подготовливаю строки для записи в базу')
    try:
        for lead in leads:
            if (convert_unix_to_date(lead['updated_at']) >= datetime.date.today() or
                    convert_unix_to_date(lead['updated_at']) == (datetime.date.today() - datetime.timedelta(days=1))):
                if str(lead['pipeline_id']) not in archive_pipelines:
                    if str(lead['pipeline_id']) not in block_pipelines:
                        lead_id = lead['id']
                        name = lead['name']
                        price = lead['price']
                        responsible = convert_responsible_id(lead['responsible_user_id'], users_dict=users_dict)
                        group_id = convert_group_id(lead['group_id'], group_dict=group_dict)
                        pipeline = convert_pipeline_id(lead['pipeline_id'], pipelines_dict=pipelines_dict)
                        pipeline_status = convert_status_id(lead['status_id'], statuses_dict=statuses_dict,
                                                            pipeline_id=lead['pipeline_id'])
                        created_at = convert_unix_to_date_time(lead['created_at'])
                        updated_at = convert_unix_to_date_time(lead['updated_at'])
                        closed_at = convert_unix_to_date_time(lead['closed_at'])
                        closest_task_at = convert_unix_to_date_time(lead['closest_task_at'])
                        lead_status = status_of_lead(lead['status_id'])
                        have_task = convert_have_task(closest_task_at)
                        overdue_task = convert_task_time(closest_task_at)

                        record_to_insert = (
                            lead_id, name, price, lead_status, responsible, group_id, pipeline, pipeline_status,
                            created_at, updated_at, closed_at, closest_task_at, have_task, overdue_task)

                        records_to_insert.append(record_to_insert)
        return records_to_insert

    except Exception as error:
        logging.error(f'get_lead_update_record: {error}')


def get_users(users: json) -> dict:
    """Возвращает словарь пользователей id+name"""
    try:
        users = users['_embedded']['users']
        logging.info('Создаю словарь пользователей')
        return {user['id']: user['name'] for user in users}
    except Exception as error:
        logging.error(f'get_users: {error}')


def get_pipelines(pipelines: json) -> dict:
    """Возвращает словарь воронок id+name"""
    try:
        block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
        pipelines = pipelines['_embedded']['pipelines']
        logging.info('Создаю словарь воронок')
        return {pipeline['id']: pipeline['name'] for pipeline in pipelines if not pipeline['is_archive']
                and str(pipeline['id']) not in block_pipelines}
    except Exception as error:
        logging.error(f'get_pipelines: {error}')


def get_archive_pipelines(pipelines: json) -> dict:
    """Возвращает словарь архивных воронок id+name"""
    try:
        pipelines = pipelines['_embedded']['pipelines']
        logging.info('Создаю словарь воронок')
        return {pipeline['id']: pipeline['name'] for pipeline in pipelines if pipeline['is_archive']}
    except Exception as error:
        logging.error(f'get_archive_pipelines: {error}')


def get_statuses(pipelines: json) -> dict:
    """Возвращает словарь воронок и этапов воронки id+name+statuses"""
    pipelines = pipelines['_embedded']['pipelines']
    try:
        return {pipeline['id']: [[status['id'], status['name']] for status in pipeline['_embedded']['statuses']] for
                pipeline in pipelines if not pipeline['is_archive']}
    except Exception as error:
        logging.error(f'get_statuses: {error}')


def get_leads_custom_fields_dict(leads) -> dict:
    """Возвращает словарь лидов с дополнительными полями"""
    try:
        archive_pipelines = read_data_file(name_of_data='archive_pipelines', page_num=1, extra_prefix='Dict')
        block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
        leads = leads['_embedded']['leads']
        logging.info('Создаю словарь пользователей и доп.полей')
        return {
            lead['id']: [[field['field_id'], field['values'][0]['value']] for field in lead['custom_fields_values']]
            for lead in leads if lead['custom_fields_values'] and (str(lead['pipeline_id']) not in archive_pipelines and
                                                                   str(lead['pipeline_id']) not in block_pipelines)}
    except Exception as error:
        logging.error(f'get_leads_custom_fields_dict: {error}')


def get_leads_custom_fields_dict_update(leads) -> dict:
    """Возвращает словарь лидов с дополнительными полями для обновления"""
    try:
        archive_pipelines = read_data_file(name_of_data='archive_pipelines', page_num=1, extra_prefix='Dict')
        block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
        leads = leads['_embedded']['leads']
        logging.info('Создаю словарь пользователей и доп.полей для обновления')
        return {
            lead['id']: [[field['field_id'], field['values'][0]['value']] for field in lead['custom_fields_values']]
            for lead in leads if lead['custom_fields_values'] and
                                 ((str(lead['pipeline_id']) not in archive_pipelines and
                                   str(lead['pipeline_id']) not in block_pipelines) and
                                  (convert_unix_to_date(lead['updated_at']) >= datetime.date.today() or
                                   convert_unix_to_date(lead['updated_at']) ==
                                   (datetime.date.today() - datetime.timedelta(days=1))))}
    except Exception as error:
        logging.error(f'get_leads_custom_fields_dict_update: {error}')


def get_custom_fields_dict(leads_custom_fields_dict: dict) -> dict:
    """Возвращает преобразовыннй словарь лидов с дополнительными полями"""
    # custom_fields = {'Был в Новая заявка': 1, 'Был в Менеджер назначен': 2, 'Был в Взята в работу': 3,
    #                  'Был в Попытка связаться': 4, 'Был в Презентация отправлена': 5, 'Был в Контакт состоялся': 6,
    #                  'Был в Встреча назначена': 7, 'Был в Встреча отменена': 8, 'Был в Встреча проведена': 9,
    #                  'Был в Ожидаем предоплату': 10, 'Был в Получена предоплата': 11, 'Был в Реквизиты получены': 12,
    #                  'Был в Договор отправлен юристу': 13, 'Был в Договор отправлен клиенту': 14,
    #                  'Был в Договор подписан': 15, 'Был в Договор согласован': 16, 'Был в Первый платеж': 17,
    #                  'Был в Второй платеж': 18, 'Был в Третий платеж': 19, 'Был в Выиграно': 20, 'Был в Проиграно': 21,
    #                  'Дата Новая заявка': 22,
    #                  'Дата Менеджер назначен': 23, 'Дата Взята в работу': 24, 'Дата Попытка связаться': 25,
    #                  'Дата Презентация отправлена': 26, 'Дата Контакт состоялся': 27, 'Дата Встреча назначена': 28,
    #                  'Дата Встреча отменена': 29, 'Дата Встреча проведена': 30, 'Дата Ожидаем предоплату': 31,
    #                  'Дата Получена предоплата': 32, 'Дата Реквизиты получены': 33,
    #                  'Дата Договор отправлен юристу': 34, 'Дата Договор отправлен клиенту': 35,
    #                  'Дата Договор согласован' : 36, 'Дата Договор подписан': 37, 'Дата Первый платеж': 38,
    #                  'Дата Второй платеж': 39, 'Дата Третий платеж': 40, 'Дата Выиграно': 41,
    #                  'Дата Проиграно': 42, 'Источник заявки / Lead source': 43, 'Взята': 44, 'Скорость взятия': 45,
    #                  'Причина отказа / Rejection reason': 46, 'Отказ подробно / Failure Detail': 47,
    #                  'Партнер / Partner': 48, 'Проект / Project': 49, 'Язык / Language': 50,
    #                  'FORMNAME': 51, 'Классификация сделки': 52}

    custom_fields = {'1150819': 1, '1150857': 2, '1150821': 3,
                     '1150823': 4, '1151845': 5, '1150825': 6,
                     '1150827': 7, '1150831': 9,
                     '1150833': 10, '1150835': 11, '1150837': 12,
                     '1155419': 13, '1150839': 14,
                     '1150841': 15, '1152329': 16, '1150843': 17,
                     '1150845': 18, '1150847': 19, '1150849': 20, '1150851': 21,
                     '1150853': 22,
                     '1150859': 23, '1150855': 24, '1150861': 25,
                     '1151847': 26, '1150863': 27, '1150865': 28,
                     '1150869': 30, '1150871': 31,
                     '1150873': 32, '1150875': 33,
                     '1155421': 34, '1150877': 35,
                     '1152327': 36, '1150879': 37, '1150881': 38,
                     '1150883': 39, '1150885': 40, '1150887': 41,
                     '1150889': 42, '1148679': 43, '1150995': 44, '1150993': 45,
                     '1150999': 46, '1151893': 47,
                     '1151863': 48, '1136749': 49, '1147847': 50,
                     '1126883': 51, '1160341': 52}

    leads_dict = leads_custom_fields_dict

    try:
        custom_fields_dict = {
            lead: [{custom_fields[f'{field[0]}']: field[1]} for field in leads_dict[lead] if field[0] in custom_fields] for
            lead
            in leads_dict.keys()}
        # очищение от пустых лидов без доп.полей
        custom_fields_dict = {lead: custom_fields for lead, custom_fields in custom_fields_dict.items() if
                              custom_fields}
        logging.info('Создаю словарь доп.полей')
        return custom_fields_dict
    except Exception as error:
        logging.error(f'get_custom_fields_dict: {error}')


def get_utm_dict(leads_custom_fields_dict: dict) -> dict:
    """Возвращает преобразовыннй словарь лидов с дополнительными полями"""
    custom_fields = {'fbclid': 1, 'yclid': 2, 'gclid': 3, 'gclientid': 4, 'from': 5,
                     'utm_source': 6, 'utm_medium': 7, 'utm_campaign': 8, 'utm_term': 9, 'utm_content': 10,
                     'utm_referrer': 11, '_ym_uid': 12, '_ym_counter': 13, 'roistat': 14}
    leads_dict = leads_custom_fields_dict

    try:
        utm_dict = {
            lead: [{custom_fields[field[0]]: field[1]} for field in leads_dict[lead] if field[0] in custom_fields] for
            lead
            in leads_dict.keys()}
        # очищение от пустых лидов без доп.полей
        utm_dict = {lead: custom_fields for lead, custom_fields in utm_dict.items() if custom_fields}
        logging.info('Создаю словарь доп.полей')
        return utm_dict
    except Exception as error:
        logging.error(f'get_utm_dict: {error}')


def convert_item(lead: str, custom_fields_dict: dict, need_item: str):
    """Конвертирует элемент и возвращает его"""
    try:
        for item in custom_fields_dict[lead]:
            if need_item in item:
                if need_item == '6' or need_item == '8' or need_item == '10':
                    try:
                        return int(item[str(need_item)])
                    except ValueError:
                        return float(item[str(need_item)])
                else:
                    return item[str(need_item)]
    except Exception as error:
        logging.error(f'convert_item: {error}')


def convert_item_2(lead: str, custom_fields_dict: dict, need_item: str) -> str:
    """Конвертирует элемент и возвращает его"""
    try:
        for item in custom_fields_dict[lead]:
            if need_item in item:
                return str(item[str(need_item)])
    except Exception as error:
        logging.error(f'convert_item_2: {error}')


def get_custom_fields_record(data: json) -> list:
    """Возвращает список записей для записи в базу"""
    records_to_insert = []
    logging.info('Возвращаю список записей для записи в базу')
    try:
        for lead in data.keys():
            lead_id = int(lead)
            was_in_a_new_application = convert_item(lead=lead, custom_fields_dict=data, need_item='1')
            was_in_manager = convert_item(lead=lead, custom_fields_dict=data, need_item='2')
            was_in_hired = convert_item(lead=lead, custom_fields_dict=data, need_item='3')
            was_trying_to_contact = convert_item(lead=lead, custom_fields_dict=data, need_item='4')
            was_in_presentation_sent = convert_item(lead=lead, custom_fields_dict=data, need_item='5')
            was_in_contact = convert_item(lead=lead, custom_fields_dict=data, need_item='6')
            was_in_a_meeting_start = convert_item(lead=lead, custom_fields_dict=data, need_item='7')
            was_in_a_meeting_close = convert_item(lead=lead, custom_fields_dict=data, need_item='8')
            was_held_in_meetings = convert_item(lead=lead, custom_fields_dict=data, need_item='9')
            was_waiting_for_prepayment = convert_item(lead=lead, custom_fields_dict=data, need_item='10')
            was_in_receiving_prepayment = convert_item(lead=lead, custom_fields_dict=data, need_item='11')
            was_in_details_received = convert_item(lead=lead, custom_fields_dict=data, need_item='12')
            was_sent_to_a_lawyer_in_the_agreement = convert_item(lead=lead, custom_fields_dict=data, need_item='13')
            was_sent_to_the_client_in_the_agreement = convert_item(lead=lead, custom_fields_dict=data, need_item='14')
            was_agreed_to_the_contract = convert_item(lead=lead, custom_fields_dict=data, need_item='15')
            was_signed_in_the_treaty = convert_item(lead=lead, custom_fields_dict=data, need_item='16')
            was_in_the_first_payment = convert_item(lead=lead, custom_fields_dict=data, need_item='17')
            was_in_the_second_payment = convert_item(lead=lead, custom_fields_dict=data, need_item='18')
            was_in_third_payment = convert_item(lead=lead, custom_fields_dict=data, need_item='19')
            was_in_won = convert_item(lead=lead, custom_fields_dict=data, need_item='20')
            was_in_lost = convert_item(lead=lead, custom_fields_dict=data, need_item='21')
            date_new_application = convert_item(lead=lead, custom_fields_dict=data, need_item='22')
            date_new_application = convert_unix_to_date_time(date_new_application)
            date_manager_appointed = convert_item(lead=lead, custom_fields_dict=data, need_item='23')
            date_manager_appointed = convert_unix_to_date_time(date_manager_appointed)
            date_hired = convert_item(lead=lead, custom_fields_dict=data, need_item='24')
            date_hired = convert_unix_to_date_time(date_hired)
            date_attempt_to_contact = convert_item(lead=lead, custom_fields_dict=data, need_item='25')
            date_attempt_to_contact = convert_unix_to_date_time(date_attempt_to_contact)
            date_presentation_sent = convert_item(lead=lead, custom_fields_dict=data, need_item='26')
            date_presentation_sent = convert_unix_to_date_time(date_presentation_sent)
            date_contact_took_place = convert_item(lead=lead, custom_fields_dict=data, need_item='27')
            date_contact_took_place = convert_unix_to_date_time(date_contact_took_place)
            date_appointed = convert_item(lead=lead, custom_fields_dict=data, need_item='28')
            date_appointed = convert_unix_to_date_time(date_appointed)
            date_meeting_canceled = convert_item(lead=lead, custom_fields_dict=data, need_item='29')
            date_meeting_canceled = convert_unix_to_date_time(date_meeting_canceled)
            date_meeting_held = convert_item(lead=lead, custom_fields_dict=data, need_item='30')
            date_meeting_held = convert_unix_to_date_time(date_meeting_held)
            date_awaiting_payment = convert_item(lead=lead, custom_fields_dict=data, need_item='31')
            date_awaiting_payment = convert_unix_to_date_time(date_awaiting_payment)
            date_prepayment_received = convert_item(lead=lead, custom_fields_dict=data, need_item='32')
            date_prepayment_received = convert_unix_to_date_time(date_prepayment_received)
            date_details_received = convert_item(lead=lead, custom_fields_dict=data, need_item='33')
            date_details_received = convert_unix_to_date_time(date_details_received)
            date_contract_sent_to_lawyer = convert_item(lead=lead, custom_fields_dict=data, need_item='34')
            date_contract_sent_to_lawyer = convert_unix_to_date_time(date_contract_sent_to_lawyer)
            date_contract_sent_to_customer = convert_item(lead=lead, custom_fields_dict=data, need_item='35')
            date_contract_sent_to_customer = convert_unix_to_date_time(date_contract_sent_to_customer)
            date_contract_agreed = convert_item(lead=lead, custom_fields_dict=data, need_item='36')
            date_contract_agreed = convert_unix_to_date_time(date_contract_agreed)
            date_contract_signed = convert_item(lead=lead, custom_fields_dict=data, need_item='37')
            date_contract_signed = convert_unix_to_date_time(date_contract_signed)
            date_first_payment = convert_item(lead=lead, custom_fields_dict=data, need_item='38')
            date_first_payment = convert_unix_to_date_time(date_first_payment)
            date_second_payment = convert_item(lead=lead, custom_fields_dict=data, need_item='39')
            date_second_payment = convert_unix_to_date_time(date_second_payment)
            date_third_payment = convert_item(lead=lead, custom_fields_dict=data, need_item='40')
            date_third_payment = convert_unix_to_date_time(date_third_payment)
            date_won = convert_item(lead=lead, custom_fields_dict=data, need_item='41')
            date_won = convert_unix_to_date_time(date_won)
            date_lost = convert_item(lead=lead, custom_fields_dict=data, need_item='42')
            date_lost = convert_unix_to_date_time(date_lost)
            application_source = convert_item(lead=lead, custom_fields_dict=data, need_item='43')
            taken = convert_item(lead=lead, custom_fields_dict=data, need_item='44')
            take_speed = convert_item(lead=lead, custom_fields_dict=data, need_item='45')
            take_speed = convert_unix_to_date_time(take_speed)
            rejection_reason = convert_item(lead=lead, custom_fields_dict=data, need_item='46')
            failure_detail = convert_item(lead=lead, custom_fields_dict=data, need_item='47')
            partner_agent = convert_item(lead=lead, custom_fields_dict=data, need_item='48')
            project = convert_item(lead=lead, custom_fields_dict=data, need_item='49')
            language = convert_item(lead=lead, custom_fields_dict=data, need_item='50')
            formname = convert_item(lead=lead, custom_fields_dict=data, need_item='51')
            transaction_classification = convert_item(lead=lead, custom_fields_dict=data, need_item='52')

            record_to_insert = (
                lead_id, was_in_a_new_application, was_in_manager, was_in_hired, was_trying_to_contact,
                was_in_presentation_sent, was_in_contact, was_in_a_meeting_start, was_in_a_meeting_close,
                was_held_in_meetings, was_waiting_for_prepayment, was_in_receiving_prepayment,
                was_in_details_received, was_sent_to_a_lawyer_in_the_agreement,
                was_sent_to_the_client_in_the_agreement, was_agreed_to_the_contract, was_signed_in_the_treaty,
                was_in_the_first_payment, was_in_the_second_payment, was_in_third_payment,
                was_in_won, was_in_lost, date_new_application, date_manager_appointed,
                date_hired, date_attempt_to_contact, date_presentation_sent, date_contact_took_place,
                date_appointed, date_meeting_canceled, date_meeting_held, date_awaiting_payment,
                date_prepayment_received, date_details_received, date_contract_sent_to_lawyer,
                date_contract_sent_to_customer, date_contract_agreed, date_contract_signed, date_first_payment, date_second_payment,
                date_third_payment, date_won, date_lost, application_source, taken, take_speed, rejection_reason,
                failure_detail, partner_agent, project, language, formname, transaction_classification)

            records_to_insert.append(record_to_insert)
        return records_to_insert
    except Exception as error:
        logging.error(f'get_custom_fields_record: {error}')


def get_utm_record(data: json) -> list:
    """Возвращает список записей для записи в базу"""
    logging.info('Возвращаю список записей для записи в базу')
    records_to_insert = []
    try:
        for lead in data.keys():
            lead_id = int(lead)
            fbclid = convert_item_2(lead=lead, custom_fields_dict=data, need_item='1')
            yclid = convert_item_2(lead=lead, custom_fields_dict=data, need_item='2')
            gclid = convert_item_2(lead=lead, custom_fields_dict=data, need_item='3')
            gclientid = convert_item_2(lead=lead, custom_fields_dict=data, need_item='4')
            utm_from = convert_item_2(lead=lead, custom_fields_dict=data, need_item='5')
            utm_source = convert_item_2(lead=lead, custom_fields_dict=data, need_item='6')
            utm_medium = convert_item_2(lead=lead, custom_fields_dict=data, need_item='7')
            utm_campaign = convert_item_2(lead=lead, custom_fields_dict=data, need_item='8')
            utm_term = convert_item_2(lead=lead, custom_fields_dict=data, need_item='9')
            utm_content = convert_item_2(lead=lead, custom_fields_dict=data, need_item='10')
            utm_referrer = convert_item_2(lead=lead, custom_fields_dict=data, need_item='11')
            ym_uid = convert_item_2(lead=lead, custom_fields_dict=data, need_item='12')
            ym_counter = convert_item_2(lead=lead, custom_fields_dict=data, need_item='13')
            roistat = convert_item_2(lead=lead, custom_fields_dict=data, need_item='14')

            record_to_insert = (lead_id, fbclid, yclid, gclid, gclientid, utm_from, utm_source, utm_medium,
                                utm_campaign, utm_term, utm_content, utm_referrer, ym_uid, ym_counter, roistat)

            records_to_insert.append(record_to_insert)
        return records_to_insert
    except Exception as error:
        logging.error(f'get_utm_record: {error}')


def get_lead_status_changed(data) -> list:
    """Возвращает словарь, где указан id лида и дата перехода в этап воронки"""
    archive_pipelines = read_data_file(name_of_data='archive_pipelines', page_num=1, extra_prefix='Dict')
    block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
    try:
        status_changed = [[lead['entity_id'], lead['created_at']] for lead in data['_embedded']['events'] if
                          (str(lead['value_after'][0]['lead_status']['pipeline_id']) not in archive_pipelines and
                           str(lead['value_after'][0]['lead_status']['pipeline_id']) not in block_pipelines)]

        final_status_changed = {}
        for status in status_changed:
            if status[0] not in final_status_changed:
                final_status_changed[status[0]] = status[1]
            else:
                if status[1] > final_status_changed[status[0]]:
                    final_status_changed[status[0]] = status[1]

        records = []
        for k, v in final_status_changed.items():
            records.append((convert_unix_to_date_time(v), k))

        return records
    except Exception as error:
        logging.error(f'get_lead_status_changed: {error}')


def get_lead_status_changed_update(data) -> list:
    """Возвращает словарь, где указан id лида и дата перехода в этап воронки"""
    archive_pipelines = read_data_file(name_of_data='archive_pipelines', page_num=1, extra_prefix='Dict')
    block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
    try:
        status_changed = [[lead['created_at'], lead['entity_id']] for lead in data['_embedded']['events'] if
                          (str(lead['value_after'][0]['lead_status']['pipeline_id']) not in archive_pipelines and
                           str(lead['value_after'][0]['lead_status']['pipeline_id']) not in block_pipelines) and
                          (convert_unix_to_date(lead['created_at']) >= datetime.date.today() or
                           convert_unix_to_date(lead['created_at']) == (
                                   datetime.date.today() - datetime.timedelta(days=1)))]

        final_status_changed = {}
        for status in status_changed:
            if status[0] not in final_status_changed:
                final_status_changed[status[0]] = status[1]
            else:
                if status[1] > final_status_changed[status[0]]:
                    final_status_changed[status[0]] = status[1]

        records = []
        for k, v in final_status_changed.items():
            records.append((convert_unix_to_date_time(v), k))

        return records
    except Exception as error:
        logging.error(f'get_lead_status_changed_update: {error}')


def get_id_deleted_leads(data) -> list:
    """Возвращает список id для удаления"""
    delete_list = []
    try:
        for lead in data['_embedded']['leads']:
            delete_list.append((lead['id'],))

        return delete_list
    except Exception as error:
        logging.error(f'get_id_deleted_leads: {error}')


def get_lost_stage(data) -> list:
    """Возвращает список этап проигрыша + ид"""
    try:
        archive_pipelines = read_data_file(name_of_data='archive_pipelines', page_num=1, extra_prefix='Dict')
        statuses_dict = read_data_file(name_of_data='statuses', extra_prefix='Dict')
        block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
        lost_stage_list = []
        for lead in data['_embedded']['events']:
            if str(lead['value_after'][0]['lead_status']['pipeline_id']) not in archive_pipelines:
                if str(lead['value_after'][0]['lead_status']['pipeline_id']) not in block_pipelines:
                    if lead['value_after'][0]['lead_status']['id'] == 143:
                        lost_stage_list.append([(lead['value_before'][0]['lead_status']['pipeline_id'],
                             lead['value_before'][0]['lead_status']['id']), lead['entity_id']])

        final_lost_stage = []
        for lead in lost_stage_list:
            pipeline = lead[0][0]
            if pipeline not in [5298025, 6229330, 5297782, 6245694, 6181218, 6437666, 6693290, 62293]:
                pipeline = statuses_dict[f'{pipeline}']
                for stage in pipeline:
                    if stage[0] == lead[0][1]:
                        final_lost_stage.append((stage[1], lead[1]))

        return final_lost_stage

    except Exception as error:
        logging.error(f'get_lost_stage: {error}')


def get_lost_stage_update(data):
    """Возвращает список этап проигрыша + ид"""
    try:
        archive_pipelines = read_data_file(name_of_data='archive_pipelines', page_num=1, extra_prefix='Dict')
        statuses_dict = read_data_file(name_of_data='statuses', extra_prefix='Dict')
        block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')

        lost_stage_list = []
        for lead in data['_embedded']['events']:
            if convert_unix_to_date(lead['created_at']) >= datetime.date.today() or \
                    convert_unix_to_date(lead['created_at']) == (datetime.date.today() - datetime.timedelta(days=1)):
                if str(lead['value_after'][0]['lead_status']['pipeline_id']) not in archive_pipelines:
                    if str(lead['value_after'][0]['lead_status']['pipeline_id']) not in block_pipelines:
                        if lead['value_after'][0]['lead_status']['id'] == 143:
                            lost_stage_list.append([(lead['value_before'][0]['lead_status']['pipeline_id'],
                                 lead['value_before'][0]['lead_status']['id']), lead['entity_id']])

        final_lost_stage = []
        for lead in lost_stage_list:
            pipeline = lead[0][0]
            if pipeline not in [5298025, 6229330, 5297782, 6245694, 6181218, 6437666, 6693290, 62293]:
                pipeline = statuses_dict[f'{pipeline}']
                for stage in pipeline:
                    if stage[0] == lead[0][1]:
                        final_lost_stage.append((stage[1], lead[1]))

        return final_lost_stage

    except Exception as error:
        logging.error(f'get_lost_stage_update: {error}')
