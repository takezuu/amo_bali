import json
import datetime


def write_tokens(tokens: dict) -> None:
    """Записывает токены в файл"""
    with open('tokens', 'w', encoding='utf-8') as file:
        json.dump(tokens, file, indent=4)


def write_data(data, name_of_data: str, page_num=None, prefix=None, second_dict_name=None) -> None:
    """Записывает сделки в файл"""
    if second_dict_name is not None:
        name_of_data = second_dict_name
    if prefix is not None:
        file_path = f'{prefix}'.capitalize() + '/' f'{name_of_data}' + '_dict' + f'{page_num}'
    else:
        file_path = f'{name_of_data}'.capitalize() + '/' f'{name_of_data}' + f'{page_num}'

    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, sort_keys=False, ensure_ascii=False, indent=4)
        print('Json получен')


def read_token() -> json:
    """Читает токен из файла"""
    with open('tokens', 'r', encoding='utf-8') as file:
        return json.load(file)


def read_data_file(name_of_data: str, page_num=1, extra_prefix=None) -> json:
    """Читает файл"""
    if extra_prefix is not None:
        file_path = f'{extra_prefix}'.capitalize() + '/' f'{name_of_data}' + '_dict' f'{page_num}'
    else:
        file_path = f'{name_of_data}'.capitalize() + '/' f'{name_of_data}' + f'{page_num}'
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)


def check_next_api_page(file_data: json) -> bool:
    """Возвращает True/False наличие след. ссылки"""
    try:
        if file_data['_links']['next']:
            return True
    except KeyError:
        print('Получил все файлы!')
        return False


def convert_time(unix_time: int) -> datetime:
    """Конвертирует время из unix в обычный формат"""
    try:
        time = datetime.datetime.fromtimestamp(unix_time).date()
    except TypeError:
        return None
    else:
        return time


def convert_group_id(group_id: int, group_dict) -> str:
    """Конвертирует group id"""
    if group_dict[str(group_id)]:
        return group_dict[str(group_id)]
    else:
        return 'Нет группы'


def convert_status_id(status_id: str, statuses_dict: dict, pipeline_id):
    """Конвертирует status id в статус"""
    return list((status[1] for status in statuses_dict[str(pipeline_id)] if status[0] == status_id))[0]


def convert_responsible_id(responsible_id: int, users_dict: dict) -> str or None:
    """Конвертирует responsible_id в пользователя"""
    responsible_id_str = str(responsible_id)
    if users_dict[responsible_id_str] is not None:
        return users_dict[responsible_id_str]
    else:
        return None


def convert_pipeline_id(user_pipeline_id: int, pipelines_dict: dict) -> str or None:
    """Конвертирует user_pipeline_id в статус в воронке"""
    user_pipeline_id_str = str(user_pipeline_id)
    if pipelines_dict[user_pipeline_id_str] is not None:
        return pipelines_dict[user_pipeline_id_str]
    else:
        return None


def status_of_lead(status_id: int) -> str:
    """Возвращает статус сделки"""
    if status_id == 143:
        return 'Проиграна'
    elif status_id == 142:
        return 'Выиграна'
    else:
        return 'Активная'


def convert_have_task(time) -> str:
    """Возвращает наличие задачи"""
    if time is None:
        return 'No'
    else:
        return 'Yes'


def convert_task_time(closest_task_at):
    """Возвращает просрочена задача или нет"""
    if closest_task_at is not None:
        if closest_task_at < datetime.date.today():
            return 'Yes'
        else:
            return 'No'
    else:
        return None


def get_lead_record(data: json, pipelines_dict, statuses_dict, users_dict, group_dict, archive_pipelines) -> list:
    """Подготовливает строки для записи в базу"""
    block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
    records_to_insert = []
    leads = data['_embedded']['leads']
    for lead in leads:
        if (str(lead['pipeline_id']) not in archive_pipelines) or (str(lead['pipeline_id']) not in block_pipelines):
            lead_id = lead['id']
            name = lead['name']
            price = lead['price']
            responsible = convert_responsible_id(lead['responsible_user_id'], users_dict=users_dict)
            group_id = convert_group_id(lead['group_id'], group_dict=group_dict)
            pipeline = convert_pipeline_id(lead['pipeline_id'], pipelines_dict=pipelines_dict)
            pipeline_status = convert_status_id(lead['status_id'], statuses_dict=statuses_dict,
                                                pipeline_id=lead['pipeline_id'])
            pipeline_date = None
            created_at = convert_time(lead['created_at'])
            updated_at = convert_time(lead['updated_at'])
            closed_at = convert_time(lead['closed_at'])
            closest_task_at = convert_time(lead['closest_task_at'])
            lead_status = status_of_lead(lead['status_id'])
            have_task = convert_have_task(closest_task_at)
            overdue_task = convert_task_time(closest_task_at)

            record_to_insert = (
                lead_id, name, price, lead_status, responsible, group_id, pipeline, pipeline_status, pipeline_date,
                created_at, updated_at, closed_at, closest_task_at, have_task, overdue_task)

            records_to_insert.append(record_to_insert)
    return records_to_insert


def get_lead_update_record(data: json, pipelines_dict, statuses_dict, users_dict, group_dict,
                           archive_pipelines) -> list:
    """Подготовливает строки для записи в базу"""
    block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
    records_to_insert = []
    leads = data['_embedded']['leads']
    for lead in leads:
        if ((str(lead['pipeline_id']) not in archive_pipelines) or (str(lead['pipeline_id']) not in block_pipelines)) and (
                convert_time(lead['updated_at']) >= datetime.date.today() or convert_time(lead['updated_at']) == (
                datetime.date.today() - datetime.timedelta(days=1))):
            lead_id = lead['id']
            name = lead['name']
            price = lead['price']
            responsible = convert_responsible_id(lead['responsible_user_id'], users_dict=users_dict)
            group_id = convert_group_id(lead['group_id'], group_dict=group_dict)
            pipeline = convert_pipeline_id(lead['pipeline_id'], pipelines_dict=pipelines_dict)
            pipeline_status = convert_status_id(lead['status_id'], statuses_dict=statuses_dict,
                                                pipeline_id=lead['pipeline_id'])
            created_at = convert_time(lead['created_at'])
            updated_at = convert_time(lead['updated_at'])
            closed_at = convert_time(lead['closed_at'])
            closest_task_at = convert_time(lead['closest_task_at'])
            lead_status = status_of_lead(lead['status_id'])
            have_task = convert_have_task(closest_task_at)
            overdue_task = convert_task_time(closest_task_at)

            record_to_insert = (
                lead_id, name, price, lead_status, responsible, group_id, pipeline, pipeline_status,
                created_at, updated_at, closed_at, closest_task_at, have_task, overdue_task)

            records_to_insert.append(record_to_insert)
    return records_to_insert


def get_users(users: json) -> dict:
    """Возвращает словарь пользователей id+name"""
    users = users['_embedded']['users']
    print('Словарь пользователей готов')
    return {user['id']: user['name'] for user in users}


def get_pipelines(pipelines: json) -> dict:
    """Возвращает словарь воронок id+name"""
    pipelines = pipelines['_embedded']['pipelines']
    print('Словарь воронок готов')
    return {pipeline['id']: pipeline['name'] for pipeline in pipelines if not pipeline['is_archive']}


def get_archive_pipelines(pipelines: json) -> dict:
    """Возвращает словарь архивных воронок id+name"""
    pipelines = pipelines['_embedded']['pipelines']
    print('Словарь воронок готов')
    return {pipeline['id']: pipeline['name'] for pipeline in pipelines if pipeline['is_archive']}


def get_statuses(pipelines: json) -> dict:
    """Возвращает словарь воронок и этапов воронки id+name+statuses"""
    pipelines = pipelines['_embedded']['pipelines']
    print('Словарь воронок и этапов готов')
    return {pipeline['id']: [[status['id'], status['name']] for status in pipeline['_embedded']['statuses']] for
            pipeline in pipelines if not pipeline['is_archive']}


def get_leads_custom_fields_dict(leads) -> dict:
    """Возвращает словарь лидов с дополнительными полями"""
    archive_pipelines = read_data_file(name_of_data='archive_pipelines', page_num=1, extra_prefix='Dict')
    block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
    leads = leads['_embedded']['leads']
    print('Словарь пользователей и доп.полей готов')
    return {lead['id']: [[field['field_name'], field['values'][0]['value']] for field in lead['custom_fields_values']]
            for lead in leads if lead['custom_fields_values'] and (str(lead['pipeline_id']) not in archive_pipelines or
            str(lead['pipeline_id']) not in block_pipelines)}


def get_leads_custom_fields_dict_update(leads) -> dict:
    """Возвращает словарь лидов с дополнительными полями для обновления"""
    archive_pipelines = read_data_file(name_of_data='archive_pipelines', page_num=1, extra_prefix='Dict')
    block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
    leads = leads['_embedded']['leads']
    print('Словарь пользователей и доп.полей готов для обновления')
    return {lead['id']: [[field['field_name'], field['values'][0]['value']] for field in lead['custom_fields_values']]
            for lead in leads if lead['custom_fields_values'] and ((str(lead['pipeline_id']) not in archive_pipelines
                                                                    or str(lead['pipeline_id']) not in block_pipelines)
                                                                   and (convert_time(
                    lead['updated_at']) >= datetime.date.today() or convert_time(lead['updated_at']) ==
                                                                        (datetime.date.today() - datetime.timedelta(
                                                                            days=1))))}


def get_custom_fields_dict(leads_custom_fields_dict: dict) -> dict:
    """Возвращает преобразовыннй словарь лидов с дополнительными полями"""
    custom_fields = {'Источник заявки': 1, 'Форма заявки': 2, 'Рекламный канал заявки': 3, 'Приоритет клиента': 4,
                     'Условия оплаты': 5,
                     'Сумма первого платежа': 6, 'Дата первого платежа': 7, 'Сумма второго платежа': 8,
                     'Дата второго платежа': 9, 'Остаток оплаты': 10, 'Дата подписания договора': 11,
                     'Причина отказа': 12}
    leads_dict = leads_custom_fields_dict

    custom_fields_dict = {
        lead: [{custom_fields[field[0]]: field[1]} for field in leads_dict[lead] if field[0] in custom_fields] for lead
        in leads_dict.keys()}
    # очищение от пустых лидов без доп.полей
    custom_fields_dict = {lead: custom_fields for lead, custom_fields in custom_fields_dict.items() if custom_fields}
    print('Словарь доп.полей готов')
    return custom_fields_dict


def get_utm_dict(leads_custom_fields_dict: dict) -> dict:
    """Возвращает преобразовыннй словарь лидов с дополнительными полями"""
    custom_fields = {'fbclid': 1, 'yclid': 2, 'gclid': 3, 'gclientid': 4, 'from': 5,
                     'utm_source': 6, 'utm_medium': 7, 'utm_campaign': 8, 'utm_term': 9, 'utm_content': 10,
                     'utm_referrer': 11, '_ym_uid': 12, '_ym_counter': 13, 'roistat': 14}
    leads_dict = leads_custom_fields_dict

    utm_dict = {
        lead: [{custom_fields[field[0]]: field[1]} for field in leads_dict[lead] if field[0] in custom_fields] for lead
        in leads_dict.keys()}
    # очищение от пустых лидов без доп.полей
    utm_dict = {lead: custom_fields for lead, custom_fields in utm_dict.items() if custom_fields}
    print('Словарь доп.полей готов')
    return utm_dict


def convert_item(lead: str, custom_fields_dict: dict, need_item: str):
    """Конвертирует элемент и возвращает его"""
    for item in custom_fields_dict[lead]:
        if need_item in item:
            if need_item == '6' or need_item == '8' or need_item == '10':
                try:
                    return int(item[str(need_item)])
                except ValueError:
                    return float(item[str(need_item)])
            else:
                return item[str(need_item)]


def convert_item_2(lead: str, custom_fields_dict: dict, need_item: str):
    """Конвертирует элемент и возвращает его"""
    for item in custom_fields_dict[lead]:
        if need_item in item:
            return str(item[str(need_item)])


def get_custom_fields_record(data: json) -> list:
    """Возвращает список записей для записи в базу"""
    records_to_insert = []
    for lead in data.keys():
        lead_id = int(lead)
        application_source = convert_item(lead=lead, custom_fields_dict=data, need_item='1')
        application_form = convert_item(lead=lead, custom_fields_dict=data, need_item='2')
        advertising_channel = convert_item(lead=lead, custom_fields_dict=data, need_item='3')
        customer_priority = convert_item(lead=lead, custom_fields_dict=data, need_item='4')
        terms_of_payment = convert_item(lead=lead, custom_fields_dict=data, need_item='5')
        first_payment_amount = convert_item(lead=lead, custom_fields_dict=data, need_item='6')
        first_payment_date = convert_item(lead=lead, custom_fields_dict=data, need_item='7')
        first_payment_date = convert_time(first_payment_date)
        second_payment_amount = convert_item(lead=lead, custom_fields_dict=data, need_item='8')
        second_payment_date = convert_item(lead=lead, custom_fields_dict=data, need_item='9')
        second_payment_date = convert_time(second_payment_date)
        remaining_payment = convert_item(lead=lead, custom_fields_dict=data, need_item='10')
        contract_date = convert_item(lead=lead, custom_fields_dict=data, need_item='11')
        contract_date = convert_time(contract_date)
        rejection_reason = convert_item(lead=lead, custom_fields_dict=data, need_item='12')

        record_to_insert = (
            lead_id, application_source, application_form, advertising_channel, customer_priority, terms_of_payment,
            first_payment_amount, first_payment_date, second_payment_amount, second_payment_date, remaining_payment,
            contract_date, rejection_reason)

        records_to_insert.append(record_to_insert)
    return records_to_insert


def get_utm_record(data: json) -> list:
    """Возвращает список записей для записи в базу"""
    records_to_insert = []
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


def get_lead_status_changed(data) -> list:
    """Возвращает словарь, где указан id лида и дата перехода в этап воронки"""
    archive_pipelines = read_data_file(name_of_data='archive_pipelines', page_num=1, extra_prefix='Dict')
    block_pipelines = read_data_file(name_of_data='block_pipelines', page_num=1, extra_prefix='Dict')
    return [(convert_time(lead['created_at']), lead['entity_id']) for lead in data['_embedded']['events'] if
            ((lead['value_after'][0]['lead_status']['pipeline_id'] not in archive_pipelines) or
            (lead['value_after'][0]['lead_status']['pipeline_id'] not in block_pipelines)) and
            (convert_time(lead['created_at']) >= datetime.date.today() or
             convert_time(lead['created_at']) == (datetime.date.today() - datetime.timedelta(days=1)))]
