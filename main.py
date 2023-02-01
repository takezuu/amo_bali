import DataFunc
from MainFunc import first_insert, create_dict, create_api, get_token, create_api_filter, \
    pipeline_statuses_count, delete_all_files, update_token, first_insert_reverse
from DB_Operations import insert_leads, insert_custom_fields, insert_utm_table, update_leads_pipelines_status_date

# авторизация и получение первого токена
# get_token()
# update_token()
# запрос файлов из апи
# create_api(api_name='users')
# create_api(api_name='pipelines')
# create_api(api_name='leads')
# create_api_filter(api_name='lead_status_changed')


# создание словарей
# create_dict(funcc=DataFunc.get_users, dict_name='users', prefix='Dict')
# create_dict(funcc=DataFunc.get_pipelines, dict_name='pipelines', prefix='Dict')
# create_dict(funcc=DataFunc.get_archive_pipelines, dict_name='pipelines', second_dict_name='archive_pipelines',
#             prefix='Dict')
# create_dict(funcc=DataFunc.get_statuses, dict_name='pipelines', second_dict_name='statuses', prefix='Dict')
# create_dict(funcc=DataFunc.get_leads_custom_fields_dict, dict_name='leads', second_dict_name='leads_custom_fields',
#             prefix='Dict')
# create_dict(funcc=DataFunc.get_custom_fields_dict, dict_name='leads_custom_fields',
#             second_dict_name='custom_fields', prefix='Dict', extra_prefix='Dict')
# create_dict(funcc=DataFunc.get_utm_dict, dict_name='leads_custom_fields', second_dict_name='utm', prefix='Dict',
#             extra_prefix='Dict')

# запись в базу
# подготовка словарей для первой записи leads_table
# pipelines_dict = DataFunc.read_data_file(name_of_data='pipelines', extra_prefix='Dict')
# archive_pipelines = DataFunc.read_data_file(name_of_data='archive_pipelines', extra_prefix='Dict')
# statuses_dict = DataFunc.read_data_file(name_of_data='statuses', extra_prefix='Dict')
# users_dict = DataFunc.read_data_file(name_of_data='users', extra_prefix='Dict')
# group_dict1 = DataFunc.read_data_file(name_of_data='group', extra_prefix='Dict')

# первая запись в leads_table
# first_insert(funcc=DataFunc.get_lead_record, insert_funcc=insert_leads, name_of_data='leads',
#              pipelines_dict=pipelines_dict, archive_pipelines=archive_pipelines, statuses_dict=statuses_dict,
#              users_dict=users_dict, group_dict=group_dict1)

# запись дат перехода в статусы в leads_table
# first_insert_reverse(funcc=DataFunc.get_lead_status_changed, insert_funcc=update_leads_pipelines_status_date,
#              name_of_data='lead_status_changed')

# запись custom_fields
# first_insert(funcc=DataFunc.get_custom_fields_record, insert_funcc=insert_custom_fields,
#              extra_prefix='Dict', name_of_data='custom_fields')

# запись utm_table
# first_insert(funcc=DataFunc.get_utm_record, insert_funcc=insert_utm_table,
#              extra_prefix='Dict', name_of_data='utm')


# удаляет созданные файлы
# delete_all_files()
