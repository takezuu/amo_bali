import DataFunc
from MainFunc import create_dict, create_api, update_insert, update_token, create_api_filter, pipeline_statuses_count, \
    delete_all_files, get_token, get_deleated_lead, delete_deleted_leads
from DB_Operations import update_leads, update_custom_fields, update_utm_table, update_leads_pipelines_status_date, \
    delete_leads

# авторизация и получение нового токена
# update_token()

# запрос файлов из апи
create_api(api_name='users')
create_api(api_name='pipelines')
create_api(api_name='leads')
create_api_filter(api_name='lead_status_changed')

# создание словарей
create_dict(funcc=DataFunc.get_users, dict_name='users', prefix='Dict')
create_dict(funcc=DataFunc.get_pipelines, dict_name='pipelines', prefix='Dict')
create_dict(funcc=DataFunc.get_archive_pipelines, dict_name='pipelines', second_dict_name='archive_pipelines',
            prefix='Dict')
create_dict(funcc=DataFunc.get_statuses, dict_name='pipelines', second_dict_name='statuses', prefix='Dict')

create_dict(funcc=DataFunc.get_leads_custom_fields_dict_update, dict_name='leads',
            second_dict_name='leads_custom_fields',
            prefix='Dict')
create_dict(funcc=DataFunc.get_custom_fields_dict, dict_name='leads_custom_fields',
            second_dict_name='custom_fields', prefix='Dict', extra_prefix='Dict')
create_dict(funcc=DataFunc.get_utm_dict, dict_name='leads_custom_fields', second_dict_name='utm', prefix='Dict',
            extra_prefix='Dict')

# запись в базу
# подготовка словарей для записи leads_table
pipelines_dict = DataFunc.read_data_file(name_of_data='pipelines', extra_prefix='Dict')
archive_pipelines = DataFunc.read_data_file(name_of_data='archive_pipelines', extra_prefix='Dict')
statuses_dict = DataFunc.read_data_file(name_of_data='statuses', extra_prefix='Dict')
users_dict = DataFunc.read_data_file(name_of_data='users', extra_prefix='Dict')
group_dict1 = DataFunc.read_data_file(name_of_data='group', extra_prefix='Dict')

# обновление leads_table
update_insert(funcc=DataFunc.get_lead_update_record, insert_funcc=update_leads, name_of_data='leads',
              pipelines_dict=pipelines_dict, archive_pipelines=archive_pipelines, statuses_dict=statuses_dict,
              users_dict=users_dict, group_dict=group_dict1)

# запись дат перехода в статусы в leads_table
update_insert(funcc=DataFunc.get_lead_status_changed_update, insert_funcc=update_leads_pipelines_status_date,
              name_of_data='lead_status_changed')

# запись custom_fields
update_insert(funcc=DataFunc.get_custom_fields_record, insert_funcc=update_custom_fields,
              extra_prefix='Dict', name_of_data='custom_fields')

# запись utm_table
update_insert(funcc=DataFunc.get_utm_record, insert_funcc=update_utm_table,
              extra_prefix='Dict', name_of_data='utm')

get_deleated_lead(api_name='delete')
delete_deleted_leads(name_of_data='Deleted_leads', funcc=DataFunc.get_id_deleted_leads, delete_funcc=delete_leads)


# удаляет созданные файлы
delete_all_files()

