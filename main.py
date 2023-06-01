import DataFunc
from MainFunc import first_insert, create_dict, create_api, get_token, create_api_filter, delete_all_files, \
    first_insert_reverse
from DB_Operations import insert_leads, insert_custom_fields, insert_utm_table, update_leads_pipelines_status_date, \
    update_lost_stage, insert_object_table, insert_finance_table

# авторизация и получение первого токена
get_token()
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
create_dict(funcc=DataFunc.get_leads_custom_fields_dict, dict_name='leads', second_dict_name='leads_custom_fields',
            prefix='Dict')
create_dict(funcc=DataFunc.get_custom_fields_dict, dict_name='leads_custom_fields',
            second_dict_name='custom_fields', prefix='Dict', extra_prefix='Dict')
create_dict(funcc=DataFunc.get_custom_object_dict, dict_name='leads_custom_fields',
            second_dict_name='object_fields', prefix='Dict', extra_prefix='Dict')
create_dict(funcc=DataFunc.get_custom_finance_dict, dict_name='leads_custom_fields',
            second_dict_name='finance_fields', prefix='Dict', extra_prefix='Dict')
create_dict(funcc=DataFunc.get_utm_dict, dict_name='leads_custom_fields', second_dict_name='utm', prefix='Dict',
            extra_prefix='Dict')

# запись в базу
# первая запись в leads_table
first_insert(funcc=DataFunc.get_lead_record, insert_funcc=insert_leads, name_of_data='leads')

# запись дат перехода в статусы в leads_table
first_insert_reverse(funcc=DataFunc.get_lead_status_changed, insert_funcc=update_leads_pipelines_status_date,
                     name_of_data='lead_status_changed')

# запись custom_fields
first_insert(funcc=DataFunc.get_custom_fields_record, insert_funcc=insert_custom_fields,
             extra_prefix='Dict', name_of_data='custom_fields')

# запись lost_stage в leads_table
first_insert(funcc=DataFunc.get_lost_stage, insert_funcc=update_lost_stage, name_of_data='lead_status_changed')

# запись object_fields
first_insert(funcc=DataFunc.get_object_fields_record, insert_funcc=insert_object_table,
             extra_prefix='Dict', name_of_data='object_fields')

# запись finance_fields
first_insert(funcc=DataFunc.get_finance_fields_record, insert_funcc=insert_finance_table,
             extra_prefix='Dict', name_of_data='finance_fields')

# запись utm_table
first_insert(funcc=DataFunc.get_utm_record, insert_funcc=insert_utm_table,
             extra_prefix='Dict', name_of_data='utm')

# удаляет созданные файлы
delete_all_files()
