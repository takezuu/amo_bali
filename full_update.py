import DataFunc
from MainFunc import create_dict, create_api, update_insert, update_token, create_api_filter, \
    delete_all_files, get_deleated_lead, delete_deleted_leads, update_insert_reverse
from DB_Operations import full_update_leads, update_custom_fields, \
    update_utm_table, update_leads_pipelines_status_date, \
    delete_leads, update_lost_stage, insert_object_table, insert_finance_table, update_object_table, \
    update_finance_table, delete_custom_fields, delete_finance, delete_object, delete_utm

# авторизация и получение нового токена
update_token()

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
create_dict(funcc=DataFunc.get_leads_custom_fields_dict, dict_name='leads',
            second_dict_name='leads_custom_fields',
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
# обновление leads_table
update_insert(funcc=DataFunc.get_lead_record, insert_funcc=full_update_leads, name_of_data='leads')

# запись дат перехода в статусы в leads_table
update_insert_reverse(funcc=DataFunc.get_lead_status_changed, insert_funcc=update_leads_pipelines_status_date,
                      name_of_data='lead_status_changed')

# запись custom_fields
update_insert(funcc=DataFunc.get_custom_fields_record, insert_funcc=update_custom_fields,
              extra_prefix='Dict', name_of_data='custom_fields')

# запись lost_stage в leads_table
update_insert(funcc=DataFunc.get_lost_stage, insert_funcc=update_lost_stage, name_of_data='lead_status_changed')

# запись object_fields
update_insert(funcc=DataFunc.get_object_fields_record, insert_funcc=update_object_table,
              extra_prefix='Dict', name_of_data='object_fields')

# запись finance_fields
update_insert(funcc=DataFunc.get_finance_fields_record, insert_funcc=update_finance_table,
              extra_prefix='Dict', name_of_data='finance_fields')

# запись utm_table
update_insert(funcc=DataFunc.get_utm_record, insert_funcc=update_utm_table,
              extra_prefix='Dict', name_of_data='utm')

get_deleated_lead(api_name='delete')
delete_deleted_leads(name_of_data='Deleted_leads', funcc=DataFunc.get_id_deleted_leads, delete_funcc1=delete_leads,
                     delete_funcc2=delete_custom_fields, delete_funcc3=delete_finance, delete_funcc4=delete_object,
                     delete_funcc5=delete_utm)

# удаляет созданные файлы
delete_all_files()
