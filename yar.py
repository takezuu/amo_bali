import os
import datetime

from paths import my_log, my_f

i = 0
file_errors = open(my_f + 'result_yar.txt', 'a', encoding='utf-8')
with open(my_log, 'r',  encoding='utf-8') as file:
    f = file.readlines()
    for row in f:
        if 'ERROR' in row:
            file_errors.write(row)
            i += 1
if i == 0:
    file_errors.write('ошибок нет, Ярик доволен :) дата\n' + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    file_errors.close()
else:
    file_errors.write('кол-во ошибок на ' + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + f': {i}\n')
    file_errors.close()

if i == 0:
    os.remove(os.path.join(my_f, 'analytic.log'))
