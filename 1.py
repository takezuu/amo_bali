import datetime

test = 1555266007

time = datetime.datetime.fromtimestamp(test)

print(time)

if time.date() < datetime.date.today():
    print('Yes')
else:
    print('No')