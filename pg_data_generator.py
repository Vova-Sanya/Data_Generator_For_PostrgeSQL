import psycopg2
import uuid
import random
import time
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

start_time = time.time()


# ----------------------------------------------------------------------------------------------------------------------

# Функция для вывода времени работы программы и частей программы
def get_checkpoint_time(x):
    time_end_sec = int(time.time() - x)
    min_availability = False
    if time_end_sec > 59:
        time_end_minutes = time_end_sec // 60
        time_end_sec -= time_end_minutes * 60
        min_availability = True
    if min_availability:
        time_string = "%s minutes %s seconds" % (time_end_minutes, time_end_sec)
    else:
        time_string = "%s seconds" % time_end_sec
    return time_string


# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Создание констант количества данных
max_users = 500000
max_categories = 5000
max_messages = 10000000
half_messages = 5000000

print('[01/16] | Constants creation completed in...              | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Создание списков uuid
author_uuid = []
category_uuid = []
message_uuid = []

print('[02/16] | Uuid lists creation completed in...             | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Заполнение списков uuid
for i in range(max_users):
    author_uuid.append(str(uuid.uuid4()))
for i in range(max_categories):
    category_uuid.append(str(uuid.uuid4()))
for i in range(max_messages):
    message_uuid.append(str(uuid.uuid4()))

print('[03/16] | Uuid lists filling completed in...              | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Подготовка переменных для случайного времени отправки сообщения
t_start = datetime(2010, 1, 1, 00, 00, 00)
t_end = t_start + timedelta(days=365 * 10)

print('[04/16] | Datetime preparations completed in...           | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Создание списков данных
users_array = [[0] * 2 for i in range(max_users)]
categories_array = [[0] * 3 for i in range(max_categories)]
messages_array = [[0] * 5 for i in range(half_messages)]
messages_array2 = [[0] * 5 for i in range(half_messages)]

print('[05/16] | Data lists creation completed in...             | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Заполнение списка пользователей
for i in range(max_users):
    users_array[i][0] = author_uuid[i]
    users_array[i][1] = 'User_' + str(i + 1)

print('[06/16] | List "users_array" filling completed in...      | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Заполнение списка категорий
for i in range(max_categories):
    categories_array[i][0] = category_uuid[i]
    categories_array[i][1] = 'Category_' + str(i + 1)
    categories_array[i][2] = category_uuid[i]

print('[07/16] | List "categories_array" filling completed in... | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Заполнение первого списка сообщений
for i in range(half_messages):
    messages_array[i][0] = message_uuid[i]
    messages_array[i][1] = 'Text_' + str(i + 1)
    messages_array[i][2] = random.choice(category_uuid)
    messages_array[i][3] = t_start + (t_end - t_start) * random.random()
    messages_array[i][4] = random.choice(author_uuid)

print('[08/16] | List "messages_array" filling completed in...   | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Подключение к базе данных
con = psycopg2.connect(
    host="localhost",
    database="название_базы_данных",
    user="postgres",
    password="пароль")
cur = con.cursor()

print('[09/16] | Database connection completed in...             | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Вставка пользователей в базу данных
execute_values(cur, "INSERT INTO users (id, name) VALUES %s", users_array)

print('[10/16] | Users insertion completed in...                 | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Вставка категорий в базу данных
execute_values(cur, "INSERT INTO categories (id, name, parent_id) VALUES %s", categories_array)

print('[11/16] | Categories insertion completed in...            | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Вставка сообщений в базу данных (Часть 1)
execute_values(cur, "INSERT INTO messages (id, text, category_id, posted_at, author_id) VALUES %s", messages_array)

print('[12/16] | First half messages insertion completed in...   | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Удаление первого списка сообщений
del (messages_array)

print('[13/16] | List "messages_array" deleting completed in...  | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Заполнение второго списка сообщений
for i in range(half_messages):
    messages_array2[i][0] = message_uuid[i + half_messages]
    messages_array2[i][1] = 'Text_' + str(i + half_messages + 1)
    messages_array2[i][2] = random.choice(category_uuid)
    messages_array2[i][3] = t_start + (t_end - t_start) * random.random()
    messages_array2[i][4] = random.choice(author_uuid)

print('[14/16] | List "messages_array2" filling completed in...  | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Вставка сообщений в базу данных (Часть 2)
execute_values(cur, "INSERT INTO messages (id, text, category_id, posted_at, author_id) VALUES %s", messages_array2)

print('[15/16] | Second half messages insertion completed in...  | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Окончание работы с базой данных
con.commit()
cur.close()
con.close()

print('[16/16] | Closing completed in...                         | ' + get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

print()
print("Mission completed in " + get_checkpoint_time(start_time) + "! Congratulations!")
print("Powered by: Python 3.7, PyCharm, psycopg2")
