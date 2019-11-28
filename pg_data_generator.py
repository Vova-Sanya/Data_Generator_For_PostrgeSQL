import psycopg2
import uuid
import random
import time
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

MAX_USERS = 500000
MAX_CATEGORIES = 5000
MAX_MESSAGES = 10000000
HALF_MESSAGES = 5000000
T_START = datetime(2010, 1, 1, 00, 00, 00)
T_END = T_START + timedelta(days=365 * 10)
STATEMENTS_LIST = [
    'Uuid lists creation and containing completed in',
    'Data lists creation completed in',
    'List "users_array" filling completed in',
    'List "categories_array" filling completed in',
    'List "messages_array" filling completed in',
    'Users insertion completed in',
    'Categories insertion completed in',
    'First half messages insertion completed in',
    'List "messages_array2" filling completed in',
    'Second half messages insertion completed in'
]
START_TIME = time.time()

time_checkpoint = time.time()
count_num = 1


# ----------------------------------------------------------------------------------------------------------------------

# Функция для вывода времени работы программы и частей программы
def get_checkpoint_time(x):
    tt = time.time()
    time_end = tt - x
    time_end_int = int(time_end)
    time_end_float = time_end - time_end_int
    min_availability = False
    if time_end_int > 59:
        time_end_minutes = time_end_int // 60
        time_end_int -= time_end_minutes * 60
        min_availability = True
    if min_availability:
        time_string = "%s minutes, %s seconds, %s milliseconds" % (time_end_minutes, time_end_int, time_end_float)
    else:
        time_string = "%s seconds, %s milliseconds" % (time_end_int, time_end_float)
    return time_string


# Функция для записи времени начала работы блока программы
def get_start_block_time():
    global time_checkpoint
    time_checkpoint = time.time()


# Функция для вывода информации и времени работы блока программы
def pseudo_logger():
    global count_num
    global STATEMENTS_LIST
    global time_checkpoint
    print('[%d/10] --- %s --- [%s]' % (count_num, STATEMENTS_LIST[count_num - 1], get_checkpoint_time(time_checkpoint)))
    count_num += 1


# ----------------------------------------------------------------------------------------------------------------------

get_start_block_time()

# Создание и заполнение списков uuid
author_uuid = [str(uuid.uuid4()) for i in range(MAX_USERS)]
category_uuid = [str(uuid.uuid4()) for i in range(MAX_CATEGORIES)]
message_uuid = [str(uuid.uuid4()) for i in range(MAX_MESSAGES)]

pseudo_logger()

# ----------------------------------------------------------------------------------------------------------------------

get_start_block_time()

# Создание списков данных
users_list = [[''] * 2 for i in range(MAX_USERS)]
categories_list = [[''] * 3 for i in range(MAX_CATEGORIES)]
messages_list = [[''] * 5 for i in range(HALF_MESSAGES)]
messages_list2 = [[''] * 5 for i in range(HALF_MESSAGES)]

pseudo_logger()

# ----------------------------------------------------------------------------------------------------------------------

get_start_block_time()

# Заполнение списка пользователей
for i in range(MAX_USERS):
    users_list[i][0] = author_uuid[i]
    users_list[i][1] = 'User_' + str(i + 1)

pseudo_logger()

# ----------------------------------------------------------------------------------------------------------------------

get_start_block_time()

# Заполнение списка категорий
for i in range(MAX_CATEGORIES):
    categories_list[i][0] = category_uuid[i]
    categories_list[i][1] = 'Category_' + str(i + 1)
    categories_list[i][2] = category_uuid[i]

pseudo_logger()

# ----------------------------------------------------------------------------------------------------------------------

get_start_block_time()

# Заполнение первого списка сообщений
for i in range(HALF_MESSAGES):
    messages_list[i][0] = message_uuid[i]
    messages_list[i][1] = 'Text_' + str(i + 1)
    messages_list[i][2] = random.choice(category_uuid)
    messages_list[i][3] = T_START + (T_END - T_START) * random.random()
    messages_list[i][4] = random.choice(author_uuid)

pseudo_logger()

# ----------------------------------------------------------------------------------------------------------------------

# Подключение к базе данных
con = psycopg2.connect(
    host="localhost",
    database="green_bird",
    user="postgres",
    password="8080sql128080")
cur = con.cursor()

# ----------------------------------------------------------------------------------------------------------------------

get_start_block_time()

# Вставка пользователей в базу данных
execute_values(cur, "INSERT INTO users (id, name) VALUES %s", users_list)

pseudo_logger()

# ----------------------------------------------------------------------------------------------------------------------

get_start_block_time()

# Вставка категорий в базу данных
execute_values(cur, "INSERT INTO categories (id, name, parent_id) VALUES %s", categories_list)

pseudo_logger()

# ----------------------------------------------------------------------------------------------------------------------

get_start_block_time()

# Вставка сообщений в базу данных (Часть 1)
execute_values(cur, "INSERT INTO messages (id, text, category_id, posted_at, author_id) VALUES %s", messages_list)

pseudo_logger()

# ----------------------------------------------------------------------------------------------------------------------

# Удаление первого списка сообщений
del (messages_list)

# ----------------------------------------------------------------------------------------------------------------------

get_start_block_time()

# Заполнение второго списка сообщений
for i in range(HALF_MESSAGES):
    messages_list2[i][0] = message_uuid[i + HALF_MESSAGES]
    messages_list2[i][1] = 'Text_' + str(i + HALF_MESSAGES + 1)
    messages_list2[i][2] = random.choice(category_uuid)
    messages_list2[i][3] = T_START + (T_END - T_START) * random.random()
    messages_list2[i][4] = random.choice(author_uuid)

pseudo_logger()

# ----------------------------------------------------------------------------------------------------------------------

get_start_block_time()

# Вставка сообщений в базу данных (Часть 2)
execute_values(cur, "INSERT INTO messages (id, text, category_id, posted_at, author_id) VALUES %s", messages_list2)

pseudo_logger()

# ----------------------------------------------------------------------------------------------------------------------

# Окончание работы с базой данных
con.commit()
cur.close()
con.close()

# ----------------------------------------------------------------------------------------------------------------------

print()
print("Mission completed in " + get_checkpoint_time(START_TIME) + "! Congratulations!")
print("Powered by: Python 3.7, PyCharm, psycopg2")
