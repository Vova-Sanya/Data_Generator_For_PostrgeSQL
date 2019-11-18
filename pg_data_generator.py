import psycopg2
import uuid
import random
import time
from datetime import timedelta
from psycopg2.extras import execute_values
import colonter

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

# Создание списков uuid
author_uuid = []
category_uuid = []
message_uuid = []

colonter.logger_alpha_x('Uuid lists creation completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Заполнение списков uuid
for i in range(colonter.MAX_USERS):
    author_uuid.append(str(uuid.uuid4()))
for i in range(colonter.MAX_CATEGORIES):
    category_uuid.append(str(uuid.uuid4()))
for i in range(colonter.MAX_MESSAGES):
    message_uuid.append(str(uuid.uuid4()))

colonter.logger_alpha_x('Uuid lists filling completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Подготовка переменных для случайного времени отправки сообщения
t_end = colonter.T_START + timedelta(days=365 * 10)

colonter.logger_alpha_x('Datetime preparations completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Создание списков данных
users_array = [0] * colonter.MAX_USERS
for i in range(colonter.MAX_USERS):
    users_array[i] = [0] * 2

categories_array = [0] * colonter.MAX_CATEGORIES
for i in range(colonter.MAX_CATEGORIES):
    categories_array[i] = [0] * 3

messages_array = [0] * colonter.HALF_MESSAGES
for i in range(colonter.HALF_MESSAGES):
    messages_array[i] = [0] * 5

messages_array2 = [0] * colonter.HALF_MESSAGES
for i in range(colonter.HALF_MESSAGES):
    messages_array2[i] = [0] * 5

colonter.logger_alpha_x('Data lists creation completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Заполнение списка пользователей
for i in range(colonter.MAX_USERS):
    users_array[i][0] = author_uuid[i]
    users_array[i][1] = 'User_' + str(i + 1)

colonter.logger_alpha_x('List "users_array" filling completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Заполнение списка категорий
for i in range(colonter.MAX_CATEGORIES):
    categories_array[i][0] = category_uuid[i]
    categories_array[i][1] = 'Category_' + str(i + 1)
    categories_array[i][2] = category_uuid[i]

colonter.logger_alpha_x('List "categories_array" filling completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Заполнение первого списка сообщений
for i in range(colonter.HALF_MESSAGES):
    messages_array[i][0] = message_uuid[i]
    messages_array[i][1] = 'Text_' + str(i + 1)
    messages_array[i][2] = random.choice(category_uuid)
    messages_array[i][3] = colonter.T_START + (t_end - colonter.T_START) * random.random()
    messages_array[i][4] = random.choice(author_uuid)

colonter.logger_alpha_x('List "messages_array" filling completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Подключение к базе данных
con = psycopg2.connect(
    host="localhost",
    database="название",
    user="postgres",
    password="пароль")
cur = con.cursor()

colonter.logger_alpha_x('Database connection completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Вставка пользователей в базу данных
execute_values(cur, "INSERT INTO users (id, name) VALUES %s", users_array)

colonter.logger_alpha_x('Users insertion completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Вставка категорий в базу данных
execute_values(cur, "INSERT INTO categories (id, name, parent_id) VALUES %s", categories_array)

colonter.logger_alpha_x('Categories insertion completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Вставка сообщений в базу данных (Часть 1)
execute_values(cur, "INSERT INTO messages (id, text, category_id, posted_at, author_id) VALUES %s", messages_array)

colonter.logger_alpha_x('First half messages insertion completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Удаление первого списка сообщений
del (messages_array)

colonter.logger_alpha_x('List "messages_array" deleting completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Заполнение второго списка сообщений
for i in range(colonter.HALF_MESSAGES):
    messages_array2[i][0] = message_uuid[i + colonter.HALF_MESSAGES]
    messages_array2[i][1] = 'Text_' + str(i + colonter.HALF_MESSAGES + 1)
    messages_array2[i][2] = random.choice(category_uuid)
    messages_array2[i][3] = colonter.T_START + (t_end - colonter.T_START) * random.random()
    messages_array2[i][4] = random.choice(author_uuid)

colonter.logger_alpha_x('List "messages_array2" filling completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Вставка сообщений в базу данных (Часть 2)
execute_values(cur, "INSERT INTO messages (id, text, category_id, posted_at, author_id) VALUES %s", messages_array2)

colonter.logger_alpha_x('Second half messages insertion completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

time_checkpoint = time.time()

# Окончание работы с базой данных
con.commit()
cur.close()
con.close()

colonter.logger_alpha_x('Closing completed in', get_checkpoint_time(time_checkpoint))

# ----------------------------------------------------------------------------------------------------------------------

print()
print("Mission completed in " + get_checkpoint_time(start_time) + "! Congratulations!")
print("Powered by: Python 3.7, PyCharm, psycopg2")
