import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Подключение к MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['sampleDB']  # Название вашей базы данных
collection = db['sample_collection']  # Название вашей коллекции

def aggregate_payments(dt_from, dt_upto, group_type):
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)
    
    # Запрос к MongoDB для извлечения данных за заданный период
    query = {'dt': {'$gte': dt_from, '$lte': dt_upto}}
    cursor = collection.find(query)
    
    # Создание DataFrame из результатов запроса
    data = pd.DataFrame(list(cursor))
    
    # Проверка загруженных данных
    if data.empty:
        return {'dataset': [], 'labels': []}
    
    # Преобразование данных в формат datetime и установка нужной группировки
    data['dt'] = pd.to_datetime(data['dt'])

    if group_type == 'day':
        data['dt'] = data['dt'].dt.date
    elif group_type == 'month':
        data['dt'] = data['dt'].dt.to_period('M').dt.to_timestamp()
    elif group_type == 'hour':
        data['dt'] = data['dt'].dt.floor('H')

    # Группировка данных по выбранному типу и агрегация суммы выплат
    result = data.groupby('dt')['value'].sum().reset_index()
    dataset = result['value'].tolist()
    labels = result['dt'].astype(str).tolist()

    return {'dataset': dataset, 'labels': labels}

# Пример вызова функции
print(aggregate_payments("2022-09-01T00:00:00", "2022-12-31T23:59:00", "month"))
