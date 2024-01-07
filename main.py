import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
from config import *
from models import * 


#DSN в файле config.py
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)
Session = sessionmaker(bind=engine)
session = Session()

#####################
# запись данных из файла. Создание массива элементов моделей.
# в иделае увести в отдельную функцию.
models = []
with open('tests_data.json') as f:
    data_all = json.load(f)
    print(len(data_all))
    for data in data_all:
        model = data['model']
        pk= data['pk']
        fields= data['fields']
        if model == 'publisher':
            model = Publisher(name=fields['name'])
            models.append(model)
        if model == 'book':
            model = Book(title=fields['title'], id_publisher=fields['id_publisher'])
            models.append(model)
        if model == 'shop':
            model = Shop(name=fields['name'])
            models.append(model)
        if model == 'stock':
            model = Stock(id_shop=fields['id_shop'], id_book=fields['id_book'],count=fields['count'])
            models.append(model)
        if model == 'sale':
            model = Sale(price=fields['price'], date_sale=fields['date_sale'],count=fields['count'], id_stock=fields['id_stock'])
            models.append(model)
#Окончание чтения файла
###################################################
session.add_all(models)
session.commit()  # фиксируем изменения

#Вводим имя автора или его ИД, Получаем имя автора
name_publisher = input('Введите имя издателя или его id: ')
if name_publisher.isdigit:
    q= session.query(Publisher.name).filter(Publisher.id == name_publisher).one()
    name_publisher, = q

q = session.query(Publisher, Book, Sale, Shop).join(Book,Book.id_publisher == Publisher.id).filter(Publisher.name == name_publisher)
q = q.join(Stock, Stock.id_book == Book.id)
q = q.join(Shop, Shop.id == Stock.id_shop)
q = q.join(Sale, Sale.id_stock == Stock.id)
print(q)
for p,b,sa,sh in q.all():
    print(f'{b.title:40}| {sh.name:^10} | {sa.price} | {sa.date_sale}')

session.close()