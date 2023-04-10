import json
import settings

import sqlalchemy
from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Shop, Book, Stock, Sale

DSN = f'postgresql://{settings.db_username}:{settings.db_pwd}@localhost:5432/bookshops_db'
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('fixtures/tests_data.json', 'r') as file:
    for record in json.load(file):
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()


publisher_ = input('Введите название издателя: ')
result = session.query(Publisher).\
    join(Book).\
    join(Stock).\
    join(Shop).\
    join(Sale).\
    filter(Publisher.name == publisher_)


for publ in result.all():
    for book_ in publ.book:
        for stock_ in book_.stock:
            for sale_ in stock_.sale:
                print(f'{book_.title} | {stock_.shop.name} | {sale_.price} | {sale_.date_sale}')

session.close()