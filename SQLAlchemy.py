from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy import func
import pandas as pd

#import CSV
df = pd.read_csv('sales.csv')
#add column 'value'
df['value'] = (df['price']*df['quantity']).round(2)
value_by_product = df.groupby('name')['value'].sum()

#create engine
engine = create_engine("sqlite:///sales.db")
#basic classs representing table
Base = declarative_base()
#create session
Session = sessionmaker(bind=engine)
session = Session()

#define class
class Sales(Base):
    __tablename__ = "Sales"
    id = Column(Integer, primary_key=True)
    date = Column(String)
    name = Column(String)
    price = Column(Float)
    quantity = Column(Integer)
    value = Column(Float)

#drop table befor creating a new one
Sales.__table__.drop(engine)
Base.metadata.create_all(engine)

#transfer data from DataFrame to database
for _, row in df.iterrows():
    sales = Sales(date = row['date'], name = row['name'], price = row['price'], quantity = row['quantity'], value = row['value'])
    session.add(sales)
session.commit()

#find top 5 selling products
results = (
    session.query(Sales.name, func.sum(Sales.value).label("total"))
    .group_by(Sales.name)
    .order_by(func.sum(Sales.value).desc())
    .limit(5)
    .all()
)

#write results in TXT file
with open("top5_sales.txt", "w", encoding="utf-8") as f:
    f.write("TOP 5 SALES\n")
    f.write("====================\n")
    for name, total in results:
        f.write(f"{name} - {total}\n")

print('Results saved in: top5_sales.txt')
session.close()

