import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship


#create_engine -> erzeugt im Hintergrund connect oder exectue zur DB
#echo = True -> With it enabled, we’ll see all the generated SQL produced. 
engine = sqlalchemy.create_engine("mysql+pymysql://root:root@localhost:3306/fhwn_orm", echo=False) 

Base = declarative_base() #"Basis Klasse" für die Tabellen aus DB und erstellten Klassen

fk_product = sqlalchemy.Table('fk_relationship_product', Base.metadata,
    sqlalchemy.Column('p_key', sqlalchemy.VARCHAR(6), sqlalchemy.ForeignKey('product.P_key'),primary_key=True),
    sqlalchemy.Column('product_key', sqlalchemy.VARCHAR(6), sqlalchemy.ForeignKey('revenue.Product'),primary_key=True))
               
fk_customer = sqlalchemy.Table('fk_relationship_customer', Base.metadata,
    sqlalchemy.Column('c_key', sqlalchemy.VARCHAR(6), sqlalchemy.ForeignKey('customer.C_key'),primary_key=True),
    sqlalchemy.Column('customer_key', sqlalchemy.VARCHAR(6), sqlalchemy.ForeignKey('revenue.Customer'),primary_key=True))         


class Product(Base): #Bezeichnung der Klasse
    __tablename__ = 'product' #Name der Tabelle aus der Datenbank
    p_key = sqlalchemy.Column("P_key", sqlalchemy.VARCHAR(6), primary_key=True)
    p_name = sqlalchemy.Column("P_product_desc", sqlalchemy.String)
    p_created = sqlalchemy.Column("P_created", sqlalchemy.String)
    
    #Building a Relationship to Revenue
    revenues_p = relationship('Revenue', secondary=fk_product, back_populates='products_r')

    def __init__(self, p_key, p_name, p_created):
        self.p_key = p_key
        self.p_name = p_name   
        self.p_created = p_created
    
    def __repr__(self):
        return "<Product(p_key='%s', p_name='%s', p_created '%s')>" % (
                self.p_key, 
                self.p_name,
                self.p_created)

class Customer(Base): #Bezeichnung der Klasse
    __tablename__ = 'customer' #Name der Tabelle aus der Datenbank
    c_key = sqlalchemy.Column("C_key", sqlalchemy.VARCHAR, primary_key=True)
    c_name = sqlalchemy.Column("C_customer_desc",sqlalchemy.String)
    c_status = sqlalchemy.Column("C_status",sqlalchemy.String) 
     
    #Building a Relationship to Revenue
    revenues_c = relationship('Revenue', secondary=fk_customer, back_populates='customers_r')

    def __init__(self, c_key, c_name, c_status):
        self.c_key = c_key
        self.c_name = c_name
        self.c_status = c_status
        
    def __repr__(self):
        return "<Customer(c_key='%s', c_name='%s',c_status='%s' )>" % (
                self.c_key, 
                self.c_name,
                self.c_status)
    
class Revenue(Base): #Bezeichnung der Klasse
    __tablename__ = 'revenue' #Name der Tabelle aus der Datenbank
    id = sqlalchemy.Column("ID", sqlalchemy.Integer, primary_key = True)
    date = sqlalchemy.Column("Date", sqlalchemy.Integer)
    region = sqlalchemy.Column("Region", sqlalchemy.String)
    product_key = sqlalchemy.Column("Product", sqlalchemy.VARCHAR(6), sqlalchemy.ForeignKey(Product.p_key)) # sqlalchemy.ForeignKey('product.P_key'))
    customer_key = sqlalchemy.Column("Customer",sqlalchemy.VARCHAR,sqlalchemy.ForeignKey(Customer.c_key))
    price = sqlalchemy.Column("Price", sqlalchemy.Float)
    quantity = sqlalchemy.Column("Quantity",sqlalchemy.Integer)
    revenue = sqlalchemy.Column("Revenue", sqlalchemy.Float)  
    
    #Building a Relationship
    products_r = relationship('Product', secondary=fk_product, back_populates='revenues_p')
    customers_r = relationship('Customer', secondary=fk_customer, back_populates='revenues_c')

    def __init__(self, id, date, region, product_key, customer_key, price, quantity, revenue):
        self.id = id
        self.date = date  
        self.region = region  
        self.product_key = product_key  
        self.customer_key = customer_key  
        self.price = price  
        self.quantity = quantity  
        self.revenue = revenue  
        
    def __repr__(self):
        return "<Product(id='%s', date='%s', region='%s', product_key='%s', customer_key='%s', price='%s', quantity='%s',revenue='%s')>" % (
                self.id, 
                self.date, 
                self.region, 
                self.product_key, 
                self.customer_key, 
                self.price, 
                self.quantity, 
                self.revenue)
                                
  
Session = sessionmaker(bind=engine) #Konfiguration der Verbindung zu MySQL wird erstellt, siehe auch Codezeile 9 
Session.configure(bind=engine)  #fügt zusätzlich eine maßgeschneiderte Konfiguration für Verbindung zu MySQL hinzu
session = Session()

#Erstellt die Abfrage durch Joins mit den einzelnen Relationen mit inhaltlicher Filterung und gibt den gewollten spezifischen Output wieder
query_result= session.query(Revenue, Customer, Product).join(Customer).join(Product).filter(Customer.c_status=='VIP').filter(Product.p_created=="USA").order_by(Customer.c_name).all()
print("Summe of all products 'Made in USA' distributed to VIP-Costomers ")
for item in query_result:
    print("<Date='%s', Customer='%s', Product='%s', Revenue='%s')>"%(item.Revenue.date, item.Customer.c_name, item.Product.p_name, item.Revenue.revenue))

#Wiedergabe aller Element in Customer
#for customer in session.query(Customer):
#    print(customer)

# Create a new DB-entry
new_revenue_entry = Revenue(None, 20170406,'REG01','P00003', 'CU001', 68.88, 1, 68.88 )
session.add(new_revenue_entry)

#SUM-Function
#q_1= session.query(sqlalchemy.func.sum(Revenue.revenue)).group_by(Revenue.product_key).all()
#for item in q_1:
#   print(item)
   
session.commit() #Bestätigt alles Änderungen
session.close() #Beendet die Session und zieht somit den Schlussstrich von der Abfrage 