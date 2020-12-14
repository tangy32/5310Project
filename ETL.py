
import csv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, ForeignKey, func, DateTime, Date, Text, null
from sqlalchemy.orm import relationship,sessionmaker
from time import time
from datetime import datetime



# Define the connection string
conn_str = 'postgresql://postgres:123@localhost/ecommerce'

# Create an engine
engine = create_engine(conn_str)
Base = declarative_base()

class Product(Base):
    __tablename__ = 'product'

    product_id = Column(Integer,primary_key=True)
    product_serial_number = Column(String(100), nullable = False)
    product_name_length = Column(Integer)
    product_description_lenght = Column(Integer)
    product_photos_qty = Column(Integer)
    product_weight_g = Column(Integer)
    product_detail_product = relationship('Product_detail', backref = 'product', lazy='dynamic')


class Category(Base):
    __tablename__ = 'category'

    category_id = Column(Integer,primary_key=True)
    product_category_name = Column(String(100), nullable = False)
    product_detail_categoty = relationship('Product_detail', backref = 'category', lazy='dynamic')


class Size(Base) :
    __tablename__ = 'size'

    size_id = Column(Integer,primary_key=True)
    product_length_cm = Column(Integer)
    product_height_cm = Column(Integer)
    product_width_cm = Column(Integer)
    product_detail_size = relationship('Product_detail', backref = 'size', lazy='dynamic')


class Product_detail(Base) :
    __tablename__ = 'product_detail'

    pd_id = Column(Integer,primary_key = True)
    product_id = Column(Integer, ForeignKey('product.product_id'),unique = True)
    category_id = Column(Integer,ForeignKey('category.category_id'), nullable = False)
    size_id = Column(Integer,ForeignKey('size.size_id'), nullable = False)
    product_merge = relationship('Merge', backref = 'product_detail', lazy='dynamic')

class Orders(Base) :
    __tablename__ = 'orders'

    order_id = Column(Integer, primary_key = True)
    order_serial_number = Column(String(100), nullable = False)
    order_status = Column(String(100))
    order_purchase_timestamp = Column(DateTime(timezone=True), server_default=func.now())
    order_delivery_relationship = relationship('Delivery', backref = 'orders', lazy='dynamic')
    
class Delivery(Base):
    __tablename__ = 'delivery'

    delivery_id = Column(Integer, primary_key = True)
    order_id = Column(Integer, ForeignKey('orders.order_id'),unique = True)
    delivered_carrier_date = Column(DateTime(timezone=True), server_default=func.now())
    order_delivered_customer_date = Column(DateTime(timezone=True), server_default=func.now())
    estimated_delivery_date = Column(DateTime(timezone=True), server_default=func.now())


class Seller(Base):
    __tablename__ = 'seller'

    seller_id = Column(Integer, primary_key = True)
    seller_serial_number = Column(String(100), nullable = False)
    seller_location_relationship = relationship('Seller_Location', backref = 'seller', lazy='dynamic')
    seller_merge = relationship('Merge', backref = 'seller', lazy='dynamic')

class Customer(Base):
    __tablename__ = 'customer'

    customer_id = Column(Integer, primary_key = True)
    customer_serial_number =  Column(String(100), nullable = False)
    customer_unique_number = Column(String(100), nullable = False)
    customer_location_relationship = relationship('Customer_Location', backref = 'customer', lazy='dynamic')
    customer_merge = relationship('Merge', backref = 'customer', lazy='dynamic')

class Location(Base):
    __tablename__ = 'location'

    zipcode =  Column(Integer, primary_key = True)
    city =  Column(String(100))
    state =  Column(String(100))
    customer_location = relationship('Customer_Location', backref = 'location', lazy='dynamic')
    seller_location = relationship('Seller_Location', backref = 'location', lazy='dynamic')

class Seller_Location(Base) :
    __tablename__ = 'seller_location'

    sl_id = Column(Integer, primary_key = True)
    seller_id = Column(Integer, ForeignKey('seller.seller_id'),unique = True)
    location_id = Column(Integer, ForeignKey('location.zipcode'), nullable = False)
 

class Customer_Location(Base) :
    __tablename__ = 'customer_location'

    cl_id = Column(Integer, primary_key = True)
    customer_id = Column(Integer, ForeignKey('customer.customer_id'))
    location_id = Column(Integer, ForeignKey('location.zipcode'), nullable = False)

class Review(Base) :

    __tablename__ = 'review'

    review_id = Column(Integer, primary_key = True)
    review_serial_number =  Column(String(100), nullable = False)
    review_score = Column(Numeric(8,2))
    review_creation_date = Column(Date())
    review_answer_timestamp = Column(DateTime(timezone=True), server_default=func.now())
    review_feedback = relationship('Feedback', backref = 'review', lazy='dynamic')

class Comment(Base) : 
    __tablename__ = 'comment'

    comment_id = Column(Integer, primary_key = True)
    review_comment_title = Column(String(255), nullable = False)
    review_comment_message = Column(Text)
    comment_feedback = relationship('Feedback', backref = 'comment', lazy='dynamic')

class Feedback(Base) :
    __tablename__ = 'feedback'

    feedback_id = Column(Integer, primary_key = True)
    review_id = Column(Integer, ForeignKey('review.review_id'),unique = True)
    comment_id = Column(Integer, ForeignKey('comment.comment_id'))
    feedback_merge = relationship('Merge', backref = 'feedback', lazy='dynamic')

    
class Payment(Base) :
    __tablename__ = 'payment'

    payment_id = Column(Integer, primary_key = True)
    payment_sequential = Column(Integer)
    payment_type = Column(String(255))
    payment_installments = Column(String(255))
    payment_merge = relationship('Merge', backref = 'payment', lazy='dynamic')


class Price(Base) :
    __tablename__ = 'price'

    price_id = Column(Integer, primary_key = True)
    price = Column(Numeric(10,2))
    freight_value = Column(Numeric(10,2))
    order_item_number = Column(Integer)
    payment_value = Column(Numeric(10,2))
    price_merge = relationship('Merge', backref = 'price', lazy='dynamic')

class Merge(Base) :
    __tablename__ = 'merge'

    merge_id = Column(Integer, primary_key = True)
    order_id = Column(Integer, ForeignKey('orders.order_id'),unique = True)
    pd_id = Column(Integer, ForeignKey('product_detail.pd_id'))
    seller_id = Column(Integer, ForeignKey('seller.seller_id'))
    customer_id = Column(Integer, ForeignKey('customer.customer_id'))
    feedback_id = Column(Integer, ForeignKey('feedback.feedback_id'))
    price_id = Column(Integer, ForeignKey('price.price_id'))
    payment_id = Column(Integer, ForeignKey('payment.payment_id'))


# This method create tables and open the file Orders_merged and load into data
def create_table_and_open_file(): 
    # Create tables on DB server
    Base.metadata.create_all(engine)

    file_name = "Orders_merged.csv"

    with open(file_name, newline='') as f:
        reader = csv.reader(f)
        data = list(reader)

    return data

# This method print the column number and name to better understand the csv file
def print_column_number_and_name():
    print(data[0])
    print(data[1])
    # product table
    # record[31] is product_name_length
    print("31: " + data[0][31])
    # record[32] is product_description length
    print("32: " + data[0][32])
    # record[33] is product_photos_qty
    print("33: " + data[0][33])
    print("34: " + data[0][34])

    # category table
   
    # record[30] is product_category_name
    print("30: " + data[0][30])

    # size table
    print("35: " + data[0][35])
    print("36: " + data[0][36])
    print("37: " + data[0][37])

    # order table 2 4 5
    print("2: " + data[0][2])
    print("4: " + data[0][4])
    print("5: " + data[0][5])

    # delivery table 7 8 9
    print("7: " + data[0][7])
    print("8: " + data[0][8])
    print("9: " + data[0][9])

    # customer table 3 10
    print("3: " + data[0][3])
    print("10: " + data[0][10])

    #  seller table
    print("3: " + data[0][1])

    # location table
    print("11: " + data[0][11])
    print("12: " + data[0][12])
    print("13: " + data[0][12])

    print("27: " + data[0][27])
    print("28: " + data[0][28])
    print("29: " + data[0][29])

    # location table
    print("14: " + data[0][14])
    print("15: " + data[0][15])
    print("16: " + data[0][16])
    print("17: " + data[0][17])
    print("18: " + data[0][18])
    print("19: " + data[0][19])

    # payment table
    print("20: " + data[0][20])
    print("21: " + data[0][21])
    print("22: " + data[0][22])

    # payment table
    print("23: " + data[0][23])
    print("24: " + data[0][24])
    print("25: " + data[0][25])
    print("26: " + data[0][26])


# Check null value
def check_null(n):
    if n == 'NA':
        return None
    return n

# Check null value for timestamp
# If null, return None. Otherwise, parse and return the proper timestamp
def check_null_timestamp(n):
    if n == 'NA' or n == '':
        return None
    return datetime.strptime(n, '%Y-%m-%d %H:%M:%S')

    


if __name__ == "__main__":

    # Insert data  
    Session = sessionmaker(bind=engine)
    session = Session()

    data = create_table_and_open_file()

    print_column_number_and_name()
    
    # table 1
    product_id = 0
    added_product_dict = dict()
    # table 2
    category_id = 0
    added_category_dict = dict()
    # table 3
    size_id = 0
    added_size_dict = dict()
    # table 5
    order_id = 0
    # table 8
    seller_id = 0
    added_seller_dict = dict()
    # table 9
    customer_id = 0
    # table 10
    added_location = set()
    # table 13
    review_id = 0
    added_review_dict = dict()
    # table 14
    comment_id = 0
    added_comment_dict = dict()
    # table 16
    payment_id = 0
    added_payment_dict = dict()
    # table 17
    price_id = 0
    added_price_dict = dict()
    # table 18
    merge_id = 0

    interrecords = iter(data)

    next(interrecords)

    for record in interrecords:
        # Add data to product table
        if record[0] in added_product_dict:
            pid = added_product_dict[record[0]]
        else:
            added_product_dict[record[0]] = product_id
            pid = product_id
            product_id = product_id + 1
            new_product = Product(**{
                'product_id' : pid,
                'product_serial_number' : record[0],
                'product_name_length' : check_null(record[31]),
                'product_description_lenght' : check_null(record[32]),
                'product_photos_qty' : check_null(record[33]),
                'product_weight_g' : check_null(record[34])
            })
            
            session.add(new_product)
        
            # Add data to category table if the category is not equal to null
            pc_name = check_null(record[30])
            cid = None
            if pc_name != None:   
                # pc_name_translated = translated_category[pc_name] 
                if pc_name in added_category_dict :
                    cid = added_category_dict[pc_name]
                else:
                    added_category_dict[pc_name] = category_id
                    cid = category_id
                    category_id = category_id + 1
                    new_category = Category(**{
                        'category_id' : cid,
                        'product_category_name' : pc_name
                    })
                session.add(new_category)
        
            # add data to the size table
            size_combination = record[35] + " " + record[36] + " " + record[37]
            if size_combination in added_size_dict:
                sid = added_size_dict[size_combination]
            else:
                added_size_dict[size_combination] = size_id
                sid = size_id
                size_id = size_id + 1
                new_size = Size(**{
                    'size_id' : sid,
                    'product_length_cm' : check_null(record[35]),
                    'product_height_cm' : check_null(record[36]),
                    'product_width_cm' : check_null(record[37])
                })
                session.add(new_size)

            new_pd = Product_detail(**{
                'pd_id': pid,
                'product_id' : pid,
                'category_id' : cid,
                'size_id' : sid
            })
            session.add(new_pd)
        
        # add data to order table
        oid = order_id
        order_id = order_id + 1
        new_order = Orders(**{
            'order_id' : oid,
            'order_serial_number' : record[2],
            'order_status' : record[4],
            'order_purchase_timestamp' : datetime.strptime(record[5], '%Y-%m-%d %H:%M:%S')
        })
        session.add(new_order)

        # add data to delivery table
        new_delivery = Delivery(**{
            'delivery_id' : oid,
            'delivered_carrier_date' : check_null_timestamp(record[7]),
            'order_delivered_customer_date' : check_null_timestamp(record[8]),
            'estimated_delivery_date' : check_null_timestamp(record[9])
        })
        session.add(new_delivery)


        # add data to customer table
        cust_id = customer_id
        customer_id = customer_id + 1
        new_customer = Customer(**{
            'customer_id' : cust_id,
            'customer_serial_number' : record[3],
            'customer_unique_number' : record[10]
        })
        session.add(new_customer)

        # add data to location table
        if record[11] not in added_location:
            new_location1 = Location(**{
                'zipcode' : record[11],
                'city': record[12],
                'state': record[13]
            })
            added_location.add(record[11])
            session.add(new_location1)
        
        new_cl = Customer_Location(**{
            'cl_id': cust_id,
            'customer_id': cust_id,
            'location_id': record[11]
        })
        session.add(new_cl)
        
        if record[27] not in added_location:
            new_location2 = Location(**{
                'zipcode' : record[27],
                'city': record[28],
                'state': record[29]
            })
            added_location.add(record[27])
            session.add(new_location2)

        # add data to seller table and Seller Location table
        if record[1] in added_seller_dict:
            slid = added_seller_dict[record[1]]
        else:
            added_seller_dict[record[1]] = seller_id
            slid = seller_id
            seller_id = seller_id + 1
            new_seller = Seller(**{
                'seller_id' : slid,
                'seller_serial_number': record[1]
            })
            session.add(new_seller)
            new_sl = Seller_Location(**{
                'sl_id': slid,
                'seller_id': slid,
                'location_id': record[27]
            })
            session.add(new_sl)
        


        # add data to comment table and feedback table
        comment_str = record[16] + " " + record[17]
        if comment_str in added_comment_dict:
            coid = added_comment_dict[comment_str]
        else:
            added_comment_dict[comment_str] = comment_id
            coid = comment_id
            comment_id = comment_id + 1
            new_comment = Comment(**{
                'comment_id': coid,
                'review_comment_title': check_null(record[16]),
                'review_comment_message' : check_null(record[17])
            })
            session.add(new_comment)




        # added data to review table
        if record[14] in added_review_dict: 
            rid = added_review_dict[record[14]]
        else:
            rid = review_id
            review_id = review_id + 1
            added_review_dict[record[14]] = rid

            new_review = Review(**{
                'review_id' : rid,
                'review_serial_number' : record[14],
                'review_score' : float(record[15]),
                'review_creation_date': check_null_timestamp(record[18]).date(),
                'review_answer_timestamp': check_null_timestamp(record[19])
            })
            session.add(new_review)

            new_feedback = Feedback(**{
                'feedback_id':rid,
                'review_id': rid,
                'comment_id': coid
            })
            session.add(new_feedback)

        # add data to payment table"
        payment_str = record[20] + " " + record[21] + " " + record[22]
        if payment_str in added_payment_dict:
            paid = added_payment_dict[payment_str]
        else:
            added_payment_dict[payment_str] = payment_id
            paid = payment_id
            payment_id = payment_id + 1
            new_payment = Payment(**{
                'payment_id' : paid,
                'payment_sequential' : check_null(record[20]),
                'payment_type' : check_null(record[21]),
                'payment_installments': check_null(record[22])
            })
            session.add(new_payment)

        # add data to price table
        price_str = record[23] +  " " + record[24] + " " + record[25] + " " + record[26]
        if price_str in added_price_dict:
            prid = added_price_dict[price_str]
        else:
            added_price_dict[price_str] = price_id
            prid = price_id
            price_id = price_id + 1
            new_price = Price(**{
                'price_id': prid,
                'price': float(record[25]),
                'freight_value': float(record[26]),
                'order_item_number': record[24],
                'payment_value': check_null(record[23])
            })
            session.add(new_price)

        # add data to merge table
        new_merge = Merge(**{
            'merge_id': oid,
            'order_id': oid,
            'pd_id':pid,
            'seller_id': slid,
            'customer_id': cust_id,
            'feedback_id': rid,
            'price_id': prid,
            'payment_id': paid,
        })
        session.add(new_merge)
        
        session.commit()

    session.close()
