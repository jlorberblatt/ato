

from fastapi import FastAPI
import pymysql 
import json
from kafka import KafkaProducer
from cloudevents.http import CloudEvent

app = FastAPI()

USERNAME="bobby"
PASSWORD="tables"
DATABASE="example_db"

def get_db():
    return pymysql.connect(user=USERNAME,passwd=PASSWORD,db=DATABASE)


@app.get("/query/{query}")
def root(query: str, q: str = None):
    '''
    This is the worst code I can conceive of while still technically functioning.
    '''
    con = pymysql.connect(user=USERNAME,passwd=PASSWORD,db=DATABASE)
    cursor = con.cursor()
    cursor.execute(f"{query}")
    row = cursor.fetchone()
    con.close()
    if row:
        return { "message": row }
    return {"message": "nothing implemented here yet"}



@app.get("/mysql")
async def root():
    con = pymysql.connect(user=USERNAME,passwd=PASSWORD,db=DATABASE)
    cursor = con.cursor()
    cursor.execute("SELECT * FROM knights_examplar LIMIT 1")
    row = cursor.fetchone()
    con.close()
    if row:
        return { "message": row }
    return {"message": "nothing implemented here yet"}


@app.get("/orders/get/{customer}/{id}")
def get_customer_orders(customer: int, id: int):
    with get_db() as db:
        cursor = db.cursor()
        if id:
            query = f"select * from orders where customer_id = ? and order = ?"
            cursor.execute(query,customer,id)
            rows = cursor.fetchall()
            if rows: return { "message": "success", "customer_id": customer, 
                             "order": id, "endpoint": "/orders/get", "data": rows }
        return { "message": "no rows found", "customer_id": customer, 
                "order": id, "endpoint": "/orders/get", "data": rows }


class EventSink:
    bootstrap_servers: list
    source: str
    k_producer: KafkaProducer
    default_topic: str

    def __init__(self, kafka_brokers, event_source, default_topic):
        self.bootstrap_servers = kafka_brokers
        self.source = event_source
        self.k_producer = KafkaProducer(self.bootstrap_servers)
        self.default_topic = default_topic

    def log_event(self, e_type: str, data: dict):
        attr = {
            "type": e_type,
            "source": self.source,
        }
        event = CloudEvent(attr, data)
        self.k_producer.send(self.default_topic, json.dumps(event))
