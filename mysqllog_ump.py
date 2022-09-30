#!/usr/bin/python

import paho.mqtt.client as mqtt
import mysql.connector
from datetime import datetime

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    client.subscribe("/saufy/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    topic=msg.topic
    table="dht_sensor"
    value=str(msg.payload.decode("utf-8"))
    sql = "insert into {}(time,topic,value)values(now(),%s,%s)".format(table)
    val = (topic,value) 
    try: 
        mycursor.execute(sql,val)
        mydb.commit()
    except:
        print("[Insert Error!]")

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="saufy1982",
  database="sensor_data"
) 
mycursor = mydb.cursor()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(username="umprpi",password="umprpi!@#")
client.connect("10.27.29.153",1883)

client.loop_forever()
logf.close()
