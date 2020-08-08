#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------
# builds on hexbug_relay.py
# prepares raw data from robotling; sets "proper" topics
# 
# SQLite database: table: robotling3
# for each package of raw data a new row gets inserted into "robotling3" table 
#
# Usage:
# 1. Start MQTT-Broker
# 2. Run Robotling with Telemetry enabled
# 3. Run skript
# ---------------------------------------------------------------------
import json
import threading
import time
import paho.mqtt.client as mqtt
import sqlite3


MQTT_BROKER     = "192.168.0.6"
MQTT_PORT       = 1883
MQTT_ALIVE_S    = 60

STATUS_TOPICS=(
              "robotling_30aea42664a8/power/motor_load",
              "robotling_30aea42664a8/power/battery_V",
              "robotling_30aea42664a8/sensor/compass/pitch_deg",
              "robotling_30aea42664a8/sensor/compass/heading_deg",
              "robotling_30aea42664a8/sensor/compass/roll_deg",
              "robotling_30aea42664a8/sensor/distance_cm",
              "robotling_30aea42664a8/state",
              "robotling_30aea42664a8/timestamp_s"
              )
# ---------------------------------------------------------------------
def onConnect(client, userdata, flags, rc):
  global isConnected
  if rc == 0:
    print("Successfully connected to `{0}`".format(MQTT_BROKER))
    print("Subscribing to `{0}` ...".format(MQTT_ROOT_TOPIC))
    client.subscribe(MQTT_ROOT_TOPIC +"/raw")
    isConnected = True
  else:
    print("Broker `{0}` replied `{1}`".format(MQTT_BROKER, rc))

def onDisconnect(client, userdata, rc):
  global isConnected
  print("Disconnected from broker `{0}`".format(MQTT_BROKER))
  isConnected = False

def onMessage(client, userdata, msg):
  global lastMsg, isNewMsg
  try:
    Lock.acquire()
    lastMsg = msg.payload.decode('utf-8')
    isNewMsg = True
  finally:
    Lock.release()

# ---------------------------------------------------------------------
def parseCmdLn():
  from argparse import ArgumentParser
  parser = ArgumentParser()
  parser.add_argument('-g', '--guid', type=str, default="")
  return parser.parse_args()

def parseRawMsg(d):
  for k, v in d.items():
    if isinstance(v, dict):
      for p in parseRawMsg(v):
        yield [k] + p
    else:
      yield [k, v]

# ---------------------------------------------------------------------
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def create_status(conn, status):
    """
    Create a new status into the robotling3 
    A status includes all values send by robotling.
    timestamp: tuple like (battery_V value,...)
    :param conn:
    :param status: tuple
    """
    sql = ''' INSERT INTO rob(motor_load_1,motor_load_2,battery_V,pitch_deg,heading_deg,roll_deg,distance_cm_1,distance_cm_2,distance_cm_3,state,timestamp_s) VALUES(?,?,?,?,?,?,?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, status)
    conn.commit()
# ---------------------------------------------------------------------

if __name__ == '__main__':

  # Initialize variables
  Lock = threading.Lock()
  isConnected = False
  isNewMsg = False
  lastMsg = ""

  # Check for command line parameter(s)
  args = parseCmdLn()
  MQTT_ROOT_TOPIC =  args.guid
  if len(MQTT_ROOT_TOPIC) == 0:
    print("No robotling GUID given (parameter --guid or -g)")

  # Create MQTT client
  Client = mqtt.Client()
  Client.on_connect = onConnect
  Client.on_message = onMessage
  Client.on_disconnect = onDisconnect

  # Connect to database
  conn=create_connection("D:\\Robotling-Dashboard\\testDatabase.db")

  try:
    while True:
      if not isConnected:
        # If not connected, try to connect to broker and start the client's
        # internal loop thread ...
        try:
          print("Trying to connect to `{0}` ...".format(MQTT_BROKER))
          Client.connect(MQTT_BROKER, port=MQTT_PORT, keepalive=MQTT_ALIVE_S)
          Client.loop_start()
          isConnected = True
        except ConnectionRefusedError:
          time.sleep(0.5)
      else:
        # Is connected to broker ...
        data = None
        if isNewMsg and len(lastMsg) > 0:
          try:
            Lock.acquire()
            try:
              # Load data from last message and decode it
              # (convert it into a dictionary)
              data = json.loads(str(lastMsg))
            except:
              print("Corrupt message")
          finally:
            Lock.release()
            isNewMsg = False

        if data:
          # New valid data available
          status=[] # new status values to insert into database
          for ln in parseRawMsg(data):
            topic = MQTT_ROOT_TOPIC +"/"
            msg = str(ln.pop())
            for j, s in enumerate(ln):
              if j > 0 and len(ln) > 1:
                topic += "/"
              topic += s
            if topic in STATUS_TOPICS:
              # just specific topics with status values
              # some need further spliting
              if topic=="robotling_30aea42664a8/power/motor_load":
                # motorload is list of both motorloas
                msg=eval(msg)
                status+=[msg[0]] # load of motor 1
                status+=[msg[1]] # load of motor 2
              elif topic=="robotling_30aea42664a8/sensor/distance_cm":
                # distance_cm is list of three values
                msg=eval(msg)
                status+=[msg[0]]
                status+=[msg[1]]
                status+=[msg[2]]
              else:
                # msg does not need to be splited further
                status+=[msg] 
              
          create_status(conn,status)

      # Sleep for a bit
      time.sleep(0.05)

  except KeyboardInterrupt:
    print("User aborted loop")

  # Stop MQTT client and disconnect
  print("Stop MQTT client loop and disconnect ...")
  Client.loop_stop()
  Client.disconnect()
  print("... done.")

# ---------------------------------------------------------------------
