from flask import Flask, request
from flask_restful import Resource, Api
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
import time
import ssl
import cassandra
from cassandra.cluster import Cluster, BatchStatement
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE
from requests.utils import DEFAULT_CA_BUNDLE_PATH
import uuid
from json import dumps
from flask import jsonify
import json


def PrintTable(rows):
    t = PrettyTable(['player_id', 'event', 'country', 'session_id', 'ts'])
    for r in rows:
        t.add_row([r.player_id, r.event, r.country, r.session_id, r.ts])
    print(t)


#<authenticateAndConnect>
ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.verify_mode = CERT_NONE
auth_provider = PlainTextAuthProvider(
    username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port=cfg.config['port'],
                  auth_provider=auth_provider, ssl_context=ssl_context)
session = cluster.connect()
#</authenticateAndConnect>

#<createKeyspace>
print("\nCreating Keyspace")
session.execute(
    'CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }')
#</createKeyspace>

#<createTable for start events>
print("\nCreating Table")
session.execute(
    'CREATE TABLE IF NOT EXISTS uprofile.startevents(player_id text, ts timestamp, country text, event text, session_id text, PRIMARY KEY(player_id, ts)) WITH CLUSTERING ORDER BY ("ts" DESC)')
#</createTable>

#<createTable for end events>
print("\nCreating Table")
session.execute(
    'CREATE TABLE IF NOT EXISTS uprofile.endevents(player_id text, ts timestamp, event text, session_id text, PRIMARY KEY(player_id, ts)) WITH CLUSTERING ORDER BY ("ts" DESC)')
#</createTable>

#push all data from https://cdn.unityads.unity3d.com/assignments/assignment_data.jsonl.bz2 saved to assignment_data.json
with open('assignment_data.json') as f:
    data = f.readlines()

# print(f"data[1]={data[1]}")

for row in data:
    row = json.loads(row)
    # print(f"row['event']={row['event']}")
    if row['event'] == "start":
        session.execute(
            "INSERT INTO  uprofile.startevents  (player_id,  event, country, session_id, ts) VALUES (%s,%s,%s,%s,%s) ",
            [row['player_id'], row['event'], row['country'], row['session_id'], row['ts']]
        )
    if row['event'] == "end":
        session.execute(
            "INSERT INTO  uprofile.endevents  (player_id,  event, session_id, ts) VALUES (%s,%s,%s,%s) ",
            [row['player_id'], row['event'], row['session_id'], row['ts']]
        )
        
print("complete")
