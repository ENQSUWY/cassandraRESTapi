import config as cfg
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE
import json

def connect_DB():
    '''
    connect to cassandra DB service in Azure
    '''

    #<authenticateAndConnect>
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.verify_mode = CERT_NONE
    auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
    cluster = Cluster([cfg.config['contactPoint']], port=cfg.config['port'],
                      auth_provider=auth_provider, ssl_context=ssl_context)
    session = cluster.connect()
    #</authenticateAndConnect>
    session.row_factory = dict_factory
    print(f"DB connected")
    return session


def create_DB():
    '''
    connect to cassandra DB service in Azure
    create Data Schema according to requirement
    startevents table with column: player_id, ts, country, event, session_id
    endevents table with column: player_id, ts, event text, session_id
    '''
    # connect to DB
    session = connect_DB()
    #<createKeyspace>
    print("\nCreating Keyspace")
    session.execute(
        'CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }')
    #</createKeyspace>

    #<createTable for start events>
    print("\nCreating startevents Table")
    session.execute(
        'CREATE TABLE IF NOT EXISTS uprofile.startevents(player_id text, ts timestamp, country text, event text, session_id text, PRIMARY KEY(player_id, ts)) WITH CLUSTERING ORDER BY ("ts" DESC)')
    #</createTable>

    #<createTable for end events>
    print("\nCreating endevents Table")
    session.execute(
        'CREATE TABLE IF NOT EXISTS uprofile.endevents(player_id text, ts timestamp, event text, session_id text, PRIMARY KEY(player_id, ts)) WITH CLUSTERING ORDER BY ("ts" DESC)')
    #</createTable>
    
    session.execute(
        'CREATE TABLE IF NOT EXISTS uprofile.events(player_id text, ts timestamp, country text, event text, session_id text, PRIMARY KEY(player_id, ts)) WITH CLUSTERING ORDER BY ("ts" DESC)')
    
    return session


def store_existing_data(session):
    '''
    store all data from assignment(assignment_data.json) to created DB
    startevents table with column: player_id, ts, country, event, session_id
    endevents table with column: player_id, ts, event text, session_id
    '''
    
    with open('assignment_data.json') as f:
        data = f.readlines()

    for row in data:
        row = json.loads(row)
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
    f.close()
    print("data import complete")


if __name__ == '__main__':
    # create_DB()
    session = connect_DB()
    # uncomment if needs to inject data, but this runs slow needs improvement
    # store_existing_data(session)
