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

def PrintTable(rows):
    t = PrettyTable(['player_id', 'event', 'country', 'session_id', 'ts'])
    for r in rows:
        t.add_row([r.player_id, r.event, r.country, r.session_id, r.ts])
    print (t)

#<authenticateAndConnect>
ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.verify_mode = CERT_NONE
auth_provider = PlainTextAuthProvider(username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider,ssl_context=ssl_context)
session = cluster.connect()
#</authenticateAndConnect>

#<createKeyspace>
print ("\nCreating Keyspace")
session.execute('CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }')
#</createKeyspace>

#<createTable>
print ("\nCreating Table")
session.execute(
    'CREATE TABLE IF NOT EXISTS uprofile.events_by_user(player_id text, ts timestamp, country text, event text, session_id uuid, PRIMARY KEY((player_id, session_id), ts, event)) WITH CLUSTERING ORDER BY ("ts" DESC, "event" ASC)')
#</createTable>

#<insertData>
session.execute("INSERT INTO  uprofile.events_by_user  (player_id, event, country, session_id, ts) VALUES (%s,%s,%s,%s,%s)", [
                "0a2d12a1a7e145de8bae44c0c6e06629", 'start', 'FI', '4a0c43c9-c43a-42ff-ba55-67563dfa35d4', '2016-12-02T12:48:05.520022'])
session.execute("INSERT INTO  uprofile.events_by_user  (player_id, event, session_id, ts) VALUES (%s,%s,%s,%s)", [
                "0a2d12a1a7e145de8bae44c0c6e06629", 'end', '4a0c43c9-c43a-42ff-ba55-67563dfa35d4', '2016-12-02T12:49:05.520022'])
session.execute("INSERT INTO  uprofile.events_by_user  (player_id, event , country, session_id, ts) VALUES (%s,%s,%s,%s,%s)", [
                "0a2d12a1a7e145de8bae44c0c6e06630", 'start', 'xx', '4a0c43c9-c43a-42ff-ba55-67563dfa35d5', '2016-12-02T12:50:05.520022'])
session.execute("INSERT INTO  uprofile.events_by_user  (player_id, event , country, session_id, ts) VALUES (%s,%s,%s,%s,%s)", [
                "0a2d12a1a7e145de8bae44c0c6e06631", 'start', 'YY', '4a0c43c9-c43a-42ff-ba55-67563dfa35d6', '2016-12-02T12:59:05.520022'])
session.execute("INSERT INTO  uprofile.events_by_user  (player_id, event , country, session_id, ts) VALUES (%s,%s,%s,%s,%s)", [
                "0a2d12a1a7e145de8bae44c0c6e06632", 'start', 'XZ', '4a0c43c9-c43a-42ff-ba55-67563dfa35d7', '2016-12-02T12:56:05.520022'])
session.execute("INSERT INTO  uprofile.events_by_user  (player_id, event , country, session_id, ts) VALUES (%s,%s,%s,%s,%s)", [
                "0a2d12a1a7e145de8bae44c0c6e06633", 'start', 'MM', '4a0c43c9-c43a-42ff-ba55-67563dfa35d8', '2016-12-02T12:55:05.520022'])
session.execute("INSERT INTO  uprofile.events_by_user  (player_id, event , country, session_id, ts) VALUES (%s,%s,%s,%s,%s)", [
                "0a2d12a1a7e145de8bae44c0c6e06634", 'start', 'NN', '4a0c43c9-c43a-42ff-ba55-67563dfa35d9', '2016-12-02T12:54:05.520022'])
#</insertData>

#<queryAllItems>
print ("\nSelecting All")
rows = session.execute('SELECT * FROM uprofile.events_by_user')
PrintTable(rows)
#</queryAllItems>

#<queryByID>
# print ("\nSelecting Id=1")
# rows = session.execute(
#     'SELECT * FROM uprofile.events_by_user where player_id="0a2d12a1a7e145de8bae44c0c6e06629"')
# PrintTable(rows)
#</queryByID>

cluster.shutdown()
