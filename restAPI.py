from flask import Flask, request
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import dict_factory
import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE
from requests.utils import DEFAULT_CA_BUNDLE_PATH
from flask import jsonify

#<authenticateAndConnect>
ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.verify_mode = CERT_NONE
auth_provider = PlainTextAuthProvider(
    username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port=cfg.config['port'],
                  auth_provider=auth_provider, ssl_context=ssl_context)
session = cluster.connect()
#</authenticateAndConnect>

app = Flask(__name__)

@app.route('/', methods=['GET'])
def getById():
    user = request.json
    print(
        f"\nSelecting 20 complete sessions for player_id={user['player_id']}")
    session.row_factory = dict_factory
    start = session.execute(
        'SELECT * FROM uprofile.startevents WHERE player_id= %s LIMIT 500', [str(user['player_id'])])
    # PrintTable(start)
    end = session.execute(
        'SELECT * FROM uprofile.endevents WHERE player_id= %s LIMIT 500', [str(user['player_id'])])
    allstart = start.all()
    allend = end.all()
    result = []
    for startevent in allstart:
        #     print(startevent)
        for endevent in allend:
            #         print(f"s={startevent['session_id']}, e={endevent['session_id']}")
            if startevent['session_id'] == endevent['session_id']:
                startevent.update(endevent)
                result.append(startevent)
    return jsonify(result[:20])


@app.route('/', methods=['POST'])
def post():
    rows = request.json
    for row in rows['rows']:
        # print(f"current rows={row}")
        if row['event'] == "start":
            session.execute(
                "INSERT INTO  uprofile.startevents  (player_id,  event, country, session_id, ts) VALUES (%s,%s,%s,%s,%s) USING TTL 865400",
                            [row['player_id'], row['event'], row['country'], row['session_id'], row['ts']]
                            )
        if row['event'] == "end":
            session.execute(
                "INSERT INTO  uprofile.endevents  (player_id,  event, session_id, ts) VALUES (%s,%s,%s,%s) USING TTL 865400",
                            [row['player_id'], row['event'], row['session_id'], row['ts']]
                            )
        return "POST success"

if __name__ == '__main__':
    app.run(debug=True)
