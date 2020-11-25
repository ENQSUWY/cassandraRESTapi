from flask import Flask, request, jsonify
from createDB import connect_DB

app = Flask(__name__)
session = connect_DB()

# API for fetching last 20 complete sessions for a given player
@app.route('/', methods=['GET'])
def getById():
    '''
    getting player_id from GET body json format
    query player_id from table "startevents"
    query player_id from table "endevents"
    output 20 complete sessions of this player_id
    '''
    user = request.json
    print(f"\nSelecting 20 complete sessions for player_id={user['player_id']}")
    #allStarteventofPlayer
    allStarteventofPlayer = session.execute(
        'SELECT * FROM uprofile.startevents WHERE player_id= %s LIMIT 500', [str(user['player_id'])])
    allEndeventofPlayer = session.execute(
        'SELECT * FROM uprofile.endevents WHERE player_id= %s LIMIT 500', [str(user['player_id'])])
    allstart = allStarteventofPlayer.all()
    allend = allEndeventofPlayer.all()
    result = []
    for startevent in allstart:
        for endevent in allend:
            if startevent['session_id'] == endevent['session_id']:
                result.append(startevent)
                result.append(endevent)
    return jsonify(result[:40])


#API for receiving event batches(1-10 events / batch)
@app.route('/', methods=['POST'])
def post():
    '''
    taking all events from POST body json
    insert them into corresponding table "startevents" or "endevents"
    return 200OK
    '''
    rows = request.json
    for row in rows['rows']:
        if row['event'] == "start":
            session.execute(
                # TTL will delete data older than 1year(865400 seconds)
                "INSERT INTO  uprofile.startevents  (player_id,  event, country, session_id, ts) VALUES (%s,%s,%s,%s,%s) USING TTL 865400",
                            [row['player_id'], row['event'], row['country'], row['session_id'], row['ts']]
                            )
        if row['event'] == "end":
            session.execute(
                # TTL will delete data older than 1year(865400 seconds)
                "INSERT INTO  uprofile.endevents  (player_id,  event, session_id, ts) VALUES (%s,%s,%s,%s) USING TTL 865400", 
                            [row['player_id'], row['event'], row['session_id'], row['ts']]
                            )
        return "POST success"

if __name__ == '__main__':
    app.run(debug=True)
