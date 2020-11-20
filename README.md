---
page_type: assignment from Unity 
languages:
- python
products:
- RestAPI with Python operates on Cassandra from Azure
description: "Design and implement a player session service using Python and Cassandra"
---

# player session service using Python + Cassandra API on Azure
design and implement a player session service which consumes events and
provides metrics about players sessions. Each user will generate two events, one start event when the
session starts and one end event when session is finished. When both events have been received the
session is considered complete. Service is expected to handle massive amount of sessions.


## requirements

* Use Python and Cassandra
* All endpoints are REST APIs
⋅⋅⋅tested with postman for GET and POST⋅⋅

* API for receiving event batches (1-10 events / batch)
⋅⋅⋅POST will take 1-many events as input in json format, and return 200OK and string "POST success"⋅⋅

* API for fetching last 20 complete sessions for a given player
⋅⋅⋅GET will take player_id as input in json format, and return 20 complete sessions⋅⋅

* Data older than 1 year should be discarded
⋅⋅⋅INSERT using TTL of 865400 seconds, so data with 1 years old is automatically cleaned up.⋅⋅

## Running this sample
* will keep active Azure Cassandra API account for 10 days, delete after
	* [Python 3.8]
	* [Git](http://git-scm.com/).
    * [Python Driver](https://github.com/datastax/python-driver)

1. Clone this repository using `git clone https://github.com/ENQSUWY/cassandraRESTapi.git playersessionservice`.

2. Change directories to the repo using `cd playersessionservice`

3. Next, substitute the contactPoint, username, password  in `config.py` with my Azure account's values from connectionstring panel of the portal.

	```
    'username': 'cassandrarestapi',
    'password': '************************************************************',
    'contactPoint': 'cassandrarestapi.cassandra.cosmos.azure.com',
    'port':'10350'
	```
4. Run 
   ```
   pip install Cassandra-driver 
   pip install flask
   pip install requests
   pip install pyopenssl
   ```
   in a terminal to install required python packages
   
5. Run `python createDB.py` in a terminal to create CassandraDB and provision all player events

6. Run `python restAPI.py` in a terminal to start Flask to take GET and POST

## About the code
only GET and POST are implemented for now

* GET json body format:
```json
{
    "player_id":"d6313e1fb7d247a6a034e2aadc30ab3f"
}
```

* POST json body format:
```json
{
    "rows":[
            {"player_id": "d6313e1fb7d247a6a034e2aadc30ab10", "country": "SK", "event": "start", "session_id": "3346a60a-0989-4041-aacc-cf6ff44bd151", "ts": "2016-12-02T05:36:16"},
            {"player_id": "d6313e1fb7d247a6a034e2aadc30ab10", "event": "end", "session_id": "3346a60a-0989-4041-aacc-cf6ff44bd151", "ts": "2016-12-02T05:46:16"}
           ]
}
```
