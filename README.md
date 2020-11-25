---
page_type: assignment from Unity 
languages:
- python
- cassandra
---

# player session service using Python + Cassandra API on Azure
design and implement a player session service which consumes events and
provides metrics about players sessions. Each user will generate two events, one start event when the
session starts and one end event when session is finished. When both events have been received the
session is considered complete. Service is expected to handle massive amount of sessions.


## requirements

* Use Python and Cassandra
* All endpoints are REST APIs

   tested with postman for GET and POST  

* API for receiving event batches (1-10 events / batch)

   POST will take 1-many events as input in json format, and return 200OK and string "POST success"  

* API for fetching last 20 complete sessions for a given player

   GET will take player_id as input in json format, and return 20 complete sessions  

* Data older than 1 year should be discarded

   INSERT using TTL of 865400 seconds, so data with 1 years old is automatically cleaned up.  

## Running this sample

1. Run `python create_DB.py` in a terminal to create data model schema in Azure cassandra DB, ONLY ONCE
2. Run `python restAPI.py` in a terminal to start Flask to take GET and POST

## About the code

test the API with below curl code or POSTMAN

* GET json body format:
```json
curl --location --request GET 'http://127.0.0.1:5000/?Content-Type=application/json&Accept=application/json' \
--header 'Content-Type: application/json' \
--data-raw '{
    "player_id":"d6313e1fb7d247a6a034e2aadc30ab3f"
}'
```

* POST json body format:
```json
curl --location --request POST 'http://127.0.0.1:5000/?Content-Type=application/json' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json' \
--data-raw '{
    "rows":[
            {"player_id": "d6313e1fb7d247a6a034e2aadc30ab10", "country": "SK", "event": "start", "session_id": "3346a60a-0989-4041-aacc-cf6ff44bd151", "ts": "2016-12-02T05:36:16"},
            {"player_id": "d6313e1fb7d247a6a034e2aadc30ab22", "country": "UK", "event": "start", "session_id": "3346a60a-1089-4041-aacc-cf6ff44bd177", "ts": "2016-12-02T05:37:16"},
            {"player_id": "d6313e1fb7d247a6a034e2aadc30ab20", "country": "SK", "event": "start", "session_id": "3346a60a-0989-4041-aacc-cf6ff44bd152", "ts": "2016-12-03T05:36:16"},
            {"player_id": "d6313e1fb7d247a6a034e2aadc30ab32", "country": "UK", "event": "start", "session_id": "3346a60a-1089-4041-aacc-cf6ff44bd178", "ts": "2016-12-03T05:37:16"},
            {"player_id": "d6313e1fb7d247a6a034e2aadc30ab40", "country": "SK", "event": "start", "session_id": "3346a60a-0989-4041-aacc-cf6ff44bd153", "ts": "2016-12-04T05:36:16"},
            {"player_id": "d6313e1fb7d247a6a034e2aadc30ab42", "country": "UK", "event": "start", "session_id": "3346a60a-1089-4041-aacc-cf6ff44bd179", "ts": "2016-12-04T05:37:16"}
    ]
}'
```
