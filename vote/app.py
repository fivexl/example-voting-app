from flask import Flask, render_template, request, make_response, g
from redis import Redis
import os
import socket
import random
import json
import logging

option_a = os.getenv('OPTION_A', "Cats")
option_b = os.getenv('OPTION_B', "Dogs")
hostname = socket.gethostname()

app = Flask(__name__)

gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.INFO)

def get_redis():
    if not hasattr(g, 'redis'):
        redis_host = os.getenv('REDIS_HOST', 'redis')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        redis_password = os.getenv('REDIS_PASSWORD', None)
        redis_db = int(os.getenv('REDIS_DB', 0))
        socket_timeout = int(os.getenv('REDIS_SOCKET_TIMEOUT', 5))

        if redis_password:
            g.redis = Redis(host=redis_host, port=redis_port, password=redis_password, db=redis_db, socket_timeout=socket_timeout)
        else:
            g.redis = Redis(host=redis_host, port=redis_port, db=redis_db, socket_timeout=socket_timeout)

    return g.redis

@app.route("/", methods=['POST','GET'])
def hello():
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    vote = None

    if request.method == 'POST':
        redis = get_redis()
        vote = request.form['vote']
        app.logger.info('Received vote for %s', vote)
        data = json.dumps({'voter_id': voter_id, 'vote': vote})
        redis.rpush('votes', data)

    resp = make_response(render_template(
        'index.html',
        option_a=option_a,
        option_b=option_b,
        hostname=hostname,
        vote=vote,
    ))
    resp.set_cookie('voter_id', voter_id)
    return resp

@app.route("/health", methods=["GET"])
def health_check():
    try:
        redis = get_redis()
        redis.ping()
        return "OK", 200
    except Exception as e:
        app.logger.error("Health check failed: %s", str(e))
        return "Unhealthy", 500


if __name__ == "__main__":
    app.run(
        host='0.0.0.0',
        port=os.getenv('PORT', 8080),
        debug=True,
        threaded=True
    )
