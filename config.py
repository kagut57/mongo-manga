import os
import json

env_file = "env.json"

if os.path.exists(env_file):
    with open(env_file) as f:
        env_vars = json.loads(f.read())
else:
    env_vars = dict(os.environ)

mongo_url = env_vars.get('MONGODB_URL') or env_vars.get('DATABASE_URL') or 'mongodb://localhost:27017'
