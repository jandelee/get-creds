"""Helper functions and classes for Chargeback Financial Model (cfm).


"""

import os
import json
from flask import Flask, render_template

app = Flask(__name__)
port = os.getenv("PORT", "5000")



def get_creds():
    env_vars = os.environ['VCAP_SERVICES']
    env_vars_json = json.loads(env_vars)
    first_service_type = next(iter(env_vars_json.values()))
    print(first_service_type)
    return first_service_type[0]['credentials']
    # for svc_name in env_vars_json:
        # print(svc_name, env_vars_json[svc_name][0], env_vars_json[svc_name][0]['credentials'])
        # for cred in env_vars_json[svc_name][0]['credentials']:
        #     print(cred, env_vars_json[svc_name][0]['credentials'][cred])

@app.route("/")
def index():
    return "hello world"

@app.route("/showme")
def showme():
    creds = get_creds()
    return render_template('showme.html', creds=creds)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)