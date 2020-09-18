"""
Module Name: forms.py
Install Date : 07-AUG-2020 (Initial)
Modified Date : []
Functionality : Handles all the routes
Version Notes:
07-AUG-2020     -   Initial Push to Production
"""
from flask import jsonify, request, send_file, render_template,send_from_directory
from werkzeug.utils import secure_filename
from engine import app
from engine.models import Authorize
from engine.query import Query

@app.route("/", methods=['POST','GET'])
@app.route("/home", methods=['POST','GET'])
def home():
    ''' Default Route : invokes models and forms module '''
    try:
        c = request.headers.get('clientid')
        s = request.headers.get('secret')
        print(f"clientid : {c} | secret {s}")
        print(f"clientid type : {type(c)} | secret {type(s)}")
        print(request.headers.keys)
        o = Authorize()
        access = o.verify(c, s)
        if access:
            # if ((c=='sparkey') and (s=='qpalzmwiskxn')):
            if request.is_json:
                q = Query()
                q.show()
            else:
                return 'Incorrect json format', 400
        else:
            return 'Incorrect credentials', 400
    except Exception as err:
        return str(err)


@app.route("/logs", methods=['GET'])
def logs():
    ''' displays the log file in stdout or in browser body '''
    logpath = '../monitor.txt'
    return send_file(logpath, as_attachment=False)


@app.route("/logsdown", methods=['GET'])
def logdown():
    ''' text file will be downloaded to the client system '''
    logpath = '../monitor.txt'
    return send_file(logpath, as_attachment=True)
