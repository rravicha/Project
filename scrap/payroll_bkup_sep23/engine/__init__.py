from flask import Flask
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

from engine import routes
from engine.api import routes
from engine.site import routes

# app.register_blueprint
