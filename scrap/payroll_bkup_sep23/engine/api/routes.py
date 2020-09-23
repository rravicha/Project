from flask import Blueprint

site_app=Blueprint('api',__name__)

@site_app.route('/')
def api_main():
    return "Successfully invoked api "