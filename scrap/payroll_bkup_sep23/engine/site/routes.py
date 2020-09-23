from flask import Blueprint

site_app=Blueprint('site',__name__)

@site_app.route('/')
def site_main():
    return "Successfully invoked site "