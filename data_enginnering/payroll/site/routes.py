from flask import Blueprint

site=Blueprint('site',__name__)

@site.route('/')
def site_main():
    return "<h1>Successfully invoked site</h1>"