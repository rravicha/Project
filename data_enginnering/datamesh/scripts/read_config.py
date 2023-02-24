
import json

def yaml_load(filename):
    from yaml import load
    try:
        from yaml import CLoader as Loader
    except ImportError:
        from yaml import Loader
    
    try:
        with open(filename,'r') as yamlfile:
            data = load(yamlfile,Loader=Loader)
        return data
    except Exception as e:
        raise Exception(e)
    
    if data is None:
        raise Exception(f"Config file {filename} has no content")

def json_load(filename):
    try:
        with open(filename,'r') as fp:
            data=json.load(fp)
        return data
    except Exception as e:
        raise Exception(e)
