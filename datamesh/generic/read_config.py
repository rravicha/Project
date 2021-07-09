import yaml

def read_config(filename):
    from yaml import load
    try:
        from yaml import CLoader as Loader
    except ImportError:
        from yaml import Loader
    
    try:
        with open(filename,'r') as yamlfile:
            data = load(yamlfile,Loader=Loader)
    except Exception as e:
        raise Exception(e)
    
    if data is None:
        raise Exception(f"Config file {filename} has no content")