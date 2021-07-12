import yaml

def config_load(filename):
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

# if __name__="__main__":
#     file='config.yaml'
#     data=read_config(file)
#     print(data)