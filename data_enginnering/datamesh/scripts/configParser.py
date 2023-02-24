import yaml
import json
import sys


from scripts import projectConstants


class ConfigParser(object):
    def __init__(self, config_dict):
        self.__dict__.update(config_dict)


def configDictToObjConverter(configDict):
    return json.loads(json.dumps(configDict), object_hook=ConfigParser)


def parseConfig(logger, configFileName):
    try:
        with open(configFileName, "r") as config_file:
            config_dict = yaml.load(config_file, Loader=yaml.SafeLoader)
            logger.info("Parsed {} from {} Successfully...".format(projectConstants.DEFAULT_CONFIG_FILES_PATH, configFileName))
            return config_dict
    except FileNotFoundError:
        logger.error("Cannot find {} in {}...".format(projectConstants.DEFAULT_CONFIG_FILES_PATH, configFileName))
        logger.info("Job terminating with exitCode(1)")
        sys.exit(1)


def getConfig(logger, appName):
    config_file_name = appName+'.yaml'
    logger.info("Trying to parse {} constants from {} configuration file...".format(appName, config_file_name))
    return parseConfig(logger, config_file_name)