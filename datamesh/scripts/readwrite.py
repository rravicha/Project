from datamesh.generic import parseYaml
from datamesh.generic import read
from datamesh.generic import write

def main():
    config = parseYaml(configFile=r'./config.yaml')
    df = read.load(config)
    write.target(config=config,dataframe=df)

if __name__ == "__main__":
    main()
