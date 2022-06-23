import yaml

#Read a yaml file.
def read_yaml(name):
    with open(name+'.yaml') as file:
        try:
            databaseConfig = yaml.safe_load(file)   
            return databaseConfig
        except yaml.YAMLError as exc:
            print(exc)