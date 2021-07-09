import boto3

session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',region_name='xxx'
)
secret=client.get_secret_value(SecretId=secret_name)

secret_dict=json.loads(secret['SecretString'])

username=secret_dict['username']
password=secret_dict['password']

dbtype=__import__(DBtype)

conn=dbname.connect(dbtype,uname,pwd,db)

return conn