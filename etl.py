#%% Imports
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import pandas as pd
import boto3
import json

#%% Variables

# config = configparser.ConfigParser()
# config.read_file(open('cluster.cfg'))

# KEY                    = config.get('AWS','KEY')
# SECRET                 = config.get('AWS','SECRET')

# DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
# DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
# DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

# DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
# DWH_DB                 = config.get("DWH","DWH_DB")
# DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
# DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
# DWH_PORT               = config.get("DWH","DWH_PORT")

# DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

# (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

# pd.DataFrame({"Param":
#                   ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
#               "Value":
#                   [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
#              })

# #%% Client variables
# client_region = "us-west-2"

# ec2 = boto3.client('ec2',
#                   region_name=client_region,
#                   aws_access_key_id=KEY,
#                   aws_secret_access_key=SECRET)

# s3 = boto3.client('s3',
#                   region_name=client_region,
#                   aws_access_key_id=KEY,
#                   aws_secret_access_key=SECRET)

# iam = boto3.client('iam',
#                   region_name=client_region,
#                   aws_access_key_id=KEY,
#                   aws_secret_access_key=SECRET)

# redshift = boto3.client('redshift',
#                   region_name=client_region,
#                   aws_access_key_id=KEY,
#                   aws_secret_access_key=SECRET)

#%% Create the IAM role
# try:
#     print('1.1 Creating a new IAM Role')
#     dwhRole = iam.create_role(
#         Path='/',
#         RoleName=DWH_IAM_ROLE_NAME,
#         Description='Allows Redshift clusters to call AWS services on your behalf.',
#         AssumeRolePolicyDocument=json.dumps(
#             {'Statement': [{'Action': 'sts:AssumeRole',
#                            'Effect': 'Allow',
#                            'Principal':{'Service': 'redshift.amazonaws.com'}}],
#             'Version': '2012-10-17'})
#     )
        
# except Exception as e:
#     print(e)

# # Attach policies
# print('1.2 Attaching Policy')
# try:
#     iam.attach_role_policy(
#         RoleName=DWH_IAM_ROLE_NAME,
#         PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
#     )['ResponseMetadata']['HTTPStatusCode']

# except Exception as e:
#     print(f"Error attaching policy: {e}")

# # Get and print the IAM role ARN
# print('1.3 Get the IAM role ARN')
# roleArn = iam.get_role(
#     RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

# print(roleArn)

#%% Create cluster
# try:
#     response = redshift.create_cluster(        
#         # Hardware
#         ClusterType = DWH_CLUSTER_TYPE,
#         NodeType = DWH_NODE_TYPE,
#         NumberOfNodes = int(DWH_NUM_NODES),

#         # Identifiers & credentials
#         DBName = DWH_DB,
#         ClusterIdentifier= DWH_CLUSTER_IDENTIFIER,
#         MasterUsername = DWH_DB_USER,
#         MasterUserPassword = DWH_DB_PASSWORD,
        
#         # Role (to allow s3 access)
#         IamRoles=[roleArn]
#     )
#     print('Cluster Succesfully Created')
# except Exception as e:
#     print(e)

#%% Check cluster creation

# def prettyRedshiftProps(props):
#     keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
#     x = [(k, v) for k,v in props.items() if k in keysToShow]
#     return pd.DataFrame(data=x, columns=["Key", "Value"])

# myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
# prettyRedshiftProps(myClusterProps)

#%% Get cluster variables
# DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
# DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
# print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
# print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

#%% Open tcl port
# try:
#     vpc = ec2.Vpc(id=myClusterProps['VpcId'])
#     defaultSg = list(vpc.security_groups.all())[0]
#     print(defaultSg)
    
#     defaultSg.authorize_ingress(
#         GroupName= defaultSg.group_name,
#         CidrIp='0.0.0.0/0',
#         IpProtocol='TCP', 
#         FromPort=int(DWH_PORT),
#         ToPort=int(DWH_PORT)
#     )
# except Exception as e:
#     print(e)

#%% Script functions

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    # insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
# %%
