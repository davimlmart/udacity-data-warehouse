#%% Imports
import configparser
import psycopg2
import pandas as pd
import boto3
from sql_queries import create_table_queries, drop_table_queries
from IAC import create_iam_role, pretty_redshift_props, wait_for_cluster_availability, create_cluster

#%% Variables

config = configparser.ConfigParser()
config.read_file(open('cluster.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

#%% Client variables
client_region = "us-west-2"

ec2 = boto3.client('ec2',
                  region_name=client_region,
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET)

s3 = boto3.client('s3',
                  region_name=client_region,
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET)

iam = boto3.client('iam',
                  region_name=client_region,
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET)

redshift = boto3.client('redshift',
                  region_name=client_region,
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET)

#%% Check for existing ARN and role and create if necessary
try:
    existing_role = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(f"ARN and role already exist, roleArn: {roleArn}")
except iam.exceptions.NoSuchEntityException:
    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)

#%% Check cluster status and create if necessary
    
try:
    cluster_status = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
except:
    cluster_status = 'ClusterNotFound'

if cluster_status == 'available':
    print("Cluster is already available.")

elif cluster_status in ('creating', 'modifying'):
    cluster_props = wait_for_cluster_availability(redshift, DWH_CLUSTER_IDENTIFIER, cluster_status)

else:
    cluster_props = create_cluster(redshift, ec2, DWH_CLUSTER_IDENTIFIER, roleArn)

#%% Print cluster properties
    
pretty_redshift_props(cluster_props)

DWH_ENDPOINT = cluster_props['Endpoint']['Address']
DWH_ROLE_ARN = cluster_props['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

#%% Update dwh.cfg if needed

parser = configparser.ConfigParser()
parser.read('dwh.cfg')

if parser['CLUSTER']['HOST'] != DWH_ENDPOINT or parser['IAM_ROLE']['ARN'] != DWH_ROLE_ARN:
    parser['CLUSTER']['HOST'] = DWH_ENDPOINT
    parser['IAM_ROLE']['ARN'] = DWH_ROLE_ARN
    with open('dwh.cfg', 'w') as configfile:
        parser.write(configfile)
    print("Updated dwh.cfg with new cluster endpoint and ARN.")

#%% Create tables functions
    
def drop_tables(cur, conn):
    """Runs specified queries to drop tables.
    Args:
        cur: connector cursor.
        conn: connector."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Runs specified queries to create tables.
    Args:
        cur: connector cursor.
        conn: connector."""    
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def main():
    """Create connector, runs functions to drop and create tables, prints final message."""
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*parser['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

    print('Cluster available and tables created')

#%% Create tables

if __name__ == "__main__":
    main()