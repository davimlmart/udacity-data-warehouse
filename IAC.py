#%% Imports
import configparser
import pandas as pd
import json
import time

#%% Variables
config = configparser.ConfigParser()
config.read_file(open('cluster.cfg'))

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

#%% Functions
def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """Create IAM role and attach policies.
    Args:
        iam: AWS IAM client.
        DWH_IAM_ROLE_NAME: desired role name.
    Returns: 
        IAM role ARN"""
    print("Creating IAM role...")
    try:
        iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description='Allows Redshift clusters to call AWS services on your behalf.',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal':{'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
    except Exception as e:
        print(f"Error creating IAM role: {e}")

    # Attach policies
    print('Attaching Policy...')
    try:
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )['ResponseMetadata']['HTTPStatusCode']

    except Exception as e:
        print(f"Error attaching policy: {e}")

    # Get and print the IAM role ARN
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(f"ARN and role created successfully, roleArn: {roleArn}")
    return roleArn

def pretty_redshift_props(props):
    """Format Redshift cluster properties.
    Args:
        props: cluster properties.
    Returns: 
        Dataframe with properties"""    
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def wait_for_cluster_availability(redshift, cluster_identifier, cluster_status):
    """Waits for the cluster to become available or fail.
    Args:
        redshift: AWS Redshift client.
        cluster_identifier: cluster name.
        cluster_status: cluster current_status.
    Returns: 
        Cluster properties"""
    print("Cluster is currently", cluster_status, ". Waiting for it to become available...")
    while cluster_status not in ('available', 'failed'):
        time.sleep(30)  # Check status every 30 seconds
        cluster_status = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]['ClusterStatus']
    print("Cluster is now", cluster_status + ".")
    cluster_props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    return cluster_props

def create_cluster(redshift, ec2, cluster_identifier, roleArn):
    """Creates a cluster and waits for it to become available or fail.
    Args:
        redshift: AWS Redshift client.
        ec2: AWS EC2 client.
        cluster_identifier: cluster name.
        roleArn: User IAM role ARN.
    Returns: 
        Cluster properties"""
    print("Cluster does not exist or is in an unexpected state. Creating cluster...")
    try:
        response = redshift.create_cluster(        
            # Hardware
            ClusterType = DWH_CLUSTER_TYPE,
            NodeType = DWH_NODE_TYPE,
            NumberOfNodes = int(DWH_NUM_NODES),

            # Identifiers & credentials
            DBName = DWH_DB,
            ClusterIdentifier= cluster_identifier,
            MasterUsername = DWH_DB_USER,
            MasterUserPassword = DWH_DB_PASSWORD,
            
            # Role (to allow s3 access)
            IamRoles=[roleArn]
        )
        print('Cluster succesfully created')
    except Exception as e:
        print(e)

    cluster_status = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]['ClusterStatus']
    cluster_props = wait_for_cluster_availability(redshift, cluster_identifier, cluster_status)
    # Open tcl port
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        
        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP', 
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
    
    return cluster_props