import boto3
import pymysql
from time import time
import json

session = boto3.Session()


# =======================================
def main():
    environments = ['staging', 'prod']
    for environment in environments:
        creds = get_creds(environment)
        queue_values = get_queue_values(creds)

        # Transforms the queue values to a single dict
        counts_by_queue = {
            i['queue']: i['count'] for i in queue_values}

        send_to_cloudwatch(environment, counts_by_queue)


# =======================================
def send_to_cloudwatch(environment, counts_by_queue):
    events_client = session.client(
        service_name='logs',
        region_name='us-west-2')
    log_group = "pub-oapi-tools/eschol-db-monitoring"
    log_stream = f"queues-{environment}"

    timestamp = int(time() * 1000)
    log_entry = json.dumps(counts_by_queue)
    log_events = [{
        'timestamp': timestamp,
        'message': log_entry}]

    response = events_client.put_log_events(
        logGroupName=log_group,
        logStreamName=log_stream,
        logEvents=log_events)

    print(response)


# =======================================
# connect to mysql DB, get queue values
def get_queue_values(creds):

    mysql_conn = pymysql.connect(
        host=creds['server'],
        user=creds['user'],
        password=creds['password'],
        database=creds['database'],
        cursorclass=pymysql.cursors.DictCursor)

    # Open cursor and send query
    query = "select queue, count(item_id) `count` " \
            "from queues group by queue;"

    with mysql_conn.cursor() as cursor:
        print("Connected to eSchol MySQL DB. Sending Query.")
        cursor.execute(query)
        queue_values = cursor.fetchall()

    mysql_conn.close()

    return queue_values


# =======================================
# Setup function, connects to AWS for creds
def get_creds(environment):

    def get_ssm_parameters(folder, names):
        ssm_client = session.client(
            service_name='ssm',
            region_name='us-west-2')

        param_names = [f"{folder}/{name}" for name in names]

        response = ssm_client.get_parameters(
            Names=param_names,
            WithDecryption=True)

        param_values = {
            (param['Name'].split('/')[-1]): param['Value']
            for param in response['Parameters']}

        return param_values

    creds = get_ssm_parameters(
        f"/pub-oapi-tools/eschol-db/{environment}",
        ['user', 'password', 'server', 'database'])

    return creds


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
