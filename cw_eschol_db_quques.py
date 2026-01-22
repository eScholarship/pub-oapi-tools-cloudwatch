from pub_oapi_tools_common import eschol_db
from pub_oapi_tools_common import aws_cloudwatch_logs
from time import time
import json

verbose = False


# =======================================
def main():
    environments = ['staging', 'prod']
    for environment in environments:
        queue_values = get_queue_values(environment)

        # Transforms the queue values to a single dict
        counts_by_queue = {i['queue']: i['count'] for i in queue_values}

        if verbose:
            print(counts_by_queue)

        send_to_cloudwatch(environment, counts_by_queue)


# =======================================
def send_to_cloudwatch(environment, counts_by_queue):
    timestamp = int(time() * 1000)
    log_entry = json.dumps(counts_by_queue)
    log_events = [{
        'timestamp': timestamp,
        'message': log_entry}]

    aws_cloudwatch_logs.put_logs(
        log_group="pub-oapi-tools/eschol-db-monitoring",
        log_stream=f"queues-{environment}",
        log_events=log_events)


# =======================================
# connect to mysql DB, get queue values
def get_queue_values(environment):
    query = "select queue, count(item_id) `count` from queues group by queue;"

    # Get connection and send query
    db = 'eschol' if environment == 'prod' else 'eschol-test'
    conn = eschol_db.get_connection(env=environment, database=db)
    with conn.cursor() as cursor:
        cursor.execute(query)
        queue_values = cursor.fetchall()
    conn.close()

    return queue_values


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
