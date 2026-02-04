from pub_oapi_tools_common import eschol_db
from pub_oapi_tools_common import aws_cloudwatch_metrics

verbose = False


# =======================================
def main():
    metrics_namespace = "ESCHOLARSHIP/QUEUES"
    eschol_environments = ['staging', 'prod']

    for env in eschol_environments:
        queue_values = get_queue_values(env)
        metrics_data = package_metrics_data(env, queue_values)
        send_to_cloudwatch(metrics_namespace, metrics_data)


# =======================================
# connect to mysql DB, get queue values
def get_queue_values(env):
    query = "select queue, count(item_id) `count` from queues group by queue;"

    # Get connection and query eschol db
    db = 'eschol' if env == 'prod' else 'eschol-test'
    conn = eschol_db.get_connection(
        env=env, database=db, quiet=True)

    with conn.cursor() as cursor:
        cursor.execute(query)
        queue_values = cursor.fetchall()
    conn.close()

    if verbose:
        print(queue_values)

    return queue_values


# Converts MySQL output to a list of Metrics Datum
def package_metrics_data(env, queue_values):
    metrics_data = [
        {
            "MetricName": item['queue'],
            "Value": item['count'],
            "Unit": "Count",
            "Dimensions": [
                {
                    "Name": "Environment",
                    "Value": env
                }
            ]
        }
        for item in queue_values]

    if verbose:
        print(metrics_data)

    return metrics_data


# Sends the packaged metrics to the indicated namespace
def send_to_cloudwatch(metrics_namespace, metrics_data):
    aws_cloudwatch_metrics.put_metrics(
        namespace=metrics_namespace,
        metrics_data=metrics_data)


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
