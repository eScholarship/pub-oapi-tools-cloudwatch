"""
A small program for getting eSchol's engine stats into CloudWatch metrics.
1. Connects to the eSchol DB and queries various engine values
2. Packages the values into CW Metrics' ingest format
3. Sends 'em to CW.
"""

from pub_oapi_tools_common import eschol_db
from pub_oapi_tools_common import aws_cloudwatch_metrics

verbose = False


def main():
    metrics_namespace = "ESCHOLARSHIP/DBEngine"
    eschol_environments = ['staging', 'prod']

    for env in eschol_environments:
        engine_values = get_engine_values(
            env, query="SHOW GLOBAL STATUS like '%sort%';")
        engine_values += get_engine_values(
            env, query="SHOW GLOBAL STATUS like '%Threads%';")

        metrics_data = package_metrics_data(env, engine_values)
        send_to_cloudwatch(metrics_namespace, metrics_data)


# connect to mysql DB, get queue values
def get_engine_values(env, query):

    # Get connection and query eschol db
    db = 'eschol' if env == 'prod' else 'eschol-test'
    conn = eschol_db.get_connection(
        env=env, database=db, quiet=True)

    with conn.cursor() as cursor:
        cursor.execute(query)
        engine_values = cursor.fetchall()
    conn.close()

    if verbose:
        print(engine_values)

    return engine_values


# Converts MySQL output to a list of Metrics Datum
def package_metrics_data(env, engine_values):
    metrics_data = [
        {
            "MetricName": item['Variable_name'],
            "Value": item['Value'],
            "Unit": "Count",
            "Dimensions": [
                {
                    "Name": "Environment",
                    "Value": env
                }
            ]
        }
        for item in engine_values]

    if verbose:
        print(metrics_data)

    return metrics_data


# Sends the packaged metrics to the indicated namespace
def send_to_cloudwatch(metrics_namespace, metrics_data):
    aws_cloudwatch_metrics.put_metrics(
        namespace=metrics_namespace,
        metrics_data=metrics_data)


# Stub for main
if __name__ == "__main__":
    main()
