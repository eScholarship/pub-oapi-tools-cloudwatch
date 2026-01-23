from pub_oapi_tools_common import eschol_db
from pub_oapi_tools_common import aws_cloudwatch_logs
from time import time
import json


# =======================================
def main():
    environments = ['prod']
    for environment in environments:
        pmid_count = get_pmid_count(environment)
        send_to_cloudwatch(environment, pmid_count)


# =======================================
def send_to_cloudwatch(environment, pmid_count):
    timestamp = int(time() * 1000)
    log_entry = json.dumps(pmid_count)
    log_events = [{
        'timestamp': timestamp,
        'message': log_entry}]

    aws_cloudwatch_logs.put_logs(
        log_group="pub-oapi-tools/eschol-db-monitoring",
        log_stream=f"pmid-count-{environment}",
        log_events=log_events,
        quiet=True)


# =======================================
# connect to mysql DB, get queue values
def get_pmid_count(environment):
    query = "select count(id) `eschol_pmid_count` from items " \
            "where attrs->>\"$.local_ids\" like '%pmid%';"

    # Get connection and send query
    db = 'eschol' if environment == 'prod' else 'eschol-test'
    conn = eschol_db.get_connection(env=environment, database=db)
    with conn.cursor() as cursor:
        cursor.execute(query)
        pmid_count = cursor.fetchone()
    conn.close()

    return pmid_count


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
