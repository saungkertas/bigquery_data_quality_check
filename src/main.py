import argparse
import subprocess
import sys
import os
from datetime import datetime

from bigquery import get_client
from bigquery.errors import BigQueryTimeoutException
from pyscaffold.cli import parse_args


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="specify job")
    parser.add_argument(
        '--key', help="location json key credentials")
    parser.add_argument('--destination_project_id', required=True)
    parser.add_argument('--destination_dataset', required=True)
    parser.add_argument('--destination_table', required=True)
    parser.add_argument('--date_start', required=True)
    parser.add_argument('--date_end')
    parser.add_argument('--datepref')
    return parser.parse_args(args)


def main(args):
    args = parse_args(args)
    if args.key:
        json_key = args.key
    else:
        json_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    client = get_client(json_key_file=json_key, readonly=False)

    date_start = datetime.strptime(args.date_start, "%Y%m%d")
    date_end = datetime.strptime(args.date_end, "%Y%m%d")

    print(
        "Starting checking completeness and UNIQUENESS for " + args.destination_dataset + '.' + args.destination_table + ' ....')
    print("-----")
    print("DATE START:" + str(date_start))
    print("DATE END:" + str(date_end))
    print("-----")
    print("")

    # get schema
    schema = client.get_table_schema(dataset=args.destination_dataset, table=args.destination_table,
                                     project_id=args.destination_project_id)

    for desc in schema:
        print('Now checking column: ' + desc['name'] + ' ....')
        if (desc['type'] != 'RECORD'):
            if (args.datepref == 'PARTITIONTIME'):
                parameterized_query = \
                    "WITH T1 as( select _partitiontime as pt, countif(_column is null) as column_null, countif(_column is not null) as column_not_null, count(distinct _column) as column_unique FROM `_project._dataset._table` WHERE _PARTITIONTIME >= 'dstart' AND _PARTITIONTIME < 'dend' group by 1 ), T2 AS( SELECT '_dataset' AS dataset_name, '_table' AS table_name, '_column' AS column_name, pt, COALESCE(column_null, 0) AS column_null, COALESCE(column_not_null, 0) AS column_not_null, COALESCE(column_unique, 0) as column_unique FROM T1 )SELECT dataset_name, table_name, column_name, pt, column_null, column_not_null, (column_not_null/(column_not_null+column_null)*100) AS completeness_percentage, (column_unique/(column_not_null+column_null)*100) as uniqueness_percentage FROM T2"
                query = parameterized_query.replace('dstart', date_start.strftime("%Y-%m-%d")) \
                    .replace('dend', date_end.strftime("%Y-%m-%d")) \
                    .replace('_column', desc['name']) \
                    .replace('_project', args.destination_project_id) \
                    .replace('_dataset', args.destination_dataset) \
                    .replace('_table', args.destination_table)
            else:
                parameterized_query = "WITH T1 AS( SELECT date(_datepref,'Asia/Jakarta') AS pt, COUNTIF(_column IS NULL) AS column_null, COUNTIF(_column IS NOT NULL) AS column_not_null, COUNT(DISTINCT _column) AS column_unique FROM `_project._dataset._table` WHERE _datepref >= 'dstart' AND _datepref < 'dend' GROUP BY 1 ), T2 AS( SELECT '_dataset' AS dataset_name, '_table' AS table_name, '_column' AS column_name, pt, COALESCE(column_null, 0) AS column_null, COALESCE(column_not_null, 0) AS column_not_null, COALESCE(column_unique, 0) AS column_unique FROM T1 ) SELECT dataset_name, table_name, column_name, CAST(pt AS TIMESTAMP) as pt, column_null, column_not_null, (column_not_null/(column_not_null+column_null)*100) AS completeness_percentage, (column_unique/(column_not_null+column_null)*100) AS uniqueness_percentage FROM T2"
                query = parameterized_query.replace('dstart', date_start.strftime("%Y-%m-%d")) \
                    .replace('dend', date_end.strftime("%Y-%m-%d")) \
                    .replace('_column', desc['name']) \
                    .replace('_project', args.destination_project_id) \
                    .replace('_dataset', args.destination_dataset) \
                    .replace('_table', args.destination_table) \
                    .replace('_datepref', args.datepref)

            # print(query)
            job_id, _ = client.query(query=query, use_legacy_sql=False, timeout=6000)

            # results = client.get_query_results(job_id=job_id, timeout=6000)
            # print(results)

            job = client.write_to_table(
                query=query,
                use_legacy_sql=False, dataset='playground', table='xx_dqs_completeness_check',
                project_id=args.destination_project_id, write_disposition='WRITE_APPEND',
                create_disposition='CREATE_IF_NEEDED')

            try:
                job_resource = client.wait_for_job(job=job, timeout=6000)
                # print(job_resource)
            except BigQueryTimeoutException:
                print("Timeout")

            print('Yeay! checking column ' + desc[
                'name'] + ' is finish! please kindly check the result in bi-gojek.playground.xx_dqs_completeness_check.')
            print('----------------------')


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
