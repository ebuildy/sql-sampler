from pyhocon import ConfigFactory
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

import logging
import json
import argparse
import csv
import os
import datetime

def sample():
    parser = argparse.ArgumentParser()

    parser.add_argument('--json', help="Generate JSON samples", action='store_true')
    parser.add_argument('--sql', help="Generate SQL samples", action='store_true')
    parser.add_argument('--csv', help="Generate CSV samples", action='store_true')
    parser.add_argument('--output', help="Folder path to write samples", default='./fixtures/{format}/{table}.{format}')
    parser.add_argument('conf')

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    appConf = ConfigFactory.parse_file(args.conf).get('sampler')

    mysqlConf = appConf.get("mysql")

    logging.debug('Connecting to "%s:%s/%s" as %s', mysqlConf.get('host'), mysqlConf.get("port"), mysqlConf.get("db"), mysqlConf.get("user"))

    engine = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=mysqlConf.get("user"),
                                                                                        password=mysqlConf.get("password"),
                                                                                        host=mysqlConf.get("host"),
                                                                                        port=mysqlConf.get("port"),
                                                                                        db=mysqlConf.get("db")))
    session_obj = sessionmaker(bind=engine)
    session = scoped_session(session_obj)
    conn = engine.connect()

    for filter in appConf.get("filters").keys():
        filterConf = appConf.get("filters").get(filter)

        sql = filterConf.get("query").format(**{k:str(tuple(v)) for k,v in filterConf.get("args").items()})

        logging.debug("filter: exec %s", sql)

        results = conn.execute(sql)

    artists = [row["artist_id"] for row in results]

    def escape_value(v):
        return v.translate(str.maketrans({'"' : '\\"'}))

    def get_output_path(format, table):
        path = args.output.format(format=format,table=table)
        dir = os.path.dirname(path)

        if not os.path.exists(dir):
            os.makedirs(dir)

        return path

    def format_value_for_sql(value):
        if type(value) in (str, datetime.datetime):
            return '"' + escape_value(str(value)) + '"'

        return str(value) if value else "null"


    for table in appConf.get("tables").keys():
        tableConf = appConf.get("tables").get(table)

        sql = "SELECT * FROM {table}".format(table=table)

        if "where" in tableConf:
            whereClause = tableConf.get("where").format(artists=str(tuple(artists)))
            sql = "{sql} WHERE {where}".format(sql=sql, where=whereClause)

        logging.debug('exec "%s"', sql)

        results = conn.execute(sql)

        columns = results.keys()
        results = [dict(r) for r in results]

        if len(results) == 0:
            continue

        #print(columns)
        #print(results[0])
        #print([type(v) for v in results[0]])
        #continue

        if args.sql:
            sqlValues = ",\n".join([('(' + ','.join([format_value_for_sql(v) for v in item.values()]) + ')') for item in results])
            sqlInsert = "INSERT INTO {table} ({columns}) VALUES \n{values} ".format(
                                                                            table=table,
                                                                            columns=','.join(['`' + c + '`' for c in columns]),
                                                                            values=sqlValues)

            with open(get_output_path("sql", table), "w") as file:
                file.write(sqlInsert)
                file.close()

        if args.json:
            with open(get_output_path("json", table), "w") as file:
                file.write(json.dumps([dict(row) for row in results], indent=4, sort_keys=True, default=str))
                file.close()

        if args.csv:
            with open(get_output_path("csv", table), "w") as file:
                writer = csv.DictWriter(file, fieldnames=list(columns), delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(results)
                file.close()

if __name__ == '__main__':
    sample()
