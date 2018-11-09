import click
from PyMongoHandler import PyMongoHandler as MongoDb
import pandas as pd
from json import dumps, loads
from constants import *


@click.group()
def cli():
    pass


@cli.command()
@click.argument('path')
@click.argument('db')
@click.argument('collection')
@click.option('--host', default="localhost")
@click.option('--port', default=27017)
def importdb(path, db, collection, host, port):
    """
    Imports data from "xls", "xlsx", "csv", "json" or "tsv" to the MongoDB collection.
    :param db: the database in which the data is to be imported
    :param collection: the collection of the database in which the db is to be imported.
    :param port: port of the DB. Optional parameter
    :param host: host of the DB. Optional parameter
    :param path: path of the source file (csv/xls). Required Paramater.
    :return:
    """
    format = str(path).split(".")[-1]
    if format not in importdb_supported_formats:
        click.echo(format + " not supported for reading", color="red")
        return
    if "=" in path:
        path = str(path).split("=")[1].strip()
    if "=" in db:
        db = db.split("=")[1].strip()
    if "=" in collection:
        collection = collection.split("=")[1].strip()

    mongo_db = MongoDb(db, host, port)
    db_instance = mongo_db.get_db_instance
    if db_instance is None:
        click.echo(db + " doesn't exists")
        return
    mongo_db.create_collection(collection)
    if str(path).endswith(".xls") or str(path).endswith(".xlsx"):
        df = pd.read_excel(path)
        rows = df.to_dict('records')
    elif str(path).endswith(".csv"):
        df = pd.read_csv(path)
        rows = df.to_dict('records')
    elif str(path).endswith(".tsv"):
        df = pd.read_csv(path, sep="\t")
        rows = df.to_dict('records')
    else:
        file = open(path, 'r')
        contents = str(file.read())
        rows = loads(contents)
    insert_result = db_instance[collection].insert_many(rows)
    if insert_result is not None:
        click.echo(str(len(insert_result.inserted_ids)) + " rows inserted", color="green")


@cli.command()
@click.argument('path')
@click.argument('db')
@click.argument('collection')
@click.option('--host', default="localhost")
@click.option('--port', default=27017)
@click.option('--ommit_ids', default=True)
def exportdb(path, db, collection, host, port, ommit_ids):
    """
    Exports the MongoDB collection to "xls", "xlsx", "csv", "json" or "tsv"
    :param path: path of the source file (csv/xls). Required Paramater.
    :param db: the database in which the data is to be imported
    :param collection: the collection of the database in which the db is to be imported.
    :param host: host of the DB. Optional parameter
    :param port: port of the DB. Optional parameter
    :param ommit_ids:
    :return:
    """
    format = path.split(".")[-1]
    if format not in exportdb_supported_formats:
        click.echo(format + " not supported", color='red')
        return

    if "=" in path:
        path = str(path).split("=")[1].strip()
    if "=" in db:
        db = db.split("=")[1].strip()
    if "=" in collection:
        collection = collection.split("=")[1].strip()
    if "=" in format:
        format = format.split("=")[1].strip()

    mongo_db = MongoDb(db, host, port)
    db_instance = mongo_db.get_db_instance
    if db_instance is None:
        click.echo(db + " doesn't exists", color="red")
        return

    if ommit_ids:
        results = db_instance[collection].find({}, {"_id": 0})
    else:
        results = db_instance[collection].find()
    count = results.count()
    results = [x for x in results]
    df = pd.DataFrame(results)
    if format == "xls" or format == "xlsx":
        df.to_excel(path, collection.replace("_", " ").replace("-", "").title(), index=False)
    if format == "csv":
        df.to_csv(path, index=False)
    if format == "tsv":
        df.to_csv(path, sep="\t", index=False)
    if format == "json" or format == "txt":
        contents = df.to_dict('records')
        file = open(path, mode="w")
        file.write(dumps(contents))
        file.close()
    click.echo(str(count) + " rows exported", color="green")


cli()
