# import click
# import requests
from flask.cli import AppGroup

management_cli = AppGroup("manage")


@management_cli.command("load-data")
def load_data():
    print("load data command")
