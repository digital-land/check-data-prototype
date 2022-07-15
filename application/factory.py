# -*- coding: utf-8 -*-
"""
Flask app factory class
"""
import os

import requests
from flask import Flask
from flask.cli import load_dotenv

from application.models import *  # noqa

load_dotenv()


def create_app(config_filename):
    """
    App factory function
    """
    app = Flask(__name__)
    app.config.from_object(config_filename)
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 10

    register_blueprints(app)
    register_context_processors(app)
    register_templates(app)
    register_filters(app)
    register_extensions(app)
    register_commands(app)

    get_specification(app)

    return app


def register_blueprints(app):
    """
    Import and register blueprints
    """

    from application.blueprints.base.views import base
    from application.blueprints.check.views import check

    app.register_blueprint(base)
    app.register_blueprint(check)


def register_context_processors(app):
    """
    Add template context variables and functions
    """

    def base_context_processor():
        return {"assetPath": "/static"}

    app.context_processor(base_context_processor)


def register_filters(app):
    from application.filters import (
        debug,
        dump_json,
        get_items_beginning_with,
        short_date,
    )

    app.add_template_filter(get_items_beginning_with, name="get_items_beginning_with")
    app.add_template_filter(debug, name="debug")
    app.add_template_filter(short_date, name="short_date")
    app.add_template_filter(dump_json, name="dump_json")


def register_extensions(app):
    from application.extensions import db, migrate

    db.init_app(app)
    migrate.init_app(app)


def register_templates(app):
    """
    Register templates from packages
    """
    from jinja2 import ChoiceLoader, PackageLoader, PrefixLoader

    multi_loader = ChoiceLoader(
        [
            app.jinja_loader,
            PrefixLoader(
                {
                    "govuk_frontend_jinja": PackageLoader("govuk_frontend_jinja"),
                    "digital-land-frontend": PackageLoader("digital_land_frontend"),
                }
            ),
        ]
    )
    app.jinja_loader = multi_loader


def register_commands(app):
    from application.commands import data_cli

    app.cli.add_command(data_cli)


def get_specification(app):

    specification_dir = os.path.join(app.config["PROJECT_ROOT"], "specification")
    if not os.path.exists(specification_dir):
        os.mkdir(specification_dir)

    specification_files = [
        "collection",
        "dataset",
        "dataset-schema",
        "schema",
        "schema-field",
        "field",
        "datatype",
        "typology",
        "pipeline",
        "theme",
    ]

    for file in specification_files:

        spec_file = os.path.join(specification_dir, f"{file}.csv")
        spec_url = f"https://raw.githubusercontent.com/digital-land/specification/main/specification/{file}.csv"

        if not os.path.exists(spec_file):
            print(f"Downloading {spec_url} to {spec_file}")
            resp = requests.get(spec_url)
            resp.raise_for_status()
            outfile_name = os.path.join(specification_dir, f"{file}.csv")
            with open(outfile_name, "w") as file:
                file.write(resp.content.decode("utf-8"))
        else:
            print(f"{spec_url} already downloaded to {spec_file}")

    print("Specification done")
