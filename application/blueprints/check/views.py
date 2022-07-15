import csv
import os
import tempfile

import requests
from digital_land.api import DigitalLandApi
from flask import Blueprint, abort, current_app, render_template, request

from application.blueprints.check.forms import CheckForm
from application.models import Dataset
from application.utils import Workspace, convert_resource

check = Blueprint("check", __name__)


@check.route("/check-your-data", methods=["GET", "POST"])
def check_data():
    form = CheckForm()
    if form.validate_on_submit():
        dataset = form.datasets.data
        url = form.url.data.strip()
        results = _run_pipeline(dataset, url)
        return render_template(
            "check-your-data.html", form=form, url=url, dataset=dataset, results=results
        )
    return render_template("check-your-data.html", form=form)


def _run_pipeline(dataset_id, resource_url):

    dataset = Dataset.query.get(dataset_id)
    DATASETTE = current_app.config["DATASETTE_URL"]
    fields_url = f"{DATASETTE}/digital-land/dataset_field.json?_shape=array&dataset__exact={dataset.dataset}"
    column_url = f"{DATASETTE}/digital-land/column.json?_shape=array&dataset__exact={dataset.dataset}"
    concat_url = f"{DATASETTE}/digital-land/concat.json?_shape=array&dataset__exact={dataset.dataset}"
    default_url = f"{DATASETTE}/digital-land/_default.json?_shape=array&dataset__exact={dataset.dataset}"
    try:
        resp = requests.get(fields_url)
        resp.raise_for_status()
        fields = [f["field"] for f in resp.json()]

        resp = requests.get(column_url)
        resp.raise_for_status()
        column = resp.json()

        resp = requests.get(concat_url)
        resp.raise_for_status()
        concat = resp.json()

        resp = requests.get(default_url)
        resp.raise_for_status()
        default = resp.json()

    except Exception as e:
        print(e)
        return "Could not run pipeline"

    dataset_config = {"column": column, "concat": concat, "default": default}

    with tempfile.TemporaryDirectory() as temp_dir:

        workspace = Workspace.factory(
            dataset,
            temp_dir,
            current_app.config["PROJECT_ROOT"],
            resource_url,
            dataset_config,
        )

        # construct API object
        api = DigitalLandApi(
            False, dataset.dataset, workspace.pipeline_dir, workspace.specification_dir
        )

        # Use API to collect resource
        api.collect_cmd(workspace.endpoint_csv, workspace.collection_dir)

        resources = os.listdir(workspace.resource_dir)

        if not resources:
            print("No resource collected")
            return abort(400)
        else:
            resource_hash = resources[0]
            limit = int(request.args.get("limit")) if request.args.get("limit") else 10
            (
                resource_fields,
                input_path,
                output_path,
                resource_rows,
            ) = convert_resource(api, workspace, resource_hash, limit)

            # Run API pipeline
            api.pipeline_cmd(
                input_path,
                output_path,
                workspace.collection_dir,
                None,
                workspace.issue_dir,
                workspace.organisation_path,
                column_field_dir=workspace.column_field_dir,
                dataset_resource_dir=workspace.dataset_resource_dir,
                endpoints=["dummy-endpoint"],
            )

            issues = []
            issue_file = os.path.join(workspace.issue_dir, f"{resource_hash}.csv")
            with open(issue_file) as file:
                reader = csv.DictReader(file)
                for row in reader:
                    issues.append(row)

    # input path could be used to show user what we collected from url if useful?

    return {
        "issues": issues,
        "resource_fields": resource_fields,
        "specification_fields": fields,
        "converted_resource_rows": resource_rows,
    }
