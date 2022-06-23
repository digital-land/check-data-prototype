import concurrent
import logging

import requests
from flask.cli import AppGroup
from sqlalchemy import delete, text
from sqlalchemy.dialects.postgresql import insert

from application.extensions import db
from application.models import (
    Collection,
    Dataset,
    DatasetIssue,
    DatasetReport,
    IssueCategory,
    IssueType,
    Organisation,
    Resource,
    dataset_resource,
    issue_type_to_category,
    organisation_resource,
)

data_cli = AppGroup("data")

logger = logging.getLogger(__name__)

resource_organisation_sql = """
SELECT resource, organisation
FROM resource_organisation;
"""

collection_sql = """
SELECT collection
FROM collection
"""

dataset_sql = """
SELECT dataset, name, collection AS collection_id
FROM dataset
WHERE ("collection" IS NOT NULL AND "collection" != "");
"""

organisation_sql = """
SELECT
  name,
  organisation
FROM
  organisation
WHERE
  (
    organisation LIKE 'local-authority%'
    or organisation LIKE 'development-corporation%'
    or organisation LIKE 'national-park-authority%'
  )
  AND (
    "entry_date" IS NULL
    OR "entry_date" = ""
  );
"""

issue_type_sql = """
SELECT
  i.issue_type,
  i.name AS issue_name,
  i.description AS issue_description,
  s.severity AS severity,
  s.name AS severity_name,
  s.description AS severity_description
FROM
  issue_type i,
  severity s
WHERE
  i.severity = s.severity;
"""


@data_cli.command("load")
def load_data():

    from flask import current_app

    datasette_url = current_app.config["DATASETTE_URL"]

    print("load organisations")
    url = (
        f"{datasette_url}/digital-land.json?sql={organisation_sql.strip()}&_shape=array"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    inserts = []
    # remove -eng from local-authority ids until digital  land db catches up
    for org in data:
        inserts.append(
            {
                "name": org["name"],
                "organisation": org["organisation"].replace("-eng", ""),
            }
        )

    stmt = insert(Organisation).values(inserts)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Organisation.organisation], set_=dict(name=stmt.excluded.name)
    )

    db.session.execute(stmt)
    db.session.commit()
    print("load organisations done")

    print("load issue type")
    url = f"{datasette_url}/digital-land.json?sql={issue_type_sql.strip()}&_shape=array"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    for row in data:
        row["category"] = issue_type_to_category.get(
            row.get("issue_name"), IssueCategory.unknown
        ).name
    stmt = insert(IssueType).values(data)
    db.session.execute(stmt)
    db.session.commit()
    print("load issue type done")

    print("load resource")
    url = f"{datasette_url}/digital-land/resource.json?_shape=array&_col=resource&_col=start_date&_col=end_date"

    while url is not None:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        try:
            url = resp.links.get("next").get("url")
        except AttributeError:
            url = None
        inserts = []
        for item in data:
            resource = item.get("resource")
            start_date = item.get("start_date") if item.get("start_date") else None
            end_date = item.get("end_date") if item.get("end_date") else None
            inserts.append(
                {"resource": resource, "start_date": start_date, "end_date": end_date}
            )
        stmt = insert(Resource).values(inserts)
        db.session.execute(stmt)
        db.session.commit()

    print("load resource done")

    print("load resource_organisation")
    url = f"{datasette_url}/digital-land/resource_organisation.json?_shape=array"
    while url is not None:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        try:
            url = resp.links.get("next").get("url")
        except AttributeError:
            url = None
        inserts = []
        for item in data:
            organisation = item.get("organisation").replace("-eng", "")
            if Organisation.query.get(organisation) is not None:
                resource = item.get("resource")
                if organisation and resource:
                    inserts.append({"resource": resource, "organisation": organisation})
        if inserts:
            print(f"inserting {len(inserts)} organisation resources")
            stmt = insert(organisation_resource).values(inserts)
            db.session.execute(stmt)
            db.session.commit()
    print("load resource_organisation done")

    print("load collections")
    url = f"{datasette_url}/digital-land.json?sql={collection_sql.strip()}&_shape=array"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    stmt = insert(Collection).values(data)
    db.session.execute(stmt)
    db.session.commit()
    print("load collections done")

    print("load datasets")
    url = f"{datasette_url}/digital-land.json?sql={dataset_sql.strip()}&_shape=array"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    stmt = insert(Dataset).values(data)
    db.session.execute(stmt)
    db.session.commit()
    print("load datasets done")

    print("load dataset resources")
    datasets = Dataset.query.all()
    for dataset in datasets:
        url = f"{datasette_url}/{dataset.dataset}/dataset_resource.json?_shape=array&_col=resource&_col=dataset"
        print(f"dataset: {dataset.dataset} url: {url}")
        while url is not None:
            try:
                resp = requests.get(url)
                resp.raise_for_status()
            except Exception as e:
                print(e)
                url = None
                continue
            data = resp.json()
            inserts = []
            for item in data:
                resource = item.get("resource")
                ds = item.get("dataset")
                if resource and dataset:
                    inserts.append({"resource": resource, "dataset": ds})
            try:
                url = resp.links.get("next").get("url")
            except AttributeError:
                url = None
            if inserts:
                print(
                    f"inserting {len(inserts)} dataset resources for {dataset.dataset}"
                )
                stmt = insert(dataset_resource).values(inserts)
                db.session.execute(stmt)
                db.session.commit()
    print("load datasets done")


@data_cli.command("drop")
def drop_data():

    stmt = delete(DatasetIssue)
    db.session.execute(stmt)

    stmt = delete(DatasetReport)
    db.session.execute(stmt)

    stmt = delete(organisation_resource)
    db.session.execute(stmt)

    stmt = delete(dataset_resource)
    db.session.execute(stmt)

    stmt = delete(Dataset)
    db.session.execute(stmt)

    stmt = delete(Collection)
    db.session.execute(stmt)

    stmt = delete(Organisation)
    db.session.execute(stmt)

    stmt = delete(Resource)
    db.session.execute(stmt)

    stmt = delete(IssueType)
    db.session.execute(stmt)

    db.session.commit()


@data_cli.command("report")
def create_dataset_issue_report():

    from flask import current_app

    url = "https://digital-land-issues.herokuapp.com/dataset-issue"

    query = """SELECT d.dataset, r.resource, o.organisation
    FROM dataset d, resource r, dataset_resource dr, organisation o, organisation_resource orgr
    WHERE d.dataset = dr.dataset
    AND r.resource = dr.resource
    AND dr.resource = orgr.resource
    AND o.organisation = orgr.organisation
    ORDER BY d.dataset"""

    sql = text(query)
    datasets = db.session.execute(sql).fetchall()

    dataset_issue_reports = []
    for ds in datasets:
        dataset_issue_reports.append({"url": url, "dataset": ds})

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for item in dataset_issue_reports:
            futures.append(
                executor.submit(
                    generate_report,
                    base_url=item["url"],
                    ds=item["dataset"],
                    app_context=current_app.app_context(),
                )
            )
        for future in concurrent.futures.as_completed(futures):
            print(future.result())


def generate_report(base_url, ds, app_context):
    with app_context:
        dataset = ds[0]
        resource = ds[1]
        organisation = ds[2]
        issue_url = f"{base_url}/{dataset}-issue.json?_shape=array&_col=resource&_col=field&_col=value&_col=issue-type&resource__exact={resource}"  # noqa
        while issue_url is not None:
            try:
                resp = requests.get(issue_url)
                issue_data = resp.json()
                if issue_data:
                    report = DatasetReport.query.filter(
                        DatasetReport.dataset_id == dataset,
                        DatasetReport.resource_id == resource,
                        DatasetReport.organisation_id == organisation,
                    ).one_or_none()
                    if report is None:
                        report = DatasetReport(
                            dataset_id=dataset,
                            resource_id=resource,
                            organisation_id=organisation,
                        )
                        db.session.add(report)
                        db.session.flush()
                        print(
                            f"created report dataset: {dataset} | resource: {resource} | organisation: {organisation}"
                        )
                    else:
                        print(
                            f"update report dataset: {dataset} | resource: {resource} | organisation: {organisation}"
                        )
                    for issue in issue_data:
                        issue_type = issue.get("issue-type")
                        field = issue.get("field")
                        if issue_type.lower() != "unknown entity":
                            dataset_issue = DatasetIssue.query.filter(
                                DatasetIssue.dataset_report_id == report.id,
                                DatasetIssue.issue_type == issue_type,
                                DatasetIssue.field == field,
                            ).one_or_none()
                            if dataset_issue is None:
                                dataset_issue = DatasetIssue(
                                    issue_type=issue["issue-type"], field=field
                                )
                                report.dataset_issues.append(dataset_issue)
                            else:
                                dataset_issue.count += 1

                    db.session.add(report)
                    db.session.commit()

                try:
                    issue_url = resp.links.get("next").get("url")
                except AttributeError:
                    issue_url = None

            except Exception as e:
                print(e)
                issue_url = None

        return f"completed report dataset: {dataset} | resource: {resource} | organisation: {organisation}"
