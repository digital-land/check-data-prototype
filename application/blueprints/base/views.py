from flask import Blueprint, abort, render_template
from sqlalchemy import text

from application.models import DatasetReport, Organisation

base = Blueprint("base", __name__)


@base.route("/")
@base.route("/index")
def index():
    lpas = Organisation.query.order_by(Organisation.name).all()
    return render_template("homepage.html", lpas=lpas)


@base.route("/organisation/<string:organisation>")
def org_summary(organisation):
    from application.extensions import db

    organisation = Organisation.query.get(organisation)
    if not organisation:
        abort(404)

    query = f"""
        SELECT d.dataset as dataset, r.resource as resource, o.organisation as organisation, r.start_date as start_date
        FROM dataset d, resource r, dataset_resource dr, organisation o, organisation_resource orgr
        WHERE d.dataset = dr.dataset
        AND r.resource = dr.resource
        AND dr.resource = orgr.resource
        AND o.organisation = orgr.organisation
        AND o.organisation = '{organisation.organisation}'
        ORDER BY d.dataset, r.start_date;
    """

    sql = text(query)
    results = db.session.execute(sql).fetchall()
    latest_resource_map = {}
    for result in results:
        dataset = result.dataset
        resource = result.resource
        start_date = result.start_date
        if dataset not in latest_resource_map:
            latest_resource_map[dataset] = {
                "resource": resource,
                "start_date": start_date,
            }
        else:
            if latest_resource_map[dataset]["start_date"] < start_date:
                latest_resource_map[dataset] = {
                    "resource": resource,
                    "start_date": start_date,
                }

    reports = []

    for item, data in latest_resource_map.items():
        resource = data.get("resource")
        r = DatasetReport.query.filter(
            DatasetReport.dataset_id == item,
            DatasetReport.resource_id == resource,
            DatasetReport.organisation_id == organisation.organisation,
        ).one_or_none()
        if r is not None:
            reports.append(r)

    last_update = None
    for report in reports:
        if last_update is None:
            last_update = report.created_date
        elif report.created_date > last_update:
            last_update = report.created_date

    return render_template(
        "organisation-summary.html",
        organisation=organisation,
        reports=reports,
        last_update=last_update,
    )


@base.route("/organisation/<string:organisation>/<string:dataset>/feedback")
def dataset_feedback(organisation, dataset):
    return render_template("dataset-feedback.html")
