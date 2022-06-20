from flask import Blueprint, abort, render_template

from application.models import Organisation

base = Blueprint("base", __name__)


@base.route("/")
@base.route("/index")
def index():
    lpas = Organisation.query.order_by(Organisation.name).all()
    return render_template("homepage.html", lpas=lpas)


@base.route("/organisation/<string:organisation>")
def org_summary(organisation):
    organisation = Organisation.query.get(organisation)
    if not organisation:
        abort(404)
    return render_template("organisation-summary.html", organisation=organisation)


@base.route("/dataset-feedback")
def dataset_feedback():
    return render_template("dataset-feedback.html")
