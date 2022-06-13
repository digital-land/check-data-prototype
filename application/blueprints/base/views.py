from flask import Blueprint, render_template

from application.models import Organisation

base = Blueprint("base", __name__)


@base.route("/")
@base.route("/index")
def index():
    lpas = Organisation.query.order_by(Organisation.name).all()
    return render_template("homepage.html", lpas=lpas)


@base.route("/organisation")
def org_summary():
    return render_template("organisation-summary.html")
