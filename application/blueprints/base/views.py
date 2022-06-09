from flask import Blueprint, render_template

base = Blueprint("base", __name__)


@base.route("/")
@base.route("/index")
def index():
    return render_template("homepage.html")


@base.route("/organisation")
def org_summary():
    return render_template("organisation-summary.html")
