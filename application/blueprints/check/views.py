from flask import Blueprint, redirect, render_template, url_for

from application.blueprints.check.forms import CheckForm

check = Blueprint("check", __name__)


@check.route("/check-your-data")
def check_data():
    form = CheckForm()
    if form.validate_on_submit():
        return redirect(url_for("check.results"))
    return render_template("check-your-data.html", form=form)
