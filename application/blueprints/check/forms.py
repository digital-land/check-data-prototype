from flask_wtf import FlaskForm
from wtforms import SelectField, URLField
from wtforms.validators import DataRequired

from application.models import Dataset


class CheckForm(FlaskForm):
    def __init__(self):
        super(CheckForm, self).__init__()
        self.datasets.choices = [
            (d.dataset, d.name) for d in Dataset.query.order_by(Dataset.name).all()
        ]

    datasets = SelectField("Datasets", validators=[DataRequired()])
    url = URLField("Source URL", validators=[DataRequired()])
