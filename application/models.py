from application.extensions import db


class Organisation(db.Model):
    organisation = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text, nullable=False)