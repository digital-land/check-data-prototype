import enum

from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

from application.extensions import db


class DatasetStatus(enum.Enum):
    action_needed = "Action needed"
    has_recommendations = "Has recommendations"
    standard_met = "Standard met"


dataset_resource = db.Table(
    "dataset_resource",
    db.Column("dataset", db.Text, db.ForeignKey("dataset.dataset"), primary_key=True),
    db.Column(
        "resource", db.Text, db.ForeignKey("resource.resource"), primary_key=True
    ),
)

organisation_resource = db.Table(
    "organisation_resource",
    db.Column(
        "organisation",
        db.Text,
        db.ForeignKey("organisation.organisation"),
        primary_key=True,
    ),
    db.Column(
        "resource", db.Text, db.ForeignKey("resource.resource"), primary_key=True
    ),
)


class Organisation(db.Model):
    organisation = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text, nullable=False)
    organisation_resources = db.relationship(
        "Resource",
        secondary=organisation_resource,
        lazy="subquery",
        back_populates="organisations",
    )


class Resource(db.Model):
    resource = db.Column(db.Text, primary_key=True, nullable=False)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    organisations = db.relationship(
        "Organisation",
        secondary=organisation_resource,
        lazy="subquery",
        back_populates="organisation_resources",
    )
    datasets = db.relationship(
        "Dataset",
        secondary=dataset_resource,
        lazy="subquery",
        back_populates="dataset_resources",
    )


class Collection(db.Model):
    collection = db.Column(db.Text, primary_key=True, nullable=False)
    datasets = relationship("Dataset")


class Dataset(db.Model):
    dataset = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text, nullable=False)
    collection_id = db.Column(db.Text, ForeignKey("collection.collection"))
    dataset_resources = db.relationship(
        "Resource",
        secondary=dataset_resource,
        lazy="subquery",
        back_populates="datasets",
    )


class IssueType(db.Model):
    issue_type = db.Column(db.Text, primary_key=True, nullable=False)
    issue_name = db.Column(db.Text, nullable=False)
    issue_description = db.Column(db.Text, nullable=False)
    severity = db.Column(db.Text, nullable=False)
    severity_name = db.Column(db.Text, nullable=False)
    severity_description = db.Column(db.Text, nullable=False)


# class DatasetStatus(db.Model):
#     id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
#     status = db.Column(ENUM(DatasetStatus), nullable=False)
#     dataset_id = db.Column(
#         db.Text, ForeignKey("dataset.dataset")
#     )
#     organisation_id = db.Column(
#         db.Text, ForeignKey("organisation.organisation")
#     )


# class DatasetReport(db.Model):
#     pass
