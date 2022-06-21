import datetime
import enum

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
        back_populates="organisations",
    )


class Resource(db.Model):
    resource = db.Column(db.Text, primary_key=True, nullable=False)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    organisations = db.relationship(
        "Organisation",
        secondary=organisation_resource,
        back_populates="organisation_resources",
    )
    datasets = db.relationship(
        "Dataset",
        secondary=dataset_resource,
        lazy="joined",
        back_populates="resources",
    )


class Collection(db.Model):
    collection = db.Column(db.Text, primary_key=True, nullable=False)
    datasets = db.relationship("Dataset")


class Dataset(db.Model):
    dataset = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text, nullable=False)
    collection_id = db.Column(db.Text, db.ForeignKey("collection.collection"))
    resources = db.relationship(
        "Resource",
        secondary=dataset_resource,
        lazy="joined",
        back_populates="datasets",
    )


class IssueType(db.Model):
    issue_type = db.Column(db.Text, primary_key=True, nullable=False)
    issue_name = db.Column(db.Text, nullable=False)
    issue_description = db.Column(db.Text, nullable=False)
    severity = db.Column(db.Text, nullable=False)
    severity_name = db.Column(db.Text, nullable=False)
    severity_description = db.Column(db.Text, nullable=False)


class DatasetReport(db.Model):
    id = db.Column(db.BIGINT, db.Sequence("dataset_report_id_seq"), primary_key=True)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"))
    dataset = db.relationship("Dataset")
    organisation_id = db.Column(db.Text, db.ForeignKey("organisation.organisation"))
    resource_id = db.Column(db.Text, db.ForeignKey("resource.resource"))
    dataset_issues = db.relationship("DatasetIssue", back_populates="dataset_report")
    created_date = db.Column(
        db.TIMESTAMP, nullable=False, default=datetime.datetime.utcnow
    )

    def has_actions(self):
        return any(i.issue.severity == "error" for i in self.dataset_issues)

    def has_recommendations(self):
        return any(i.issue.severity == "warn" for i in self.dataset_issues)

    def standard_met(self):
        return not (self.has_actions() and self.has_recommendations())


class DatasetIssue(db.Model):
    id = db.Column(
        db.BIGINT, db.Sequence("dataset_report_line_id_seq"), primary_key=True
    )
    issue_type = db.Column(db.Text, db.ForeignKey("issue_type.issue_type"))
    issue = db.relationship("IssueType")
    dataset_report_id = db.Column(db.BIGINT, db.ForeignKey("dataset_report.id"))
    count = db.Column(db.BIGINT, nullable=False, default=1)
    dataset_report = db.relationship("DatasetReport", back_populates="dataset_issues")
