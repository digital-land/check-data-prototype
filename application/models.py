import datetime
import enum

from application.extensions import db


class DatasetStatus(enum.Enum):
    action_needed = "Action needed"
    has_recommendations = "Has recommendations"
    standard_met = "Standard met"


class ActionCategory(enum.Enum):
    add = "Add missing data"
    change = "Change the data format"
    # remove = "Remove incorrect or inaccurate data"
    check = "Check accuracy of data"


class IssueCategory(enum.Enum):

    does_not_meet_standard = "Data doesn't meet the standard"
    incorrect_format = "Data format incorrect"
    inaccurate_data = "Inaccurate data"
    invalid_data = "Invalid data"
    missing_data = "Missing data"
    unknown = "Uknown"


issue_type_to_category = {
    "Combined values": IssueCategory.does_not_meet_standard,
    "Removed URI prefix": IssueCategory.does_not_meet_standard,
    "Patched value": IssueCategory.does_not_meet_standard,
    "Mercator conversion": IssueCategory.incorrect_format,
    "Mercator flipped": IssueCategory.incorrect_format,
    "OSGB conversion": IssueCategory.incorrect_format,
    "OSGB flipped": IssueCategory.incorrect_format,
    "WGS84 flipped": IssueCategory.inaccurate_data,
    "WGS84 out of bounds": IssueCategory.inaccurate_data,
    "Future entry date": IssueCategory.inaccurate_data,
    "Invalid URI": IssueCategory.invalid_data,
    "Invalid WKT": IssueCategory.invalid_data,
    "Invalid coordinates": IssueCategory.invalid_data,
    "Invalid date": IssueCategory.invalid_data,
    "Invalid decimal": IssueCategory.invalid_data,
    "Invalid flag": IssueCategory.invalid_data,
    "Invalid geometry": IssueCategory.invalid_data,
    "Invalid integer": IssueCategory.invalid_data,
    "Value too large": IssueCategory.invalid_data,
    "Value too small": IssueCategory.invalid_data,
    "Default field": IssueCategory.missing_data,
    "Default value": IssueCategory.missing_data,
}


issue_category_to_type = {
    IssueCategory.does_not_meet_standard: set(
        [
            "Combined values",
            "Removed URI prefix",
            "Patched value",
        ]
    ),
    IssueCategory.incorrect_format: set(
        [
            "Mercator conversion",
            "Mercator flipped",
            "OSGB conversion",
            "OSGB flipped",
        ]
    ),
    IssueCategory.inaccurate_data: set(
        [
            "WGS84 flipped",
            "WGS84 out of bounds",
            "Future entry date",
        ]
    ),
    IssueCategory.invalid_data: set(
        [
            "Invalid WKT",
            "Invalid coordinates",
            "Invalid date",
            "Invalid decimal",
            "Invalid flag",
            "Invalid geometry",
            "Invalid integer",
            "Value too large",
            "Value too small",
        ]
    ),
    IssueCategory.missing_data: set(["Default field", "Default value"]),
}


action_to_issue_categories = {
    ActionCategory.add: set([IssueCategory.missing_data]),
    ActionCategory.change: set(
        [
            IssueCategory.invalid_data,
            IssueCategory.inaccurate_data,
            IssueCategory.incorrect_format,
        ]
    ),
    ActionCategory.check: set([IssueCategory.does_not_meet_standard]),
}

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
    category = db.Column(db.String, nullable=False)


class DatasetReport(db.Model):
    id = db.Column(db.BIGINT, db.Sequence("dataset_report_id_seq"), primary_key=True)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"))
    dataset = db.relationship("Dataset")
    organisation_id = db.Column(db.Text, db.ForeignKey("organisation.organisation"))
    organisation = db.relationship("Organisation")
    resource_id = db.Column(db.Text, db.ForeignKey("resource.resource"))
    resource = db.relationship("Resource")
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

    def get_additions(self):
        issues = self.get_issues_by_action_type(ActionCategory.add)
        return issues

    def get_changes(self):
        issues = self.get_issues_by_action_type(ActionCategory.change)
        return issues

    def get_checks(self):
        issues = self.get_issues_by_action_type(ActionCategory.check)
        return issues

    def get_issues_by_action_type(self, action):
        issues = []
        categories = action_to_issue_categories.get(action)
        issue_types = []
        for category in categories:
            issue_types.extend(list(issue_category_to_type[category]))

        for issue in self.dataset_issues:
            if issue.issue.issue_name in issue_types:
                issues.append(issue)

        return issues


class DatasetIssue(db.Model):
    id = db.Column(
        db.BIGINT, db.Sequence("dataset_report_line_id_seq"), primary_key=True
    )
    issue_type = db.Column(db.Text, db.ForeignKey("issue_type.issue_type"))
    issue = db.relationship("IssueType")
    dataset_report_id = db.Column(db.BIGINT, db.ForeignKey("dataset_report.id"))
    count = db.Column(db.BIGINT, nullable=False, default=1)
    dataset_report = db.relationship("DatasetReport", back_populates="dataset_issues")
    field = db.Column(db.String, nullable=False)
    value = db.Column(db.String)
