import requests
from flask.cli import AppGroup

from application.models import Organisation

management_cli = AppGroup("manage")


organisation_sql = """
SELECT
  name,
  organisation
FROM
  organisation
WHERE
  (
    organisation LIKE 'local-authority%'
    or organisation LIKE 'development-corporation%'
    or organisation LIKE 'national-park-authority%'
  )
  AND (
    "entry_date" is null
    OR "entry_date" = ""
  )
"""


@management_cli.command("load-data")
def load_data():

    from flask import current_app
    from sqlalchemy.dialects.postgresql import insert

    from application.extensions import db

    print("load organisations")
    datasette_organisation_url = f"{current_app.config['DATASETTE_URL']}/digital-land.json?sql={organisation_sql.strip()}&_shape=array"  # noqa
    resp = requests.get(datasette_organisation_url)
    resp.raise_for_status()
    data = resp.json()
    inserts = []
    # remove -eng from local-authority ids until digital  land db catches up
    for org in data:
        inserts.append(
            {
                "name": org["name"],
                "organisation": org["organisation"].replace("-eng", ""),
            }
        )

    stmt = insert(Organisation).values(inserts)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Organisation.organisation], set_=dict(name=stmt.excluded.name)
    )

    db.session.execute(stmt)
    db.session.commit()
    print("load organisations done")
