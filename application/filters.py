import json

import jsonpickle


def get_items_beginning_with(mylist, letter):
    items = [item for item in mylist if item.name.startswith(letter)]
    return items


def debug(thing):
    return f"<script>console.log({json.dumps(json.loads(jsonpickle.encode(thing)), indent=4)});</script>"


def short_date(date):
    if date is None:
        return date
    return date.strftime("%d %B %Y")
