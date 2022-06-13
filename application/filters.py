def get_items_beginning_with(mylist, letter):
    items = [item for item in mylist if item.name.startswith(letter)]
    return items
