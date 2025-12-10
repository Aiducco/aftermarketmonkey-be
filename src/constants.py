
ECOMMERCE_DESTINATION_PARTS_FIELDS = {
    "handle": {
        "type": "string",
        "required": True,
    },
    "title": {
        "type": "string",
        "required": True,
    },
    "part_number": {
        "type": "string",
        "required": True,
    },
    "sku": {
        "type": "string",
        "required": True,
    },
    "weight": {
        "type": "float",
        "required": False,
    },
    "weight_unit": {
        "type": "str",
        "required": False,
    },
    "listing_price": {
        "type": "float",
        "required": True,
    },
    "cost": {
        "type": "float",
        "required": True,
    },
    "categories": {
        "type": "list",
        "required": False,
    },
    "images": {
        "type": "list",
        "required": False,
    },
}