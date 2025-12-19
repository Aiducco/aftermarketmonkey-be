# Field priority configuration for merging CATALOG and DISTRIBUTOR parts
# Each field maps to its primary source (CATALOG or DISTRIBUTOR)
# If field is null/empty in primary source, fallback to the other source
# Fields not listed default to CATALOG priority

BIGCOMMERCE_PART_FIELD_PRIORITY = {
    'brand_id': 'CATALOG',
    'product_title': 'CATALOG',
    'sku': 'CATALOG',
    'mpn': 'CATALOG',
    'description': 'CATALOG',
    'images': 'CATALOG',
    'custom_fields': 'CATALOG',
    'active': 'CATALOG',
    'default_price': 'DISTRIBUTOR',
    'cost': 'DISTRIBUTOR',
    'msrp': 'DISTRIBUTOR',
    'weight': 'DISTRIBUTOR',
    'width': 'DISTRIBUTOR',
    'height': 'DISTRIBUTOR',
    'depth': 'DISTRIBUTOR',
    'inventory': 'DISTRIBUTOR',
}

