import dataclasses


@dataclasses.dataclass
class BigCommercePart:
    brand_id: int
    product_title: str
    sku: str
    mpn: str
    default_price: float
    weight: float
    description: str
    images: list
    inventory: int
    custom_fields: list

