import dataclasses
import typing


@dataclasses.dataclass
class BigCommercePart:
    brand_id: int
    product_title: str
    sku: str
    mpn: str
    default_price: float
    cost: float
    msrp: float
    weight: float
    width: typing.Optional[float]
    height: typing.Optional[float]
    depth: typing.Optional[float]
    description: str
    images: list
    inventory: int
    custom_fields: list
    active: bool
    category: typing.Optional[str] = None
    subcategory: typing.Optional[str] = None
    fitments: typing.Optional[typing.List[typing.Dict[str, typing.Any]]] = None

