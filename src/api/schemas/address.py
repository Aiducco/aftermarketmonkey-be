from marshmallow import Schema, fields, validate


class SuggestAddressSchema(Schema):
    # 3 characters is the frontend's own threshold for firing a request at all; enforcing it
    # here too means a hand-rolled client can't turn every single keystroke into a billed
    # provider call. 200 caps what we forward to the provider.
    q = fields.String(required=True, validate=validate.Length(min=3, max=200))
    # ISO 3166-1 alpha-2. Optional: the user may not have picked a country yet.
    country = fields.String(
        required=False, allow_none=True, load_default=None, validate=validate.Length(equal=2)
    )
    session = fields.UUID(required=True)


class ResolveAddressSchema(Schema):
    id = fields.String(required=True, validate=validate.Length(min=1, max=64))
    session = fields.UUID(required=True)


class AddressSchema(Schema):
    """
    The shared Address shape. Only ``line1`` and ``country`` are required -- validation is
    advisory, so a partially filled form should get an "unverified" answer rather than a 400
    the frontend has to treat as a special case. ``state`` is legitimately empty for countries
    that have no state codes.

    Lengths mirror PurchaseOrder.ship_to_* so anything that validates here also stores.
    """
    line1 = fields.String(required=True, validate=validate.Length(min=1, max=255))
    line2 = fields.String(
        required=False, allow_none=True, load_default="", validate=validate.Length(max=255)
    )
    city = fields.String(
        required=False, allow_none=True, load_default="", validate=validate.Length(max=128)
    )
    state = fields.String(
        required=False, allow_none=True, load_default="", validate=validate.Length(max=64)
    )
    postal_code = fields.String(
        required=False, allow_none=True, load_default="", validate=validate.Length(max=32)
    )
    country = fields.String(required=True, validate=validate.Length(equal=2))
