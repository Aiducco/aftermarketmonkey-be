from marshmallow import EXCLUDE, Schema, fields, validate


class SpaceAddressSchema(Schema):
    """A Location space's one address: the company-location fields without ``is_primary`` (a space's address
    is never an address-book entry). ``label`` is accepted but optional and ignored: the address row's label is
    always the space's name. Anything else sent (e.g. ``is_primary``) is ignored too, never rejected."""

    class Meta:
        unknown = EXCLUDE

    label = fields.String(required=False, allow_none=True, validate=validate.Length(max=255))
    name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    attention = fields.String(required=False, allow_none=True, load_default=None, validate=validate.Length(max=255))
    address1 = fields.String(required=True, validate=validate.Length(min=1, max=255))
    address2 = fields.String(required=False, allow_none=True, load_default=None, validate=validate.Length(max=255))
    city = fields.String(required=True, validate=validate.Length(min=1, max=128))
    state = fields.String(required=True, validate=validate.Length(min=1, max=64))
    postal_code = fields.String(required=True, validate=validate.Length(min=1, max=32))
    country = fields.String(required=True, validate=validate.Length(min=1, max=64))
    phone = fields.String(required=False, allow_none=True, load_default=None, validate=validate.Length(max=32))


class CreateSpaceSchema(Schema):
    kind = fields.String(required=True, validate=validate.OneOf(["location", "online_store"]))
    name = fields.String(required=True, validate=validate.Length(min=1, max=100))
    # Optional here so a missing prefix answers 400 prefix_required (a code the fe branches on), not a
    # generic invalid-payload error.
    prefix = fields.String(required=False, allow_none=True)
    address = fields.Nested(SpaceAddressSchema, required=False, allow_none=True)


class UpdateSpaceSchema(Schema):
    name = fields.String(required=False, validate=validate.Length(min=1, max=100))
    prefix = fields.String(required=False, allow_none=True)
    address = fields.Nested(SpaceAddressSchema, required=False, allow_none=True)
