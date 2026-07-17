from marshmallow import Schema, fields, validate


class CreateCompanyLocationSchema(Schema):
    label = fields.String(required=True, validate=validate.Length(min=1, max=100))
    name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    attention = fields.String(required=False, allow_none=True, load_default=None, validate=validate.Length(max=255))
    address1 = fields.String(required=True, validate=validate.Length(min=1, max=255))
    address2 = fields.String(required=False, allow_none=True, load_default=None, validate=validate.Length(max=255))
    city = fields.String(required=True, validate=validate.Length(min=1, max=128))
    state = fields.String(required=True, validate=validate.Length(min=1, max=64))
    postal_code = fields.String(required=True, validate=validate.Length(min=1, max=32))
    country = fields.String(required=True, validate=validate.Length(min=1, max=64))
    phone = fields.String(required=False, allow_none=True, load_default=None, validate=validate.Length(max=32))
    is_primary = fields.Boolean(required=False, load_default=False)


class UpdateCompanyLocationSchema(Schema):
    label = fields.String(required=False, validate=validate.Length(min=1, max=100))
    name = fields.String(required=False, validate=validate.Length(min=1, max=255))
    attention = fields.String(required=False, allow_none=True, validate=validate.Length(max=255))
    address1 = fields.String(required=False, validate=validate.Length(min=1, max=255))
    address2 = fields.String(required=False, allow_none=True, validate=validate.Length(max=255))
    city = fields.String(required=False, validate=validate.Length(min=1, max=128))
    state = fields.String(required=False, validate=validate.Length(min=1, max=64))
    postal_code = fields.String(required=False, validate=validate.Length(min=1, max=32))
    country = fields.String(required=False, validate=validate.Length(min=1, max=64))
    phone = fields.String(required=False, allow_none=True, validate=validate.Length(max=32))
    is_primary = fields.Boolean(required=False)
