from marshmallow import Schema, fields, validate


class UpdateProfileSchema(Schema):
    first_name = fields.String(required=False, validate=validate.Length(min=1, max=150))
    last_name = fields.String(required=False, validate=validate.Length(min=1, max=150))
    email = fields.Email(required=False)


class UpdateCompanySettingsSchema(Schema):
    name = fields.String(required=False, validate=validate.Length(max=255))
    business_type = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))
    country = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))
    state_province = fields.String(required=False, allow_none=True, validate=validate.Length(max=128))
    tax_id = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))


class AddCompanyUserSchema(Schema):
    email = fields.Email(required=True)
    first_name = fields.String(required=True, validate=validate.Length(min=1, max=150))
    last_name = fields.String(required=True, validate=validate.Length(min=1, max=150))
    password = fields.String(required=True, validate=validate.Length(min=8))
    is_company_admin = fields.Boolean(required=False, load_default=False)


class UpdateCompanyUserRoleSchema(Schema):
    is_company_admin = fields.Boolean(required=True)
