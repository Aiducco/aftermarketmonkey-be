from marshmallow import Schema, fields, validate


class CreateUserSchema(Schema):
    email = fields.Email(required=True)
    password = fields.String(required=True)
    first_name = fields.String(required=True)
    last_name = fields.String(required=True)
    company_id = fields.Integer(required=False)


class UpdateUserSchema(Schema):
    email = fields.Email(required=True)
    first_name = fields.String(required=True)
    last_name = fields.String(required=True)
    company_id = fields.Integer(required=False)


class KeptIntegrationsSchema(Schema):
    # CompanyProviders ids the admin wants to keep active on Scout. Whether they belong to the
    # company, are feed-connected, and fit the Scout cap is checked in plan_lifecycle, which owns
    # those rules and their error codes; the schema only bounds the payload.
    ids = fields.List(fields.Integer(), required=True, validate=validate.Length(min=1, max=50))
