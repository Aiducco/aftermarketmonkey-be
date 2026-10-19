from marshmallow import Schema, fields, validate


class AdminCreateCompanySchema(Schema):
    company_name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    email = fields.Email(required=True)
    first_name = fields.String(required=True, validate=validate.Length(min=1, max=150))
    last_name = fields.String(required=True, validate=validate.Length(min=1, max=150))
    # Optional: admin can set one explicitly, otherwise a secure one is generated and returned
    # in the response so staff can relay it to the customer.
    password = fields.String(required=False, allow_none=True, validate=validate.Length(min=8))
    trial_days = fields.Integer(required=False, allow_none=True, validate=validate.Range(min=1, max=365))
    subscription_plan = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))


class AdminResetUserPasswordSchema(Schema):
    # Optional for the same reason as AdminCreateCompanySchema.password above.
    new_password = fields.String(required=False, allow_none=True, validate=validate.Length(min=8))


class AdminExtendTrialSchema(Schema):
    days = fields.Integer(required=True, validate=validate.Range(min=1, max=365))
    plan = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))
