from marshmallow import Schema, ValidationError, fields, validate, validates_schema


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


class AdminKeptIntegrationsSchema(Schema):
    # CompanyProviders ids. Both keys are optional but at least one is required: ``ids`` sets the
    # kept distributors, ``unlock`` reopens the customer's one post-expiry change.
    ids = fields.List(fields.Integer(), required=False, validate=validate.Length(min=1, max=50))
    unlock = fields.Boolean(required=False)

    @validates_schema
    def _require_ids_or_unlock(self, data, **kwargs):
        if "ids" not in data and not data.get("unlock"):
            raise ValidationError("Provide ids and/or unlock.")


class AdminSpaceAllowanceSchema(Schema):
    # The staff base for the company's space allowance: an integer >= 1, or null to clear it. Required so
    # an empty body cannot silently clear the base. The upper bound is the column's (smallint).
    space_base = fields.Integer(required=True, allow_none=True, strict=True, validate=validate.Range(min=1, max=32767))
