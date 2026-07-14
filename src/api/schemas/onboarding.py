from marshmallow import Schema, fields, validate

USER_ROLE_CHOICES = [
    "owner",
    "parts_manager",
    "service_advisor",
    "technician",
    "other",
]


class RegisterSchema(Schema):
    """
    Step 1+2 (atomic): Account + company in one request.
    No partial state - user is never created without company details.
    """
    # Account
    first_name = fields.String(required=True, validate=validate.Length(min=1, max=150))
    last_name = fields.String(required=True, validate=validate.Length(min=1, max=150))
    email = fields.Email(required=True)
    password = fields.String(required=True, validate=validate.Length(min=8))
    role = fields.String(required=False, allow_none=True, validate=validate.OneOf(USER_ROLE_CHOICES))
    # Company (required for atomic creation)
    company_name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    business_type = fields.List(fields.String(validate=validate.Length(max=64)), required=False, load_default=list)
    country = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))
    state_province = fields.String(required=False, allow_none=True, validate=validate.Length(max=128))
    city = fields.String(required=False, allow_none=True, validate=validate.Length(max=128))
    postal_code = fields.String(required=False, allow_none=True, validate=validate.Length(max=32))
    tax_id = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))


class CompanyDetailsSchema(Schema):
    """Step 2: Company details & verification."""
    company_name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    business_type = fields.List(
        fields.String(
            validate=validate.OneOf(
                [
                    "retail_store",
                    "installation_repair_shop",
                    "ecommerce",
                    "dealership",
                    "fleet_manager",
                ]
            ),
        ),
        required=False,
        load_default=list,
    )
    country = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))
    state_province = fields.String(required=False, allow_none=True, validate=validate.Length(max=128))
    city = fields.String(required=False, allow_none=True, validate=validate.Length(max=128))
    postal_code = fields.String(required=False, allow_none=True, validate=validate.Length(max=32))
    tax_id = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))


class CompanyDetailsSchemaAllowAny(Schema):
    """Step 2: Allow any business_type strings for flexibility."""
    company_name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    business_type = fields.List(fields.String(validate=validate.Length(max=64)), required=False, load_default=list)
    country = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))
    state_province = fields.String(required=False, allow_none=True, validate=validate.Length(max=128))
    city = fields.String(required=False, allow_none=True, validate=validate.Length(max=128))
    postal_code = fields.String(required=False, allow_none=True, validate=validate.Length(max=32))
    tax_id = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))


class PersonalizationSchema(Schema):
    """
    Step 3: Personalization preferences only — which distributors/categories the
    company is interested in. No credentials here; connecting real distributor
    credentials to unlock pricing/inventory happens later via the integrations flow
    (POST /integrations/catalog/<id>/connect/), not during onboarding.
    """
    preferred_distributor_ids = fields.List(
        fields.Integer(),
        required=False,
        load_default=list,
    )
    top_categories = fields.List(
        fields.String(),
        required=False,
        load_default=list,
    )
