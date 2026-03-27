from marshmallow import Schema, fields, validate


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
    # Company (required for atomic creation)
    company_name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    business_type = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))
    country = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))
    state_province = fields.String(required=False, allow_none=True, validate=validate.Length(max=128))
    tax_id = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))


class CompanyDetailsSchema(Schema):
    """Step 2: Company details & verification."""
    company_name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    business_type = fields.String(
        required=False,
        allow_none=True,
        validate=validate.OneOf(
            [
                "retail_store",
                "installation_repair_shop",
                "ecommerce",
                "dealership",
                "fleet_manager",
            ]
        ),
    )
    country = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))
    state_province = fields.String(required=False, allow_none=True, validate=validate.Length(max=128))
    tax_id = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))


class CompanyDetailsSchemaAllowAny(Schema):
    """Step 2: Allow any business_type string for flexibility."""
    company_name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    business_type = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))
    country = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))
    state_province = fields.String(required=False, allow_none=True, validate=validate.Length(max=128))
    tax_id = fields.String(required=False, allow_none=True, validate=validate.Length(max=64))


class DistributorCredentialsSchema(Schema):
    """Turn14 credentials."""
    client_id = fields.String(required=True)
    client_secret = fields.String(required=True)


class KeystoneCredentialsSchema(Schema):
    """Keystone credentials."""
    ftp_user = fields.String(required=True)
    ftp_password = fields.String(required=True)


class MeyerCredentialsSchema(Schema):
    """Meyer Distributing SFTP credentials."""
    sftp_user = fields.String(required=True)
    sftp_password = fields.String(required=True)
    sftp_server = fields.String(required=False, allow_none=True)
    sftp_directory = fields.String(required=False, allow_none=True)
    pricing_remote_file = fields.String(required=False, allow_none=True)
    inventory_remote_file = fields.String(required=False, allow_none=True)


class PersonalizationSchema(Schema):
    """Step 3: Tool personalization."""
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
    # Optional: credentials per distributor. Keys: turn_14, keystone, meyer
    distributor_credentials = fields.Dict(
        required=False,
        allow_none=True,
        load_default=dict,
    )
