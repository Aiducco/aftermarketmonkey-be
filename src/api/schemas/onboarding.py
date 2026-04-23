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
    """Meyer relay SFTP — credentials from info@aftermarketmonkey.com; host/path/files from settings."""

    sftp_user = fields.String(required=True)
    sftp_password = fields.String(required=True)


class AtechCredentialsSchema(Schema):
    """A-Tech relay SFTP — credentials from info@aftermarketmonkey.com; combined catalog, multi-DC inventory, and pricing feed (remote name configurable via settings or credentials)."""

    sftp_user = fields.String(required=True)
    sftp_password = fields.String(required=True)


class DlgCredentialsSchema(Schema):
    """DLG: dealer address that receives DLG’s inventory email (ingest from relay uses app settings, not this row)."""

    email_from = fields.String(required=True)


class WheelProsCredentialsSchema(Schema):
    """Wheel Pros SFTP: user/password; optional path and per-feed % off MSRP for dealer cost."""

    sftp_user = fields.String(required=True)
    sftp_password = fields.String(required=True)
    sftp_path = fields.String(required=False, allow_none=True)
    wheel_markup = fields.String(required=False, allow_none=True)
    tire_markup = fields.String(required=False, allow_none=True)
    accessories_markup = fields.String(required=False, allow_none=True)


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
    # Optional: credentials per distributor. Keys: turn_14, keystone, meyer, atech, dlg, wheelpros
    distributor_credentials = fields.Dict(
        required=False,
        allow_none=True,
        load_default=dict,
    )
