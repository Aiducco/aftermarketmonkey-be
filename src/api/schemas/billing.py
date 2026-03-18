from marshmallow import Schema, fields, validate


class CreatePortalSessionSchema(Schema):
    return_url = fields.Url(required=False, allow_none=True)


class CreateCheckoutSessionSchema(Schema):
    plan_id = fields.String(required=True, validate=validate.OneOf(["starter", "pro", "growth"]))
    success_url = fields.Url(required=True)
    cancel_url = fields.Url(required=True)
