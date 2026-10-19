from marshmallow import Schema, fields, validate


class CreatePortalSessionSchema(Schema):
    return_url = fields.Url(required=False, allow_none=True)


class CreateCheckoutSessionSchema(Schema):
    # "scout" is free — no checkout session for it.
    plan_id = fields.String(required=True, validate=validate.OneOf(["tracker", "hunter", "pack"]))
    billing_period = fields.String(required=True, validate=validate.OneOf(["monthly", "annual"]))
    # Only meaningful for plan_id="pack" (sold per-location); the service layer clamps this to 1
    # for every other plan rather than rejecting a client that always sends quantity=1.
    quantity = fields.Integer(required=False, load_default=1, validate=validate.Range(min=1, max=20))
    success_url = fields.Url(required=True)
    cancel_url = fields.Url(required=True)


class SwitchTrialPlanSchema(Schema):
    # Same choices as checkout -- "scout" has no comp'd-trial state to switch to, it's just the
    # fallback when subscription_plan is unset.
    plan_id = fields.String(required=True, validate=validate.OneOf(["tracker", "hunter", "pack"]))
