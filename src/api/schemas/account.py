from marshmallow import Schema, fields


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
