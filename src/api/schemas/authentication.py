from marshmallow import Schema, fields


class LoginSchema(Schema):
    email = fields.String(required=True)
    password = fields.String(required=True)


class ChangePasswordSchema(Schema):
    current_password = fields.String(required=True)
    new_password = fields.String(required=True)

class CreateUserSchema(Schema):
    email = fields.Email(required=True)
    password = fields.String(required=True)
    first_name = fields.String(required=True)
    last_name = fields.String(required=True)