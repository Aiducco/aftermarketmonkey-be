from marshmallow import Schema, fields, validate


class CreateTicketSchema(Schema):
    subject = fields.String(required=True, validate=validate.Length(min=1, max=100))
    message = fields.String(required=True, validate=validate.Length(min=1))
