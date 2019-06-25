from marshmallow import Schema, fields, post_load


class User():
    def __init__(self, id, firstname, lastname):
        self.id = id
        self.firstname = firstname
        self.lastname = lastname

    def __repr__(self):
        return f"User(id={self.id}, firstname={self.firstname}, lastname={self.lastname})"


class UserSchema(Schema):
    version = 1
    name = 'UserSchema'
    id = fields.Str(
        required=True,
    )
    firstname = fields.Str(
        required=True,
    )
    lastname = fields.Str(
        required=True,
    )
    created_date = fields.DateTime()
    updated_date = fields.DateTime()

    @post_load
    def make_user(self, data, **kwargs):
        return User(**data)
