class AuthenticationServiceException(Exception):
    pass


class InvalidJWTTokenError(AuthenticationServiceException):
    pass


class InvalidLoginCredentialsError(AuthenticationServiceException):
    pass


class UserDoesNotExistError(AuthenticationServiceException):
    pass


class UserIncorrectPasswordError(AuthenticationServiceException):
    pass


class UserCurrentPasswordDoesNotMatchError(AuthenticationServiceException):
    pass


class UserNotActiveError(AuthenticationServiceException):
    pass
