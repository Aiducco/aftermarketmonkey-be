import jwt
from django.http import JsonResponse
from django.contrib.auth import get_user_model

from src.authentication.services import decode_jwt_token
from src.authentication.exceptions import InvalidJWTTokenError

User = get_user_model()


class JWTAuthenticationMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        auth_header = request.headers.get("Authorization")

        if auth_header:
            try:
                # Expected format: "Bearer <token>"
                token = auth_header.split()[1]

                decoded_data = decode_jwt_token(token)
                user_id = decoded_data.get("user_id")

                try:
                    user = User.objects.get(id=user_id)
                    request.user = user
                except User.DoesNotExist:
                    return JsonResponse(
                        {"error": "User does not exist"},
                        status=404
                    )

            except (IndexError, InvalidJWTTokenError, jwt.ExpiredSignatureError):
                return JsonResponse(
                    {"error": "Invalid or expired token"},
                    status=401
                )

        return self.get_response(request)
