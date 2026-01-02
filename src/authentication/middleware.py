import logging
import jwt
from django.http import JsonResponse
from django.contrib.auth import get_user_model

from src.authentication.services import decode_jwt_token
from src.authentication.exceptions import InvalidJWTTokenError

User = get_user_model()
logger = logging.getLogger(__name__)
_LOG_PREFIX = "[JWT-AUTH-MIDDLEWARE]"


class JWTAuthenticationMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        auth_header = request.headers.get("Authorization")

        if auth_header:
            try:
                # Expected format: "Bearer <token>"
                if not auth_header.startswith("Bearer "):
                    logger.warning(
                        f"{_LOG_PREFIX} Authorization header does not start with 'Bearer '"
                    )
                    return JsonResponse(
                        {"error": "Invalid authorization header format. Expected 'Bearer <token>'"},
                        status=401
                    )
                
                token = auth_header.split()[1]

                decoded_data = decode_jwt_token(token)

                request.company_id = decoded_data.get("company_id")
                user_id = decoded_data.get("user_id")

                if not user_id:
                    logger.warning(
                        f"{_LOG_PREFIX} Token does not contain user_id"
                    )
                    return JsonResponse(
                        {"error": "Invalid token: missing user_id"},
                        status=401
                    )

                try:
                    user = User.objects.get(id=user_id)
                    request.user = user
                    logger.debug(
                        f"{_LOG_PREFIX} Authenticated user (id={user_id}, email={user.email})"
                    )
                except User.DoesNotExist:
                    logger.warning(
                        f"{_LOG_PREFIX} User (id={user_id}) does not exist"
                    )
                    return JsonResponse(
                        {"error": "User does not exist"},
                        status=404
                    )

            except IndexError:
                logger.warning(
                    f"{_LOG_PREFIX} Authorization header format invalid: {auth_header}"
                )
                return JsonResponse(
                    {"error": "Invalid authorization header format"},
                    status=401
                )
            except (InvalidJWTTokenError, jwt.PyJWTError) as e:
                logger.warning(
                    f"{_LOG_PREFIX} JWT token validation failed: {str(e)}"
                )
                return JsonResponse(
                    {"error": "Invalid or expired token"},
                    status=401
                )
            except Exception as e:
                logger.exception(
                    f"{_LOG_PREFIX} Unexpected error during JWT authentication: {str(e)}"
                )
                return JsonResponse(
                    {"error": "Authentication error"},
                    status=401
                )

        return self.get_response(request)
