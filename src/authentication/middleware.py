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
        
        # Log all requests to protected endpoints (those starting with /api/auth/)
        if request.path.startswith("/api/auth/"):
            logger.info(
                f"{_LOG_PREFIX} Processing request: {request.method} {request.path}"
            )
            if auth_header:
                logger.info(
                    f"{_LOG_PREFIX} Authorization header present: {auth_header[:20]}..."
                )
            else:
                logger.warning(
                    f"{_LOG_PREFIX} No Authorization header found for {request.method} {request.path}"
                )

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
                    logger.info(
                        f"{_LOG_PREFIX} Authenticated user (id={user_id}, email={user.email}) for {request.path}"
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


# A suspended member must still be able to sign in, change their password, read the plan status
# (the frontend renders its "account needs upgrade" screen off plan-status's ``current_user.state``),
# and complete onboarding; Stripe's webhook is anonymous anyway but is listed so it can never be
# caught by a future change to how request.user is set.
_SUSPENDED_ALLOWED_PATHS = (
    "/api/auth/login/",
    "/api/auth/change-password/",
    "/api/account/plan-status/",
    "/api/billing/webhook/",
)
_SUSPENDED_ALLOWED_PREFIXES = ("/api/onboarding/",)


class SuspendedMemberMiddleware:
    """
    Plan-limit enforcement for members: a UserProfile with ``suspended_at`` set (see
    src.api.services.plan_lifecycle) can log in but every other /api route answers 403
    ``account_needs_upgrade``.

    A middleware rather than a per-view check because there is no shared auth decorator -- every
    view re-implements its own _auth_check -- so this is the only choke point all requests pass.
    It reads the suspension from the DB on every authenticated request, never from the JWT: the
    token lives 12 hours and would keep a freshly suspended member in (or a restored one out).
    No cache, on purpose: a cache outage must not decide who can use the product, and this is one
    indexed lookup on a one-to-one row.

    The status is 403, not 401 (the frontend logs the user out on 401) and not 402 (it opens the
    global upgrade dialog on 402, which a non-admin member cannot act on).
    Must sit after JWTAuthenticationMiddleware, which is what sets request.user.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        path = request.path
        if path.startswith("/api/") and not self._is_allowed(path):
            user = getattr(request, "user", None)
            if user is not None and user.is_authenticated and not user.is_staff:
                # Imported here, not at module load: this module is imported while Django is
                # still assembling MIDDLEWARE, before the app registry is guaranteed ready.
                from src.models import UserProfile

                if UserProfile.objects.filter(user_id=user.id, suspended_at__isnull=False).exists():
                    return JsonResponse(
                        {
                            "message": "Your account needs an upgrade to continue. Ask your admin to upgrade the plan.",
                            "error_code": "account_needs_upgrade",
                            "upgrade_required": True,
                        },
                        status=403,
                    )
        return self.get_response(request)

    @staticmethod
    def _is_allowed(path: str) -> bool:
        return path in _SUSPENDED_ALLOWED_PATHS or path.startswith(_SUSPENDED_ALLOWED_PREFIXES)
