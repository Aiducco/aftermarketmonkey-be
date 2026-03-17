"""
Audit helpers for part-related API requests (search, detail view).
Enables tracking request volume per company/user.
"""
from src import models as src_models


def record_part_request(
    company_id: int,
    user_id: int | None,
    action: str,
    search_query: str | None = None,
    master_part_id: int | None = None,
) -> None:
    """
    Record a part request for audit (e.g. parts search or part detail view).
    Call from API views after a successful response.
    """
    try:
        company = src_models.Company.objects.get(id=company_id)
        user = None
        if user_id:
            from django.contrib.auth import get_user_model
            User = get_user_model()
            user = User.objects.filter(id=user_id).first()
        src_models.PartRequestAudit.objects.create(
            company=company,
            user=user,
            action=action,
            search_query=search_query,
            master_part_id=master_part_id,
        )
    except Exception:
        # Do not fail the request if audit write fails (e.g. log and ignore)
        pass
