from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth.models import User

from src.models import UserProfile, Company


@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    """
    Automatically create a UserProfile when a new User is created.
    """
    if created:
        company = getattr(instance, "_company_id", None)
        UserProfile.objects.create(
            user=instance,
            company=Company.objects.get(id=company) if company else None
        )
