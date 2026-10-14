"""
Enable or disable a per-company feature flag (CompanyFeatureFlags) -- see
src/api/services/feature_flags.py for the FEATURE_* names and company_has_feature().

    manage.py set_company_feature --company-id 16 --feature shop_management
    manage.py set_company_feature --company-id 16 --feature shop_management --disable
"""
import typing

from django.core.management.base import BaseCommand, CommandError

from src import models as src_models
from src.api.services import feature_flags as feature_flags_services


class Command(BaseCommand):
    help = "Enable (default) or disable a CompanyFeatureFlags row for one company."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--company-id", type=int, required=True)
        parser.add_argument("--feature", type=str, required=True, help="e.g. shop_management")
        parser.add_argument("--disable", action="store_true", help="Disable instead of enable.")

    def handle(self, *args: typing.Any, **options: typing.Any) -> None:
        company_id = options["company_id"]
        feature = options["feature"]

        company = src_models.Company.objects.filter(id=company_id).first()
        if not company:
            raise CommandError("No company with id={}.".format(company_id))

        if options["disable"]:
            feature_flags_services.disable_company_feature(company_id, feature)
            self.stdout.write(self.style.SUCCESS(
                "Disabled feature {!r} for company_id={} ({}).".format(feature, company_id, company.name)
            ))
        else:
            feature_flags_services.enable_company_feature(company_id, feature)
            self.stdout.write(self.style.SUCCESS(
                "Enabled feature {!r} for company_id={} ({}).".format(feature, company_id, company.name)
            ))
