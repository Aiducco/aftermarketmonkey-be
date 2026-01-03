from django.contrib import admin
from django.urls import path, include
from src.api.views.authentication import LoginView, ChangePasswordView
from src.api.views.company import CompanyDestinationsView
from src.api.views.integrations import CompanyProvidersView, CompanyProviderDetailView, BrandsWithProvidersView, CompanyDestinationsWithBrandsView, CompanyDestinationDetailView, ExecutionRunsView, ExecutionRunPartsHistoryView

urlpatterns = [
    path(
        "auth/login/",
        LoginView.as_view(),
        name="login"
    ),
    path(
        "auth/change-password/",
        ChangePasswordView.as_view(),
        name="change_password"
    ),
    path(
        "auth/company/destinations/",
        CompanyDestinationsView.as_view(),
        name="company_destinations",
    ),
    path(
        "company-providers/",
        CompanyProvidersView.as_view(),
        name="company_providers",
    ),
    path(
        "company-providers/<int:id>/",
        CompanyProviderDetailView.as_view(),
        name="company_provider_detail",
    ),
    path(
        "brands/",
        BrandsWithProvidersView.as_view(),
        name="brands_with_providers",
    ),
    path(
        "destinations/",
        CompanyDestinationsWithBrandsView.as_view(),
        name="company_destinations_with_brands",
    ),
    path(
        "destinations/<int:id>/",
        CompanyDestinationDetailView.as_view(),
        name="company_destination_detail",
    ),
    path(
        "execution-runs/",
        ExecutionRunsView.as_view(),
        name="execution_runs",
    ),
    path(
        "execution-runs/<int:destination_id>/",
        ExecutionRunsView.as_view(),
        name="execution_runs_by_destination",
    ),
    path(
        "execution-runs/<int:execution_run_id>/parts-history/",
        ExecutionRunPartsHistoryView.as_view(),
        name="execution_run_parts_history",
    ),
]