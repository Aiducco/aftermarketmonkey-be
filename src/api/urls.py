from django.contrib import admin
from django.urls import path, include
from src.api.views.authentication import LoginView, ChangePasswordView
from src.api.views.company import CompanyDestinationsView
from src.api.views.integrations import CompanyProvidersView, CompanyProviderDetailView

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
]