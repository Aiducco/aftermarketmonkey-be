from django.contrib import admin
from django.urls import path, include
from src.api.views.authentication import LoginView, ChangePasswordView
from src.api.views.company import CompanyDestinationsView

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
]