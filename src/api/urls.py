from django.contrib import admin
from django.urls import path, include
from src.api.views.authentication import LoginView, ChangePasswordView

urlpatterns = [
    path("auth/login/", LoginView.as_view(), name="login"),
    path("auth/change-password/", ChangePasswordView.as_view(), name="change_password"),
]