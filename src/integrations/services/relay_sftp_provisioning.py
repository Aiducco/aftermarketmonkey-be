import logging
import re
import secrets
import string
import typing

import paramiko
from django.conf import settings
from django.utils import timezone

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[RELAY-SFTP-PROVISIONING]"

_USERNAME_DISALLOWED_RE = re.compile(r"[^a-z0-9]+")
_PASSWORD_ALPHABET = string.ascii_letters + string.digits


def _slugify_username(company: src_models.Company) -> str:
    base = _USERNAME_DISALLOWED_RE.sub("", (company.slug or company.name or "").lower())[:20]
    base = base or "company"
    return "{}{}".format(base, company.id)


def _generate_password(length: int = 20) -> str:
    return "".join(secrets.choice(_PASSWORD_ALPHABET) for _ in range(length))


def _ssh_client() -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        settings.RELAY_SFTP_SSH_HOST,
        port=settings.RELAY_SFTP_SSH_PORT,
        username=settings.RELAY_SFTP_SSH_USER,
        password=settings.RELAY_SFTP_SSH_PASSWORD,
        timeout=15,
    )
    return client


def _run(client: paramiko.SSHClient, command: str) -> str:
    _stdin, stdout, stderr = client.exec_command(command)
    exit_status = stdout.channel.recv_exit_status()
    if exit_status != 0:
        raise RuntimeError(
            "Relay SSH command failed ({}): {}\n{}".format(exit_status, command, stderr.read().decode().strip())
        )
    return stdout.read().decode()


def provision_company_sftp_account(company: src_models.Company) -> typing.Optional[str]:
    """
    Idempotently ensure a dedicated relay SFTP account exists for ``company``, matching the
    manually created "thor" account: a nologin Linux user in the ``sftpusers`` group, chrooted
    (via sshd's ``Match Group sftpusers`` block) to ``<RELAY_SFTP_BASE_DIR>/<username>``, with a
    root-owned jail directory and a company-owned ``uploads/`` subdirectory.

    No-op (returns None immediately) if the company already has an account — provisioning never
    resets an existing password, so distributor reps who were already given credentials keep
    working.

    Returns the generated username on success.
    """
    if company.relay_sftp_username and company.relay_sftp_password:
        return None

    username = _slugify_username(company)
    password = _generate_password()
    group = settings.RELAY_SFTP_GROUP
    jail_dir = "{}/{}".format(settings.RELAY_SFTP_BASE_DIR, username)
    uploads_dir = "{}/uploads".format(jail_dir)

    logger.info("{} Provisioning relay SFTP account for company_id={} username={}.".format(
        _LOG_PREFIX, company.id, username
    ))

    client = _ssh_client()
    try:
        user_exists = _run(client, "id -u {} 2>/dev/null || true".format(username)).strip() != ""
        if not user_exists:
            _run(client, "useradd -m -g {} -s /usr/sbin/nologin {}".format(group, username))
        _run(client, "echo '{}:{}' | chpasswd".format(username, password))
        _run(client, "mkdir -p {jail} && chown root:root {jail} && chmod 755 {jail}".format(jail=jail_dir))
        _run(
            client,
            "mkdir -p {uploads} && chown {user}:{group} {uploads} && chmod 775 {uploads}".format(
                uploads=uploads_dir, user=username, group=group,
            ),
        )
    finally:
        client.close()

    company.relay_sftp_username = username
    company.relay_sftp_password = password
    company.relay_sftp_provisioned_at = timezone.now()
    company.save(update_fields=["relay_sftp_username", "relay_sftp_password", "relay_sftp_provisioned_at"])

    logger.info("{} Provisioned relay SFTP account for company_id={} username={}.".format(
        _LOG_PREFIX, company.id, username
    ))

    return username
