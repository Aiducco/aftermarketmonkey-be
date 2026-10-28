"""
MotorStateLiveInventoryProvider -- on-demand single-part stock check, for the part-detail
refresh action. The daily FTP feed stays the bulk source.

Uses OrderInquiry rather than a stock endpoint, because Motor State has no stock endpoint:
their API offers no bulk product export and no per-part availability lookup that a dealer key
can reach. An inquiry for one part at quantity 1 answers the same question -- it is
non-committal, and its ``QuantityAvailable`` is the live figure -- so that is what this asks.

Credentials are the ORDER namespace's ``api_key``, like the order adapter and for the same
reason: the stock check IS the ordering API. A dealer with a working feed but no key gets a
ValueError here, which the registry turns into "no live check for this connection".

Motor State's stock is distributor-wide (one shared MotorStateProduct row, see
master_parts.sync_provider_inventory_from_motorstate) and they publish no per-warehouse
breakdown, so a refresh updates the shared row for every company and warehouse_availability
carries the status rather than a location split -- matching what the feed sync writes.
"""
import logging
import typing

from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.motorstate import exceptions as ms_exceptions
from src.integrations.clients.motorstate import order_client as ms_order_client
from src.integrations.live_inventory import base
from src.integrations.live_inventory import exceptions as live_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[MOTOR-STATE-LIVE-INVENTORY]"


def _int_or_zero(value: typing.Any) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


class MotorStateLiveInventoryProvider(base.LiveInventoryProvider):
    provider_kind = src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.LiveInventoryProvider.__init__(self, company_provider)
        self._client = ms_order_client.MotorStateOrderClient(
            credentials=credentials_helper.get_order_credentials(company_provider)
        )

    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        # provider_external_id is "{motorstate_brand_id}_{part_number}" (master_parts'
        # _motorstate_provider_external_id), not the part number itself -- the same compound
        # shape NTW and AllPro use. Matching on it directly finds nothing, every time.
        external_id = (provider_part.provider_external_id or "").strip()
        brand_id, _, part_number = external_id.partition("_")
        if not (brand_id.isdigit() and part_number):
            part_number = external_id
        if not part_number:
            raise live_exceptions.LiveInventoryNotFoundError(
                "ProviderPart {} has no Motor State part number.".format(provider_part.id)
            )
        product = src_models.MotorStateProduct.objects.filter(part_number=part_number).first()
        if not product:
            raise live_exceptions.LiveInventoryNotFoundError(
                "No MotorStateProduct row for {}.".format(part_number)
            )

        try:
            payment_code, last4 = self._client.default_payment_method()
            if not payment_code:
                raise live_exceptions.LiveInventoryTransportError(
                    "Motor State reports no payment method for this account; an inquiry cannot "
                    "be priced without one."
                )
            result = self._client.order_inquiry(
                [{"line_number": 1, "part_number": part_number, "quantity": 1}],
                payment_method_code=payment_code,
                credit_card_last4=last4,
                # Required even here: an inquiry without one is refused with "Purchase Order
                # Number has not been entered", despite creating nothing.
                purchase_order_number=ms_order_client.quote_reference("AMS-STOCK"),
            )
        except ms_exceptions.MotorStatePermissionError as e:
            logger.error("{} API key rejected: {}".format(
                _LOG_PREFIX, getattr(e, "message", "") or str(e)))
            raise live_exceptions.LiveInventoryTransportError(
                getattr(e, "message", "") or "Motor State rejected this API key."
            ) from e
        except ms_exceptions.MotorStateAPIException as e:
            message = getattr(e, "message", "") or str(e) or e.__class__.__name__
            logger.error("{} inquiry failed for {}: {}".format(_LOG_PREFIX, part_number, message))
            raise live_exceptions.LiveInventoryTransportError(message) from e

        match = None
        for line in (result or {}).get("Lines") or []:
            if str(line.get("PartNumber") or "").strip().upper() == part_number.upper():
                match = line
                break
        if match is None:
            # An inquiry echoes every requested line, so nothing back means Motor State does
            # not carry it for this account -- a real not-found, not a transport failure.
            raise live_exceptions.LiveInventoryNotFoundError(
                "Motor State returned no line for part {}.".format(part_number)
            )

        # QuantityAvailable is the live figure; QuantityShip is only what this 1-unit inquiry
        # would ship, so it must not be read as stock.
        quantity = _int_or_zero(match.get("QuantityAvailable"))
        return self._write(provider_part, product, quantity)

    def _write(
        self,
        provider_part: src_models.ProviderPart,
        product: src_models.MotorStateProduct,
        quantity: int,
    ) -> src_models.ProviderPartInventory:
        now = timezone.now()
        # Raw table first, so the next bulk sync reads back what the dealer was just shown.
        product.quantity = quantity
        product.save(update_fields=["quantity", "updated_at"])

        inventory, _ = src_models.ProviderPartInventory.objects.update_or_create(
            provider_part=provider_part,
            defaults={
                "warehouse_total_qty": quantity,
                "manufacturer_inventory": None,
                "manufacturer_esd": None,
                # None, matching the feed sync: Motor State publishes no per-warehouse split,
                # and the part-detail endpoint renders whatever is here as warehouse rows. An
                # earlier version of this wrote {"status": ...}, which both disagreed with the
                # feed's own shape AND dropped the quantity, so a refresh replaced a real number
                # with a phantom warehouse.
                "warehouse_availability": None,
                "last_synced_at": now,
            },
        )
        return inventory
