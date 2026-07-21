import enum

class CompanyStatus(enum.Enum):
    ACTIVE = 1
    INACTIVE = 2


class IntegrationDestinationType(enum.Enum):
    BIGCOMMERCE = 1

class IntegrationDestinationStatus(enum.Enum):
    ACTIVE = 1
    INACTIVE = 2

class BrandProvider(enum.Enum):
    CATALOG = 1
    DISTRIBUTOR = 2

class BrandProviderKind(enum.Enum):
    TURN_14 = 1
    SDC = 2
    KEYSTONE = 3
    ROUGH_COUNTRY = 4
    WHEELPROS = 5
    MEYER = 6
    ATECH = 7
    DLG = 8
    # Coming soon distributors
    ATD = 9
    ALLPRO_DISTRIBUTING = 10
    AUTOMATIC_DISTRIBUTORS = 11
    CTP_DISTRIBUTORS = 12
    CROWN_AUTOMOTIVE = 13
    DIX_PERF_NORTH = 14
    EARL_OWEN = 15
    ELITE_WHEEL = 16
    FASTCO = 17
    GRANDWEST_ENTERPRISES = 18
    HELMHOUSE = 19
    HOLLEY_PERFORMANCE = 20
    THIBAULT = 21
    MARCOR = 22
    MOTOR_STATE_DISTRIBUTING = 23
    OVERLAND_VEHICLE_SYSTEMS = 24
    PARTS_AUTHORITY = 25
    PARTS_CANADA = 26
    PARTS_UNLIMITED = 27
    PREMIER_PERFORMANCE = 28
    SSF_IMPORTED_AUTO_PARTS = 29
    THE_WHEEL_GROUP = 30
    THIBERT = 31
    WESTERN_POWER_SPORTS = 32
    XDP = 33
    ASAP_NETWORK = 34

class BrandProviderStatus(enum.Enum):
    ACTIVE = 1
    INACTIVE = 2

class CompanyBrandStatus(enum.Enum):
    ACTIVE = 1
    INACTIVE = 2

class DestinationExecutionRunStatus(enum.Enum):
    STARTED = 1
    COMPLETED = 2
    FAILED = 3


class ScheduledTaskExecutionStatus(enum.Enum):
    """Status for scheduled task / cron execution audit records."""
    STARTED = 1
    COMPLETED = 2
    FAILED = 3
    SKIPPED = 4  # e.g. early exit when no work to do


class IntegrationPricingSyncJobStatus(enum.Enum):
    """Queue status for per-company-provider pricing sync jobs (cron-processed)."""
    OPEN = 1
    RUNNING = 2
    COMPLETED = 3
    FAILED = 4


class CompanyProviderConnectionStatus(enum.Enum):
    """
    Live connectivity/sync status for a CompanyProviders connection. Refreshed by the
    check_company_provider_connections cron for rows where initial_sync_completed is False;
    set to CONNECTED directly by integration_pricing_sync_jobs once that flips True.
    """
    CONNECTED = 1  # credentials valid and the initial pricing sync has completed
    INGESTING = 2  # credentials valid (or relay file received); initial sync not finished yet
    WAITING = 3    # credentials valid, but relay-provisioned and the distributor's file hasn't arrived yet
    FAILING = 4    # credentials/connectivity check failed


class CompanyProviderOrderConnectionStatus(enum.Enum):
    """
    Order-placement connectivity status for a CompanyProviders connection — independent of
    (and always a separate check from) the feed status above. Ordering is meaningless without
    a live feed, so CONNECTED requires both valid order credentials AND the feed itself being
    CompanyProviderConnectionStatus.CONNECTED; WAITING models "order credentials check out fine,
    but the feed hasn't finished its initial sync yet". Refreshed at connect/update time and by
    the check_company_provider_connections cron (independently of feed initial_sync_completed,
    since order credentials can go stale on their own — e.g. a rotated API key).
    """
    CONNECTED = 1  # order credentials valid AND feed status is CONNECTED
    WAITING = 2    # order credentials valid, but feed status isn't CONNECTED yet
    ERROR = 3      # order credentials invalid, or the order connectivity check failed


class NotificationEmailType(enum.Enum):
    """Kind of transactional notification email logged in NotificationEmailLog."""
    FIRST_SYNC_COMPLETED = 1


class NotificationEmailStatus(enum.Enum):
    SENT = 1
    FAILED = 2


class PurchaseOrderStatus(enum.Enum):
    """
    Lifecycle status for an internal PurchaseOrder. DRAFT doubles as the per-distributor
    "Add to PO" cart (see purchase_order_jobs / cart API) until a staff user reviews and
    quotes it — it is not just a pre-submission state.
    """
    DRAFT = 1
    QUOTED = 2
    SUBMITTING = 3
    SUBMITTED = 4
    CONFIRMED = 5
    PARTIALLY_FULFILLED = 6
    FULFILLED = 7
    CANCELLED = 8
    FAILED = 9


class PurchaseOrderLineItemStatus(enum.Enum):
    PENDING = 1
    CONFIRMED = 2
    BACKORDERED = 3
    REJECTED = 4
    PARTIALLY_SHIPPED = 5
    SHIPPED = 6
    CANCELLED = 7


class PurchaseOrderSource(enum.Enum):
    """Where a PurchaseOrder originated. Only STAFF_MANUAL is used today; the others are
    reserved for a future shop-management-system webhook / public API integration."""
    STAFF_MANUAL = 1
    SMS_WEBHOOK = 2
    API = 3


class PurchaseOrderOperation(enum.Enum):
    """Which DistributorOrderAdapter call a PurchaseOrderJob / PurchaseOrderSubmissionAttempt performs."""
    QUOTE = 1
    SUBMIT = 2
    STATUS_CHECK = 3
    CANCEL = 4


class PurchaseOrderJobStatus(enum.Enum):
    """Queue status for PurchaseOrderJob rows (cron-processed), same shape as
    IntegrationPricingSyncJobStatus."""
    OPEN = 1
    RUNNING = 2
    COMPLETED = 3
    FAILED = 4


class PurchaseOrderDistributorOrderStatus(enum.Enum):
    """Status of one distributor-side order slice (PurchaseOrderDistributorOrder)."""
    SUBMITTED = 1
    CONFIRMED = 2
    PARTIALLY_SHIPPED = 3
    SHIPPED = 4
    CANCELLED = 5