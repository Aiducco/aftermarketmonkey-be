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