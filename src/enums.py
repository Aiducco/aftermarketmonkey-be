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