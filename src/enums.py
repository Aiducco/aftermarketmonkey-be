import enum

class CompanyStatus(enum.Enum):
    ACTIVE = 1
    INACTIVE = 2


class IntegrationDestinationType(enum.Enum):
    ECOMMERCE = 1

class IntegrationDestination(enum.Enum):
    BIGCOMMERCE = 1

class BrandProvider(enum.Enum):
    CATALOG = 1
    DISTRIBUTOR = 2