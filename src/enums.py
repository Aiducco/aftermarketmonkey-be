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

class BrandProviderStatus(enum.Enum):
    ACTIVE = 1
    INACTIVE = 2

class CompanyBrandStatus(enum.Enum):
    ACTIVE = 1
    INACTIVE = 2