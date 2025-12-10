import enum

class CompanyStatus(enum.Enum):
    ACTIVE = 1
    INACTIVE = 2


class IntegrationDestinationType(enum.Enum):
    ECOMMERCE = 1

class IntegrationDestination(enum.Enum):
    BIGCOMMERCE = 1