import logging
import typing

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = '[INTEGRATIONS-SERVICES]'


def get_company_providers(company_id: int) -> typing.List[typing.Dict]:
    """
    Get company providers for a given company_id.
    Left joins with Providers to get provider details.
    
    Args:
        company_id: The ID of the company
        
    Returns:
        List of dictionaries containing company provider data with provider details
    """
    logger.info('{} Fetching company providers for company_id: {}.'.format(
        _LOG_PREFIX, company_id
    ))
    
    company_providers = src_models.CompanyProviders.objects.filter(
        company_id=company_id
    ).select_related('provider').all()
    
    data = []
    for cp in company_providers:
        provider = cp.provider
        data.append({
            "id": cp.id,
            "company_id": cp.company_id,
            "provider_id": cp.provider_id,
            "provider_name": provider.name if provider else None,
            "provider_status": provider.status if provider else None,
            "provider_status_name": provider.status_name if provider else None,
            "provider_type": provider.type if provider else None,
            "provider_type_name": provider.type_name if provider else None,
            "provider_kind": provider.kind if provider else None,
            "provider_kind_name": provider.kind_name if provider else None,
            "credentials": cp.credentials,
            "primary": cp.primary,
            "created_at": cp.created_at.isoformat() if cp.created_at else None,
            "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
        })
    
    logger.info('{} Found {} company providers for company_id: {}.'.format(
        _LOG_PREFIX, len(data), company_id
    ))
    
    return data


def get_company_provider_by_id(company_id: int, provider_id: int) -> typing.Optional[typing.Dict]:
    """
    Get a single company provider by ID for a given company_id.
    Left joins with Providers to get provider details.
    
    Args:
        company_id: The ID of the company
        provider_id: The ID of the company provider
        
    Returns:
        Dictionary containing company provider data with provider details, or None if not found
    """
    logger.info('{} Fetching company provider with id: {} for company_id: {}.'.format(
        _LOG_PREFIX, provider_id, company_id
    ))
    
    try:
        company_provider = src_models.CompanyProviders.objects.filter(
            id=provider_id,
            company_id=company_id
        ).select_related('provider').first()
        
        if not company_provider:
            logger.warning('{} Company provider with id: {} not found for company_id: {}.'.format(
                _LOG_PREFIX, provider_id, company_id
            ))
            return None
        
        provider = company_provider.provider
        data = {
            "id": company_provider.id,
            "company_id": company_provider.company_id,
            "provider_id": company_provider.provider_id,
            "provider_name": provider.name if provider else None,
            "provider_status": provider.status if provider else None,
            "provider_status_name": provider.status_name if provider else None,
            "provider_type": provider.type if provider else None,
            "provider_type_name": provider.type_name if provider else None,
            "provider_kind": provider.kind if provider else None,
            "provider_kind_name": provider.kind_name if provider else None,
            "credentials": company_provider.credentials,
            "primary": company_provider.primary,
            "created_at": company_provider.created_at.isoformat() if company_provider.created_at else None,
            "updated_at": company_provider.updated_at.isoformat() if company_provider.updated_at else None,
        }
        
        logger.info('{} Found company provider with id: {} for company_id: {}.'.format(
            _LOG_PREFIX, provider_id, company_id
        ))
        
        return data
    except Exception as e:
        logger.error('{} Error fetching company provider with id: {} for company_id: {}. Error: {}.'.format(
            _LOG_PREFIX, provider_id, company_id, str(e)
        ))
        raise

