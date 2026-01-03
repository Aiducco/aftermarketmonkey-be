import logging
import typing

from django.core.paginator import Paginator

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


def get_all_brands_with_providers() -> typing.List[typing.Dict]:
    """
    Get all brands with their associated providers.
    Left joins with BrandProviders and Providers to get provider details.
    
    Returns:
        List of dictionaries containing brand data with their providers
    """
    logger.info('{} Fetching all brands with providers.'.format(_LOG_PREFIX))
    
    brands = src_models.Brands.objects.prefetch_related(
        'providers__provider'
    ).all()
    
    data = []
    for brand in brands:
        providers_data = []
        for brand_provider in brand.providers.all():
            provider = brand_provider.provider
            providers_data.append({
                "id": provider.id if provider else None,
                "name": provider.name if provider else None,
                "status": provider.status if provider else None,
                "status_name": provider.status_name if provider else None,
                "type": provider.type if provider else None,
                "type_name": provider.type_name if provider else None,
                "kind": provider.kind if provider else None,
                "kind_name": provider.kind_name if provider else None,
                "created_at": brand_provider.created_at.isoformat() if brand_provider.created_at else None,
                "updated_at": brand_provider.updated_at.isoformat() if brand_provider.updated_at else None,
            })
        
        data.append({
            "id": brand.id,
            "name": brand.name,
            "status": brand.status,
            "status_name": brand.status_name,
            "data": brand.data,
            "providers": providers_data,
            "created_at": brand.created_at.isoformat() if brand.created_at else None,
            "updated_at": brand.updated_at.isoformat() if brand.updated_at else None,
        })
    
    logger.info('{} Found {} brands with providers.'.format(
        _LOG_PREFIX, len(data)
    ))
    
    return data


def get_company_destinations_with_brands(company_id: int) -> typing.List[typing.Dict]:
    """
    Get all destinations for a company with their associated brands.
    Joins through CompanyBrandDestination -> CompanyBrands -> Brands.
    
    Args:
        company_id: The ID of the company
        
    Returns:
        List of dictionaries containing destination data with their brands
    """
    logger.info('{} Fetching company destinations with brands for company_id: {}.'.format(
        _LOG_PREFIX, company_id
    ))
    
    destinations = src_models.CompanyDestinations.objects.filter(
        company_id=company_id
    ).prefetch_related(
        'company_brands__company_brand__brand'
    ).all()
    
    data = []
    for destination in destinations:
        brands_data = []
        for company_brand_destination in destination.company_brands.all():
            company_brand = company_brand_destination.company_brand
            brand = company_brand.brand
            
            brands_data.append({
                "id": brand.id if brand else None,
                "name": brand.name if brand else None,
                "status": company_brand.status if company_brand else None,
                "status_name": company_brand.status_name if company_brand else None,
                "data": brand.data if brand else None,
                "company_brand_id": company_brand.id if company_brand else None,
                "created_at": company_brand.created_at.isoformat() if company_brand.created_at else None,
                "updated_at": company_brand.updated_at.isoformat() if company_brand.updated_at else None,
            })
        
        data.append({
            "id": destination.id,
            "status": destination.status,
            "status_name": destination.status_name,
            "destination_type": destination.destination_type,
            "destination_type_name": destination.destination_type_name,
            "credentials": destination.credentials,
            "company_id": destination.company_id,
            "brands": brands_data,
            "created_at": destination.created_at.isoformat() if destination.created_at else None,
            "updated_at": destination.updated_at.isoformat() if destination.updated_at else None,
        })
    
    logger.info('{} Found {} destinations with brands for company_id: {}.'.format(
        _LOG_PREFIX, len(data), company_id
    ))
    
    return data


def get_company_destination_by_id(company_id: int, destination_id: int) -> typing.Optional[typing.Dict]:
    """
    Get a single company destination by ID for a given company_id.
    Includes all destination details including credentials.
    
    Args:
        company_id: The ID of the company
        destination_id: The ID of the company destination
        
    Returns:
        Dictionary containing destination data, or None if not found
    """
    logger.info('{} Fetching company destination with id: {} for company_id: {}.'.format(
        _LOG_PREFIX, destination_id, company_id
    ))
    
    try:
        destination = src_models.CompanyDestinations.objects.filter(
            id=destination_id,
            company_id=company_id
        ).first()
        
        if not destination:
            logger.warning('{} Company destination with id: {} not found for company_id: {}.'.format(
                _LOG_PREFIX, destination_id, company_id
            ))
            return None
        
        data = {
            "id": destination.id,
            "status": destination.status,
            "status_name": destination.status_name,
            "destination_type": destination.destination_type,
            "destination_type_name": destination.destination_type_name,
            "credentials": destination.credentials,
            "company_id": destination.company_id,
            "created_at": destination.created_at.isoformat() if destination.created_at else None,
            "updated_at": destination.updated_at.isoformat() if destination.updated_at else None,
        }
        
        logger.info('{} Found company destination with id: {} for company_id: {}.'.format(
            _LOG_PREFIX, destination_id, company_id
        ))
        
        return data
    except Exception as e:
        logger.error('{} Error fetching company destination with id: {} for company_id: {}. Error: {}.'.format(
            _LOG_PREFIX, destination_id, company_id, str(e)
        ))
        raise


def get_company_execution_runs(
    company_id: int,
    destination_id: typing.Optional[int] = None,
    page: int = 1,
    page_size: int = 20
) -> typing.Dict:
    """
    Get execution runs for a company with pagination.
    Optionally filter by destination_id.
    Includes brand and destination information.
    
    Args:
        company_id: The ID of the company
        destination_id: Optional destination ID to filter by
        page: Page number (default: 1)
        page_size: Number of items per page (default: 20)
        
    Returns:
        Dictionary containing paginated execution runs data with brand and destination info
    """
    logger.info('{} Fetching execution runs for company_id: {}, destination_id: {}, page: {}, page_size: {}.'.format(
        _LOG_PREFIX, company_id, destination_id, page, page_size
    ))
    
    # Filter execution runs by company_id through the relationships
    execution_runs = src_models.CompanyDestinationExecutionRun.objects.filter(
        company_brand_destination__company_brand__company_id=company_id
    )
    
    # Optionally filter by destination_id
    if destination_id:
        execution_runs = execution_runs.filter(
            company_brand_destination__destination_id=destination_id
        )
    
    execution_runs = execution_runs.select_related(
        'company_brand_destination__company_brand__brand',
        'company_brand_destination__destination'
    ).order_by('-created_at')
    
    # Paginate the results
    paginator = Paginator(execution_runs, page_size)
    
    try:
        page_obj = paginator.page(page)
    except Exception as e:
        logger.warning('{} Invalid page number: {}. Error: {}. Returning first page.'.format(
            _LOG_PREFIX, page, str(e)
        ))
        page_obj = paginator.page(1)
    
    data = []
    for execution_run in page_obj:
        company_brand_destination = execution_run.company_brand_destination
        company_brand = company_brand_destination.company_brand if company_brand_destination else None
        brand = company_brand.brand if company_brand else None
        destination = company_brand_destination.destination if company_brand_destination else None
        
        data.append({
            "id": execution_run.id,
            "status": execution_run.status,
            "status_name": execution_run.status_name,
            "products_processed": execution_run.products_processed,
            "products_created": execution_run.products_created,
            "products_updated": execution_run.products_updated,
            "products_failed": execution_run.products_failed,
            "error_message": execution_run.error_message,
            "message": execution_run.message,
            "brand": {
                "id": brand.id if brand else None,
                "name": brand.name if brand else None,
                "status": brand.status if brand else None,
                "status_name": brand.status_name if brand else None,
            } if brand else None,
            "company_brand": {
                "id": company_brand.id if company_brand else None,
                "status": company_brand.status if company_brand else None,
                "status_name": company_brand.status_name if company_brand else None,
            } if company_brand else None,
            "destination": {
                "id": destination.id if destination else None,
                "status": destination.status if destination else None,
                "status_name": destination.status_name if destination else None,
                "destination_type": destination.destination_type if destination else None,
                "destination_type_name": destination.destination_type_name if destination else None,
            } if destination else None,
            "created_at": execution_run.created_at.isoformat() if execution_run.created_at else None,
            "updated_at": execution_run.updated_at.isoformat() if execution_run.updated_at else None,
            "completed_at": execution_run.completed_at.isoformat() if execution_run.completed_at else None,
        })
    
    result = {
        "data": data,
        "pagination": {
            "page": page_obj.number,
            "page_size": page_size,
            "total_pages": paginator.num_pages,
            "total_count": paginator.count,
            "has_next": page_obj.has_next(),
            "has_previous": page_obj.has_previous(),
        }
    }
    
    logger.info('{} Found {} execution runs for company_id: {} (page {} of {}).'.format(
        _LOG_PREFIX, len(data), company_id, page_obj.number, paginator.num_pages
    ))
    
    return result


def get_execution_run_parts_history(
    company_id: int,
    execution_run_id: int,
    page: int = 1,
    page_size: int = 20
) -> typing.Dict:
    """
    Get parts history for a specific execution run with pagination.
    Includes destination part, brand, and destination information.
    
    Args:
        company_id: The ID of the company
        execution_run_id: The ID of the execution run
        page: Page number (default: 1)
        page_size: Number of items per page (default: 20)
        
    Returns:
        Dictionary containing paginated parts history data with part, brand, and destination info
    """
    logger.info('{} Fetching parts history for execution_run_id: {}, company_id: {}, page: {}, page_size: {}.'.format(
        _LOG_PREFIX, execution_run_id, company_id, page, page_size
    ))
    
    # First verify the execution run belongs to the company
    execution_run = src_models.CompanyDestinationExecutionRun.objects.filter(
        id=execution_run_id,
        company_brand_destination__company_brand__company_id=company_id
    ).first()
    
    if not execution_run:
        logger.warning('{} Execution run with id: {} not found for company_id: {}.'.format(
            _LOG_PREFIX, execution_run_id, company_id
        ))
        return None
    
    # Get parts history for this execution run
    parts_history = src_models.CompanyDestinationPartsHistory.objects.filter(
        execution_run_id=execution_run_id
    ).select_related(
        'destination_part__brand',
        'destination_part__company_destination'
    ).order_by('-created_at')
    
    # Paginate the results
    paginator = Paginator(parts_history, page_size)
    
    try:
        page_obj = paginator.page(page)
    except Exception as e:
        logger.warning('{} Invalid page number: {}. Error: {}. Returning first page.'.format(
            _LOG_PREFIX, page, str(e)
        ))
        page_obj = paginator.page(1)
    
    data = []
    for history in page_obj:
        destination_part = history.destination_part
        brand = destination_part.brand if destination_part else None
        destination = destination_part.company_destination if destination_part else None
        
        data.append({
            "id": history.id,
            "data": history.data,
            "changes": history.changes,
            "synced": history.synced,
            "destination_part": {
                "id": destination_part.id if destination_part else None,
                "part_unique_key": destination_part.part_unique_key if destination_part else None,
                "source_external_id": destination_part.source_external_id if destination_part else None,
                "destination_external_id": destination_part.destination_external_id if destination_part else None,
            } if destination_part else None,
            "brand": {
                "id": brand.id if brand else None,
                "name": brand.name if brand else None,
                "status": brand.status if brand else None,
                "status_name": brand.status_name if brand else None,
            } if brand else None,
            "destination": {
                "id": destination.id if destination else None,
                "status": destination.status if destination else None,
                "status_name": destination.status_name if destination else None,
                "destination_type": destination.destination_type if destination else None,
                "destination_type_name": destination.destination_type_name if destination else None,
            } if destination else None,
            "created_at": history.created_at.isoformat() if history.created_at else None,
            "updated_at": history.updated_at.isoformat() if history.updated_at else None,
        })
    
    result = {
        "data": data,
        "pagination": {
            "page": page_obj.number,
            "page_size": page_size,
            "total_pages": paginator.num_pages,
            "total_count": paginator.count,
            "has_next": page_obj.has_next(),
            "has_previous": page_obj.has_previous(),
        }
    }
    
    logger.info('{} Found {} parts history records for execution_run_id: {} (page {} of {}).'.format(
        _LOG_PREFIX, len(data), execution_run_id, page_obj.number, paginator.num_pages
    ))
    
    return result

