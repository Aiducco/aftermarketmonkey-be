import logging
import typing

from src import enums as src_enums
from src import models as src_models
from src.integrations.ecommerce.bigcommerce.gateways import client as bigcommerce_client
from src.integrations.ecommerce.bigcommerce.gateways import exceptions as bigcommerce_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = '[BRANDS-SERVICES]'


def create_brand(
    brand_name: str,
    company_id: int,
    company_destination_id: int,
    providers: typing.List[str],
    turn_14_id: typing.Optional[str] = None,
) -> src_models.Brands:
    logger.info('{} Creating brand setup for brand: {}, company_id: {}, destination_id: {}'.format(
        _LOG_PREFIX, brand_name, company_id, company_destination_id
    ))
    
    # Normalize brand name to uppercase
    brand_name_upper = brand_name.upper().strip()
    
    if not brand_name_upper:
        raise ValueError('Brand name cannot be empty')
    
    # Get or create Brand
    brand, brand_created = src_models.Brands.objects.get_or_create(
        name=brand_name_upper,
        defaults={
            'status': src_enums.BrandProviderStatus.ACTIVE.value,
            'status_name': src_enums.BrandProviderStatus.ACTIVE.name,
        }
    )
    
    if brand_created:
        logger.info('{} Created new brand: {}'.format(_LOG_PREFIX, brand_name_upper))
    else:
        logger.info('{} Using existing brand: {}'.format(_LOG_PREFIX, brand_name_upper))
    
    # Get Company
    try:
        company = src_models.Company.objects.get(id=company_id)
    except src_models.Company.DoesNotExist:
        raise ValueError('Company with id {} not found'.format(company_id))
    
    # Get CompanyDestination
    try:
        company_destination = src_models.CompanyDestinations.objects.get(id=company_destination_id)
    except src_models.CompanyDestinations.DoesNotExist:
        raise ValueError('CompanyDestination with id {} not found'.format(company_destination_id))
    
    # Create or get CompanyBrands
    company_brand, company_brand_created = src_models.CompanyBrands.objects.get_or_create(
        company=company,
        brand=brand,
        defaults={
            'status': src_enums.CompanyBrandStatus.ACTIVE.value,
            'status_name': src_enums.CompanyBrandStatus.ACTIVE.name,
        }
    )
    
    if company_brand_created:
        logger.info('{} Created CompanyBrands for company: {} and brand: {}'.format(
            _LOG_PREFIX, company.name, brand_name_upper
        ))
    else:
        logger.info('{} CompanyBrands already exists for company: {} and brand: {}'.format(
            _LOG_PREFIX, company.name, brand_name_upper
        ))
    
    # Create or get CompanyBrandDestination
    company_brand_destination, cbd_created = src_models.CompanyBrandDestination.objects.get_or_create(
        company_brand=company_brand,
        destination=company_destination,
    )
    
    if cbd_created:
        logger.info('{} Created CompanyBrandDestination for company_brand and destination: {}'.format(
            _LOG_PREFIX, company_destination_id
        ))
    else:
        logger.info('{} CompanyBrandDestination already exists for company_brand and destination: {}'.format(
            _LOG_PREFIX, company_destination_id
        ))
    
    # Check CompanyProviders for each provider name
    for provider_name in providers:
        try:
            provider = src_models.Providers.objects.get(name=provider_name)
        except src_models.Providers.DoesNotExist:
            logger.warning('{} Provider with name "{}" not found. Skipping.'.format(
                _LOG_PREFIX, provider_name
            ))
            continue
        
        # Check if CompanyProviders exists, if not return early
        try:
            company_provider = src_models.CompanyProviders.objects.get(
                company=company,
                provider=provider,
            )
        except src_models.CompanyProviders.DoesNotExist:
            logger.warning('{} CompanyProviders does not exist for company: {} and provider: {}. Exiting.'.format(
                _LOG_PREFIX, company.name, provider_name
            ))
            return brand
        
        # Create or get BrandProviders (links brand to provider)
        brand_provider, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=provider,
        )
        
        if bp_created:
            logger.info('{} Created BrandProviders for brand: {} and provider: {}'.format(
                _LOG_PREFIX, brand_name_upper, provider_name
            ))
        else:
            logger.info('{} BrandProviders already exists for brand: {} and provider: {}'.format(
                _LOG_PREFIX, brand_name_upper, provider_name
            ))
    
    # Create BrandTurn14BrandMapping if turn_14_id is provided
    if turn_14_id:
        try:
            turn14_brand = src_models.Turn14Brand.objects.get(external_id=turn_14_id)
        except src_models.Turn14Brand.DoesNotExist:
            logger.warning('{} Turn14Brand with external_id "{}" not found. Skipping brand mapping creation.'.format(
                _LOG_PREFIX, turn_14_id
            ))
        else:
            brand_turn14_mapping, mapping_created = src_models.BrandTurn14BrandMapping.objects.get_or_create(
                brand=brand,
                turn14_brand=turn14_brand,
            )
            
            if mapping_created:
                logger.info('{} Created BrandTurn14BrandMapping for brand: {} and Turn14Brand: {}'.format(
                    _LOG_PREFIX, brand_name_upper, turn_14_id
                ))
            else:
                logger.info('{} BrandTurn14BrandMapping already exists for brand: {} and Turn14Brand: {}'.format(
                    _LOG_PREFIX, brand_name_upper, turn_14_id
                ))
    
    logger.info('{} Successfully completed brand setup for brand: {}'.format(
        _LOG_PREFIX, brand_name_upper
    ))
    
    return brand


def create_bigcommerce_brand(
    turn_14_id: str,
    company_destination_id: int,
) -> src_models.BigCommerceBrands:
    logger.info('{} Creating BigCommerce brand for turn_14_id: {}, company_destination_id: {}'.format(
        _LOG_PREFIX, turn_14_id, company_destination_id
    ))
    
    # Get Turn14Brand
    try:
        turn14_brand = src_models.Turn14Brand.objects.get(external_id=turn_14_id)
    except src_models.Turn14Brand.DoesNotExist:
        raise ValueError('Turn14Brand with external_id "{}" not found'.format(turn_14_id))
    
    # Get Brand from BrandTurn14BrandMapping
    try:
        brand_mapping = src_models.BrandTurn14BrandMapping.objects.get(turn14_brand=turn14_brand)
        brand = brand_mapping.brand
    except src_models.BrandTurn14BrandMapping.DoesNotExist:
        raise ValueError('BrandTurn14BrandMapping not found for Turn14Brand with external_id "{}"'.format(turn_14_id))
    
    # Get CompanyDestination
    try:
        company_destination = src_models.CompanyDestinations.objects.get(id=company_destination_id)
    except src_models.CompanyDestinations.DoesNotExist:
        raise ValueError('CompanyDestination with id {} not found'.format(company_destination_id))
    
    # Get credentials and create API client
    credentials = company_destination.credentials
    try:
        api_client = bigcommerce_client.BigCommerceApiClient(credentials=credentials)
    except ValueError as e:
        raise ValueError('Invalid credentials for CompanyDestination: {}. Error: {}'.format(
            company_destination_id, str(e)
        ))
    
    # Prepare brand data for BigCommerce API
    # Format brand name: first letter of each word capitalized
    brand_name_formatted = brand.name.title()
    
    brand_data = {
        'name': brand_name_formatted,
    }
    
    # Add logo if available from Turn14Brand
    if turn14_brand.logo:
        brand_data['image_url'] = turn14_brand.logo
    
    # Create brand on BigCommerce
    try:
        bigcommerce_brand_response = api_client.create_brand(brand_data=brand_data)
    except bigcommerce_exceptions.BigCommerceAPIException as e:
        logger.error('{} Error creating brand on BigCommerce API for turn_14_id: {}. Error: {}.'.format(
            _LOG_PREFIX, turn_14_id, str(e)
        ))
        raise
    
    external_id = str(bigcommerce_brand_response.get('id', ''))
    if not external_id:
        raise ValueError('No brand ID returned from BigCommerce API for turn_14_id: {}'.format(turn_14_id))
    
    # Create or get BigCommerceBrands record
    bigcommerce_brand, created = src_models.BigCommerceBrands.objects.get_or_create(
        external_id=external_id,
        brand=brand,
        company_destination=company_destination,
        defaults={
            'name': brand_name_formatted,
        }
    )
    
    if created:
        logger.info('{} Created BigCommerceBrands for brand: {}, external_id: {}, turn_14_id: {}'.format(
            _LOG_PREFIX, brand.name, external_id, turn_14_id
        ))
    else:
        logger.info('{} BigCommerceBrands already exists for brand: {}, external_id: {}, turn_14_id: {}'.format(
            _LOG_PREFIX, brand.name, external_id, turn_14_id
        ))
    
    return bigcommerce_brand

