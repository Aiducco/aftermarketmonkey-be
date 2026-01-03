import dataclasses
import json
import logging
import typing
import time
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote, urlparse, urlunparse
from django.db.models import F
from django.db.models.functions import TruncWeek
from django.utils import timezone
import pgbulk

from src import constants as src_constants
from src import enums as src_enums
from src import messages as src_messages
from src import models as src_models
from src.integrations.ecommerce.bigcommerce.gateways import client as bigcommerce_client
from src.integrations.ecommerce.bigcommerce.gateways import exceptions as bigcommerce_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = '[BIGCOMMERCE-SERVICES]'

# Configuration for parallel processing and retries
_MAX_WORKERS = 1  # Number of parallel threads
_MAX_RETRIES = 3  # Maximum number of retry attempts
_RETRY_BASE_DELAY = 1  # Base delay in seconds for exponential backoff
_RETRY_MAX_DELAY = 10  # Maximum delay in seconds
_PARALLEL_REQUEST_DELAY = 0.0  # Base delay in seconds between parallel requests (increased to reduce 500 errors)
_PARALLEL_REQUEST_DELAY_JITTER = 0.0  # Random jitter to add to delay (0 to this value)
_SERVER_ERROR_RETRY_DELAY = 2  # Additional delay for 500 errors (in seconds)


def fetch_and_save_all_bigcommerce_brands() -> None:
    logger.info('{} Started fetching and saving BigCommerce brands.'.format(_LOG_PREFIX))

    all_destinations = src_models.CompanyDestinations.objects.filter(
        destination_type=src_enums.IntegrationDestinationType.BIGCOMMERCE.value
    )

    if not all_destinations.exists():
        logger.info('{} No BigCommerce destinations found.'.format(_LOG_PREFIX))
        return

    logger.info('{} Found {} BigCommerce destinations.'.format(_LOG_PREFIX, all_destinations.count()))

    for destination in all_destinations:
        company = destination.company
        credentials = destination.credentials

        logger.info('{} Processing destination: {} (company: {}).'.format(
            _LOG_PREFIX, destination.id, company.name
        ))

        try:
            api_client = bigcommerce_client.BigCommerceApiClient(credentials=credentials)
        except ValueError as e:
            logger.error('{} Invalid credentials for destination: {} (company: {}). Error: {}. Skipping.'.format(
                _LOG_PREFIX, destination.id, company.name, str(e)
            ))
            continue

        page = 1
        total_processed = 0
        total_skipped = 0

        while page is not None:
            try:
                brands_data, next_page = api_client.get_brands(page=page)
            except bigcommerce_exceptions.BigCommerceAPIException as e:
                logger.error('{} BigCommerce API error for destination: {} (company: {}), page: {}. Error: {}. Skipping destination.'.format(
                    _LOG_PREFIX, destination.id, company.name, page, str(e)
                ))
                break

            if not brands_data:
                logger.warning('{} No brands data returned for destination: {} (company: {}), page: {}.'.format(
                    _LOG_PREFIX, destination.id, company.name, page
                ))
                page = next_page
                continue

            logger.info('{} Fetched {} brands for destination: {} (company: {}), page: {}.'.format(
                _LOG_PREFIX, len(brands_data), destination.id, company.name, page
            ))

            brand_instances = _transform_brands_data(brands_data, destination, company)

            if not brand_instances:
                logger.warning('{} No valid brand instances created for destination: {} (company: {}), page: {}.'.format(
                    _LOG_PREFIX, destination.id, company.name, page
                ))
                page = next_page
                continue

            try:
                upserted_brands = pgbulk.upsert(
                    src_models.BigCommerceBrands,
                    brand_instances,
                    unique_fields=['external_id', 'brand', 'company_destination'],
                    update_fields=['name'],
                    returning=True,
                )

                processed_count = len(upserted_brands) if upserted_brands else 0
                total_processed += processed_count
                total_skipped += len(brands_data) - processed_count

                logger.info('{} Successfully upserted {} brands for destination: {} (company: {}), page: {}.'.format(
                    _LOG_PREFIX, processed_count, destination.id, company.name, page
                ))
            except Exception as e:
                logger.error('{} Error during bulk upsert for destination: {} (company: {}), page: {}. Error: {}.'.format(
                    _LOG_PREFIX, destination.id, company.name, page, str(e)
                ))
                page = next_page
                continue

            page = next_page

        logger.info('{} Completed fetching brands for destination: {} (company: {}). Processed: {}, Skipped: {}.'.format(
            _LOG_PREFIX, destination.id, company.name, total_processed, total_skipped
        ))


def _transform_brands_data(
    brands_data: typing.List[typing.Dict],
    destination: src_models.CompanyDestinations,
    company: src_models.Company
) -> typing.List[src_models.BigCommerceBrands]:
    brand_instances = []

    for brand_data in brands_data:
        try:
            external_id = str(brand_data.get('id', ''))
            name = brand_data.get('name', '').strip()

            if not external_id or not name:
                logger.warning('{} Skipping brand with missing external_id or name: {}'.format(
                    _LOG_PREFIX, brand_data
                ))
                continue

            brand_name_upper = name.upper()
            brand = src_models.Brands.objects.filter(name=brand_name_upper).first()

            if not brand:
                logger.debug('{} Brand not found in Brands table: {}. Skipping.'.format(
                    _LOG_PREFIX, brand_name_upper
                ))
                continue

            company_brand = src_models.CompanyBrands.objects.filter(
                company=company,
                brand=brand
            ).first()

            if not company_brand:
                logger.debug('{} Brand {} not found in CompanyBrands for company: {}. Skipping.'.format(
                    _LOG_PREFIX, brand_name_upper, company.name
                ))
                continue

            brand_provider = src_models.BrandProviders.objects.filter(
                brand=brand
            ).first()

            if not brand_provider:
                logger.debug('{} Brand {} not found in BrandProviders. Skipping.'.format(
                    _LOG_PREFIX, brand_name_upper
                ))
                continue

            brand_instance = src_models.BigCommerceBrands(
                external_id=external_id,
                name=name,
                brand=brand,
                company_destination=destination,
            )

            brand_instances.append(brand_instance)

        except Exception as e:
            logger.warning('{} Error transforming brand data {}: {}. Skipping.'.format(
                _LOG_PREFIX, brand_data, str(e)
            ))
            continue

    return brand_instances


def fetch_and_save_all_bigcommerce_products() -> None:
    logger.info('{} Started fetching and saving BigCommerce products.'.format(_LOG_PREFIX))

    all_destinations = src_models.CompanyDestinations.objects.filter(
        destination_type=src_enums.IntegrationDestinationType.BIGCOMMERCE.value
    )

    if not all_destinations.exists():
        logger.info('{} No BigCommerce destinations found.'.format(_LOG_PREFIX))
        return

    logger.info('{} Found {} BigCommerce destinations.'.format(_LOG_PREFIX, all_destinations.count()))

    for destination in all_destinations:
        company = destination.company
        credentials = destination.credentials

        logger.info('{} Processing destination: {} (company: {}).'.format(
            _LOG_PREFIX, destination.id, company.name
        ))

        try:
            api_client = bigcommerce_client.BigCommerceApiClient(credentials=credentials)
        except ValueError as e:
            logger.error('{} Invalid credentials for destination: {} (company: {}). Error: {}. Skipping.'.format(
                _LOG_PREFIX, destination.id, company.name, str(e)
            ))
            continue

        page = 1
        total_processed = 0

        while page is not None:
            try:
                products_data, next_page = api_client.get_products(page=page)
            except bigcommerce_exceptions.BigCommerceAPIException as e:
                logger.error('{} BigCommerce API error for destination: {} (company: {}), page: {}. Error: {}. Skipping destination.'.format(
                    _LOG_PREFIX, destination.id, company.name, page, str(e)
                ))
                break

            if not products_data:
                logger.warning('{} No products data returned for destination: {} (company: {}), page: {}.'.format(
                    _LOG_PREFIX, destination.id, company.name, page
                ))
                page = next_page
                continue

            logger.info('{} Fetched {} products for destination: {} (company: {}), page: {}.'.format(
                _LOG_PREFIX, len(products_data), destination.id, company.name, page
            ))

            product_instances = _transform_products_data(products_data, destination)

            if not product_instances:
                logger.warning('{} No valid product instances created for destination: {} (company: {}), page: {}.'.format(
                    _LOG_PREFIX, destination.id, company.name, page
                ))
                page = next_page
                continue

            try:
                upserted_products = pgbulk.upsert(
                    src_models.BigCommerceParts,
                    product_instances,
                    unique_fields=['external_id', 'sku', 'company_destination'],
                    update_fields=['raw_data', 'external_brand_id'],
                    returning=True,
                )

                processed_count = len(upserted_products) if upserted_products else 0
                total_processed += processed_count

                logger.info('{} Successfully upserted {} products for destination: {} (company: {}), page: {}.'.format(
                    _LOG_PREFIX, processed_count, destination.id, company.name, page
                ))
            except Exception as e:
                logger.error('{} Error during bulk upsert for destination: {} (company: {}), page: {}. Error: {}.'.format(
                    _LOG_PREFIX, destination.id, company.name, page, str(e)
                ))
                page = next_page
                continue

            page = next_page

        logger.info('{} Completed fetching products for destination: {} (company: {}). Processed: {}.'.format(
            _LOG_PREFIX, destination.id, company.name, total_processed
        ))


def _transform_products_data(
    products_data: typing.List[typing.Dict],
    destination: src_models.CompanyDestinations
) -> typing.List[src_models.BigCommerceParts]:
    product_instances = []

    for product_data in products_data:
        try:
            external_id = str(product_data.get('id', ''))
            sku = product_data.get('sku', '').strip()

            if not external_id:
                logger.warning('{} Skipping product with missing external_id: {}'.format(
                    _LOG_PREFIX, product_data
                ))
                continue

            if not sku:
                sku = external_id

            brand_id = product_data.get('brand_id')
            external_brand_id = str(brand_id) if brand_id is not None else None

            product_instance = src_models.BigCommerceParts(
                external_id=external_id,
                sku=sku,
                raw_data=product_data,
                external_brand_id=external_brand_id,
                company_destination=destination,
            )

            product_instances.append(product_instance)

        except Exception as e:
            logger.warning('{} Error transforming product data {}: {}. Skipping.'.format(
                _LOG_PREFIX, product_data, str(e)
            ))
            continue

    return product_instances


def fetch_and_sync_all_ecommerce_parts_to_bigcommerce_destination() -> None:
    '''
        1. Fetch all company brands where company is active and brand is active
        2. Prepare all parts for it for bigcommerce -> create message for BigCommercePart
        3. Continue script

    '''
    logger.info('{} Started fetching and syncing all ecommerce parts to bigcommerce destination.'.format(_LOG_PREFIX))
    bigcommerce_active_destinations = src_models.CompanyDestinations.objects.filter(
        destination_type=src_enums.IntegrationDestinationType.BIGCOMMERCE.value,
        status=src_enums.IntegrationDestinationStatus.ACTIVE.value,
    )
    if not bigcommerce_active_destinations:
        logger.info('{} No active destinations found for bigcommerce destination.'.format(_LOG_PREFIX))
        return

    company_brands_for_bigcommerce_destination = src_models.CompanyBrandDestination.objects.filter(
        destination__in=bigcommerce_active_destinations,
    )

    if not company_brands_for_bigcommerce_destination:
        logger.info('{} Found no active company brands for bigcommerce destination.'.format(_LOG_PREFIX))
        return

    logger.info('{} Found {} company brands for bigcommerce destination.'.format(_LOG_PREFIX, len(company_brands_for_bigcommerce_destination)))
    for company_brand in company_brands_for_bigcommerce_destination:
        try:
            fetch_and_sync_ecommerce_parts_for_company_brand_to_bigcommerce(
                company_brand=company_brand
            )
        except Exception as e:
            logger.exception('{} Error while fetching and syncing ecommerce parts for company brand to bigcommerce. Error: {}'.format(
                _LOG_PREFIX, str(e)
            ))

def fetch_and_sync_ecommerce_parts_for_company_brand_to_bigcommerce(
    company_brand: src_models.CompanyBrandDestination
) -> None:
    logger.info('{} Started fetching and syncing parts (destination_id={}, brand_id={}) to bigcommerce destination.'.format(
        _LOG_PREFIX, company_brand.destination_id, company_brand.company_brand.brand_id
    ))

    if company_brand.company_brand.brand.status_name != src_enums.CompanyBrandStatus.ACTIVE.name:
        logger.info('{} Company brand {} is not ACTIVE. Skipping fetching and syncing ecomm parts.'.format(
            _LOG_PREFIX, company_brand.company_brand.brand.name
        ))
        return

    execution_run = src_models.CompanyDestinationExecutionRun.objects.create(
        company_brand_destination=company_brand,
        status=src_enums.DestinationExecutionRunStatus.STARTED.value,
        status_name=src_enums.DestinationExecutionRunStatus.STARTED.name,
    )

    try:
        products_candidates_for_sync = prepare_products_for_syncing_into_bigcommerce(
            company=company_brand.company_brand.company, brand=company_brand.company_brand.brand, destination=company_brand.destination
        )

        if not products_candidates_for_sync:
            message = 'No product candidates found to sync into BigCommerce.'
            logger.info('{} {}'.format(_LOG_PREFIX, message))
            execution_run.status = src_enums.DestinationExecutionRunStatus.COMPLETED.value
            execution_run.status_name = src_enums.DestinationExecutionRunStatus.COMPLETED.name
            execution_run.message = message
            execution_run.completed_at = timezone.now()
            execution_run.save()
            return

        logger.info(
            '{} Found {} products candidates to sync into BigCommerce.'.format(_LOG_PREFIX, len(products_candidates_for_sync))
        )

        products_for_sync = select_products_for_syncing_into_bigcommerce(
            products_candidates_for_sync=products_candidates_for_sync,
            execution_run=execution_run
        )
        if not products_for_sync:
            message = 'No products found to sync into BigCommerce.'
            logger.info('{} {}'.format(_LOG_PREFIX, message))
            execution_run.status = src_enums.DestinationExecutionRunStatus.COMPLETED.value
            execution_run.status_name = src_enums.DestinationExecutionRunStatus.COMPLETED.name
            execution_run.message = message
            execution_run.completed_at = timezone.now()
            execution_run.save()
            return

        logger.info(
            '{} Found {} products to sync into BigCommerce.'.format(_LOG_PREFIX, len(products_for_sync))
        )

        destination = company_brand.destination
        company = company_brand.company_brand.company
        brand = company_brand.company_brand.brand

        try:
            api_client = bigcommerce_client.BigCommerceApiClient(credentials=destination.credentials)
        except ValueError as e:
            error_msg = 'Invalid credentials for destination: {} (company: {}). Error: {}.'.format(
                destination.id, company.name, str(e)
            )
            logger.error('{} {}'.format(_LOG_PREFIX, error_msg))
            execution_run.status = src_enums.DestinationExecutionRunStatus.FAILED.value
            execution_run.status_name = src_enums.DestinationExecutionRunStatus.FAILED.name
            execution_run.error_message = error_msg
            execution_run.message = error_msg
            execution_run.completed_at = timezone.now()
            execution_run.save()
            return

        products_to_update, products_to_create = _categorize_products_for_sync(
            products_for_sync=products_for_sync,
            destination=destination,
            brand=brand
        )

        # Process products in parallel with retry logic
        total_products = len(products_to_update) + len(products_to_create)
        logger.info('{} Processing {} products in parallel ({} to update, {} to create) with max {} workers.'.format(
            _LOG_PREFIX, total_products, len(products_to_update), len(products_to_create), _MAX_WORKERS
        ))

        # Thread-safe counters
        counters = {
            'processed': 0,
            'created': 0,
            'updated': 0,
            'failed': 0,
            'lock': threading.Lock()
        }

        # Process updates and creates in parallel
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = []
            
            # Submit update tasks
            for product_to_sync, bigcommerce_part, company_destination_part in products_to_update:
                future = executor.submit(
                    _process_product_update_with_retry,
                    product_to_sync=product_to_sync,
                    bigcommerce_part=bigcommerce_part,
                    company_destination_part=company_destination_part,
                    destination=destination,
                    brand=brand,
                    api_client=api_client,
                    execution_run=execution_run,
                    counters=counters
                )
                futures.append(future)
            
            # Submit create tasks
            for product_to_sync, company_destination_part in products_to_create:
                future = executor.submit(
                    _process_product_create_with_retry,
                    product_to_sync=product_to_sync,
                    company_destination_part=company_destination_part,
                    destination=destination,
                    brand=brand,
                    api_client=api_client,
                    execution_run=execution_run,
                    counters=counters
                )
                futures.append(future)
            
            # Wait for all tasks to complete and log progress
            completed = 0
            for future in as_completed(futures):
                completed += 1
                if completed % 10 == 0 or completed == total_products:
                    with counters['lock']:
                        logger.info('{} Progress: {}/{} products processed (Created: {}, Updated: {}, Failed: {}).'.format(
                            _LOG_PREFIX, completed, total_products,
                            counters['created'], counters['updated'], counters['failed']
                        ))
        
        # Update execution_run with final counts
        execution_run.products_processed = counters['processed']
        execution_run.products_created = counters['created']
        execution_run.products_updated = counters['updated']
        execution_run.products_failed = counters['failed']

        message = 'Completed sync run. Processed: {}, Created: {}, Updated: {}, Failed: {}.'.format(
            execution_run.products_processed, execution_run.products_created,
            execution_run.products_updated, execution_run.products_failed
        )
        logger.info('{} {} (id={})'.format(_LOG_PREFIX, message, execution_run.id))
        execution_run.status = src_enums.DestinationExecutionRunStatus.COMPLETED.value
        execution_run.status_name = src_enums.DestinationExecutionRunStatus.COMPLETED.name
        execution_run.message = message
        execution_run.completed_at = timezone.now()
        execution_run.save()

    except Exception as e:
        error_msg = 'Error during sync: {}'.format(str(e))
        logger.exception('{} {}'.format(_LOG_PREFIX, error_msg))
        execution_run.status = src_enums.DestinationExecutionRunStatus.FAILED.value
        execution_run.status_name = src_enums.DestinationExecutionRunStatus.FAILED.name
        execution_run.error_message = error_msg
        execution_run.message = error_msg
        execution_run.completed_at = timezone.now()
        execution_run.save()


def prepare_products_for_syncing_into_bigcommerce(
        company: src_models.Company,
        brand: src_models.Brands,
        destination: src_models.CompanyDestinations
) -> list[src_messages.BigCommercePart]:
    brand_providers = src_models.BrandProviders.objects.filter(
        brand=brand
    )
    if not brand_providers:
        logger.error('{} No brand providers found for brand {}.'.format(
            _LOG_PREFIX, brand.name
        ))
        raise Exception('{} No brand providers found for brand {}.'.format(_LOG_PREFIX, brand.name))

    # Group providers by type (CATALOG vs DISTRIBUTOR)
    catalog_providers = []
    distributor_providers = []
    
    for brand_provider in brand_providers:
        provider = brand_provider.provider
        provider_type = provider.type_name
        
        if provider_type == src_enums.BrandProvider.CATALOG.name:
            catalog_providers.append(brand_provider)
        elif provider_type == src_enums.BrandProvider.DISTRIBUTOR.name:
            distributor_providers.append(brand_provider)
    
    # Prepare parts from all CATALOG providers (grouped by provider)
    catalog_parts_by_provider = {}
    for catalog_provider in catalog_providers:
        try:
            parts = _prepare_parts_by_kind(catalog_provider.provider.kind_name, brand)
            # Store parts by provider
            catalog_parts_by_provider[catalog_provider] = {part.sku: part for part in parts}
        except Exception as e:
            logger.exception('{} Error while preparing catalog products (kind: {}) for brand {}. Error: {}.'.format(
                _LOG_PREFIX, catalog_provider.provider.kind_name, brand, str(e)
            ))
    
    # Prepare parts from all DISTRIBUTOR providers (grouped by provider)
    distributor_parts_by_provider = {}
    for distributor_provider in distributor_providers:
        try:
            parts = _prepare_parts_by_kind(distributor_provider.provider.kind_name, brand)
            # Store parts by provider
            distributor_parts_by_provider[distributor_provider] = {part.sku: part for part in parts}
        except Exception as e:
            logger.exception('{} Error while preparing distributor products (kind: {}) for brand {}. Error: {}.'.format(
                _LOG_PREFIX, distributor_provider.provider.kind_name, brand, str(e)
            ))
    
    # Use parts from first catalog provider (later we'll add logic to determine which provider to use)
    catalog_parts = {}
    if catalog_parts_by_provider and catalog_providers:
        first_catalog_provider = catalog_providers[0]
        catalog_parts = catalog_parts_by_provider.get(first_catalog_provider, {})
    
    # Use parts from first distributor provider (later we'll add logic to determine which provider to use)
    distributor_parts = {}
    if distributor_parts_by_provider and distributor_providers:
        first_distributor_provider = distributor_providers[0]
        distributor_parts = distributor_parts_by_provider.get(first_distributor_provider, {})
    
    # If no catalog parts, return distributor parts as-is
    if not catalog_parts:
        return list(distributor_parts.values())
    
    # Merge catalog and distributor parts
    merged_parts = []
    
    # Go through each catalog part and try to find matching distributor part
    for sku, catalog_part in catalog_parts.items():
        distributor_part = distributor_parts.get(sku)
        if distributor_part is None:
            logger.info('{} Catalog SKU {} not found in distributor parts. Using catalog part only.'.format(
                _LOG_PREFIX, sku
            ))
            continue

        merged_part = _merge_catalog_and_distributor_parts(catalog_part, distributor_part)
        merged_parts.append(merged_part)
    
    for sku, distributor_part in distributor_parts.items():
        if sku not in catalog_parts:
            merged_parts.append(distributor_part)
    
    return merged_parts


def _prepare_parts_by_kind(kind_name: str, brand: src_models.Brands) -> list[src_messages.BigCommercePart]:
    """
    Prepare parts based on provider kind_name.
    Routes to the appropriate preparation function.
    """
    if kind_name == src_enums.BrandProviderKind.SDC.name:
        return prepare_sdc_products_for_bigcommerce(brand=brand)
    elif kind_name == src_enums.BrandProviderKind.TURN_14.name:
        return prepare_turn_14_products_for_bigcommerce(brand=brand)
    else:
        logger.warning('{} Unknown provider kind: {}. Skipping.'.format(_LOG_PREFIX, kind_name))
        return []


def _merge_catalog_and_distributor_parts(
    catalog_part: src_messages.BigCommercePart,
    distributor_part: typing.Optional[src_messages.BigCommercePart]
) -> src_messages.BigCommercePart:
    """
    Merge catalog and distributor parts according to field priority rules.
    If distributor_part is None, returns catalog_part as-is.
    """
    if not distributor_part:
        return catalog_part
    
    # Get field priority configuration
    field_priority = src_constants.BIGCOMMERCE_PART_FIELD_PRIORITY
    
    # Build merged part field by field
    merged_fields = {}
    
    # Get all fields from BigCommercePart dataclass
    for field_name in catalog_part.__dataclass_fields__.keys():
        # Special handling for custom_fields - merge/combine from both sources
        if field_name == 'custom_fields':
            catalog_custom_fields = getattr(catalog_part, field_name, None) or []
            distributor_custom_fields = getattr(distributor_part, field_name, None) or []
            
            # Combine custom fields from both sources
            # Create a map by name to avoid duplicates
            combined_custom_fields_map = {}
            
            # Add catalog custom fields first
            if isinstance(catalog_custom_fields, list):
                for field in catalog_custom_fields:
                    if isinstance(field, dict):
                        field_name_key = field.get('name', '').strip()
                        if field_name_key:
                            combined_custom_fields_map[field_name_key] = field
            
            # Add distributor custom fields (will overwrite catalog if same name)
            if isinstance(distributor_custom_fields, list):
                for field in distributor_custom_fields:
                    if isinstance(field, dict):
                        field_name_key = field.get('name', '').strip()
                        if field_name_key:
                            combined_custom_fields_map[field_name_key] = field
            
            merged_fields[field_name] = list(combined_custom_fields_map.values())
            continue
        
        primary_source = field_priority.get(field_name, 'CATALOG')  # Default to CATALOG
        
        if primary_source == 'CATALOG':
            # Try catalog first, fallback to distributor
            catalog_value = getattr(catalog_part, field_name, None)
            distributor_value = getattr(distributor_part, field_name, None)
            
            # Check if value is null/empty
            if _is_value_empty(catalog_value):
                merged_fields[field_name] = distributor_value
            else:
                merged_fields[field_name] = catalog_value
        else:  # DISTRIBUTOR
            # Try distributor first, fallback to catalog
            distributor_value = getattr(distributor_part, field_name, None)
            catalog_value = getattr(catalog_part, field_name, None)
            
            # Check if value is null/empty
            if _is_value_empty(distributor_value):
                merged_fields[field_name] = catalog_value
            else:
                merged_fields[field_name] = distributor_value
    
    # Create merged BigCommercePart
    return src_messages.BigCommercePart(**merged_fields)


def _is_value_empty(value: typing.Any) -> bool:
    """
    Check if a value is considered empty (None, empty string, empty list/dict).
    Note: 0 is considered a valid value for numeric fields (prices, inventory, etc.).
    """
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, (list, dict)) and len(value) == 0:
        return True
    return False

def prepare_sdc_products_for_bigcommerce(brand: src_models.Brands) -> list[src_messages.BigCommercePart]:
    bigcommerce_parts = []
    sdc_brand = src_models.BrandSDCBrandMapping.objects.get(brand_id=brand.id)
    sdc_items = src_models.SDCParts.objects.filter(
        brand_id=sdc_brand.sdc_brand.id
    )
    bigcommerce_brand = src_models.BigCommerceBrands.objects.get(brand_id=brand.id)

    # Get fitments for all SDC items in bulk
    all_part_numbers = [item.part_number for item in sdc_items]
    fitments_dict = {}
    for fitment in src_models.SDCPartFitment.objects.filter(
        sku__in=all_part_numbers,
        brand=sdc_brand.sdc_brand
    ).order_by('year', 'make', 'model'):
        # Store all fitments for each SKU as a list
        if fitment.sku not in fitments_dict:
            fitments_dict[fitment.sku] = []
        fitments_dict[fitment.sku].append(fitment)

    for sdc_item in sdc_items:
        default_price, cost, msrp = _get_sdc_prices(sdc_item)
        width, height, depth = _get_sdc_dimensions(sdc_item)
        weight = _get_sdc_weight(sdc_item)
        
        # Get all fitments for this part
        fitments = fitments_dict.get(sdc_item.part_number, [])
        
        # Get category and subcategory from first fitment
        first_fitment = fitments[0] if fitments else None
        category = first_fitment.category_pcdb if first_fitment else None
        subcategory = first_fitment.subcategory_pcdb if first_fitment else None
        
        # Get description with fitment table
        description = _get_sdc_description(sdc_item, fitments=fitments if fitments else None)
        images = _get_sdc_images(sdc_item)
        inventory = _get_sdc_inventory(sdc_item)
        
        # Active only if Life Cycle Status is 'Available To Order'
        is_active = sdc_item.life_cycle_status == 'Available To Order'
        
        # Custom fields (fitments not added as custom fields for now)
        custom_fields = []
        
        # Prepare fitments data for vehicle hierarchy
        fitments_data = []
        for fitment in fitments:
            fitments_data.append({
                'year': fitment.year,
                'make': fitment.make,
                'model': fitment.model,
            })
        
        bigcommerce_parts.append(
            src_messages.BigCommercePart(
                brand_id=int(bigcommerce_brand.external_id),
                product_title='{} - {}'.format(sdc_item.title or '', sdc_item.part_number),
                sku=sdc_item.part_number,
                mpn=sdc_item.part_number,
                default_price=default_price,
                cost=cost,
                msrp=msrp,
                weight=weight,
                width=width,
                height=height,
                depth=depth,
                description=description,
                images=images,
                inventory=inventory,
                custom_fields=custom_fields,
                active=is_active,
                category=category,
                subcategory=subcategory,
                fitments=fitments_data if fitments_data else None,
            )
        )

    return bigcommerce_parts

def _get_sdc_prices(sdc_item: src_models.SDCParts) -> typing.Tuple[float, float, float]:
    """
    Extract prices from SDC part.
    Returns: (default_price, cost, msrp)
    - default_price: MAP if available, otherwise retail, otherwise jobber
    - cost: jobber_usd (typically the cost price)
    - msrp: retail_usd (retail/MSRP price)
    """
    default_price = 0.0
    cost = 0.0
    msrp = 0.0
    
    # Convert Decimal to float, handling None values
    map_price = float(sdc_item.map_usd) if sdc_item.map_usd is not None else None
    retail_price = float(sdc_item.retail_usd) if sdc_item.retail_usd is not None else None
    jobber_price = float(sdc_item.jobber_usd) if sdc_item.jobber_usd is not None else None
    
    # For default_price: use MAP if available, otherwise Retail, otherwise Jobber
    default_price = map_price if map_price is not None else (retail_price if retail_price is not None else (jobber_price if jobber_price is not None else 0.0))
    
    # Cost is jobber_usd
    cost = jobber_price if jobber_price is not None else 0.0
    
    # MSRP is retail_usd
    msrp = retail_price if retail_price is not None else 0.0
    
    return default_price, cost, msrp


def _get_sdc_weight(sdc_item: src_models.SDCParts) -> float:
    """Get weight from SDC part. Weight is stored in weight_for_case in pounds."""
    if sdc_item.weight_for_case is not None:
        try:
            return float(sdc_item.weight_for_case)
        except (ValueError, TypeError):
            pass
    return 0.0


def _get_sdc_dimensions(sdc_item: src_models.SDCParts) -> typing.Tuple[typing.Optional[float], typing.Optional[float], typing.Optional[float]]:
    """
    Extract width, height, and depth from SDC part.
    Returns: (width, height, depth)
    """
    width = None
    height = None
    depth = None
    
    if sdc_item.width_for_case is not None:
        try:
            width = float(sdc_item.width_for_case)
        except (ValueError, TypeError):
            width = None
    
    if sdc_item.height_for_case is not None:
        try:
            height = float(sdc_item.height_for_case)
        except (ValueError, TypeError):
            height = None
    
    if sdc_item.length_for_case is not None:
        try:
            depth = float(sdc_item.length_for_case)
        except (ValueError, TypeError):
            depth = None
    
    return (width, height, depth)


def _get_sdc_description(sdc_item: src_models.SDCParts, fitments: typing.Optional[typing.List[src_models.SDCPartFitment]] = None) -> str:
    """
    Format SDC descriptions as HTML.
    Combines long_description, extended_description, marketing_description, and features_and_benefits.
    Adds fitment table at the end if fitments are provided.
    """
    html_parts = []
    
    # Add long description or marketing description as overview
    overview_text = None
    if sdc_item.marketing_description:
        overview_text = sdc_item.marketing_description
    elif sdc_item.extended_description:
        overview_text = sdc_item.extended_description
    elif sdc_item.long_description:
        overview_text = sdc_item.long_description
    
    if overview_text:
        html_parts.append('<p><strong>Overview:</strong></p>')
        html_parts.append('<p>{}</p>'.format(overview_text))
    
    # Add extended description if different from overview
    if sdc_item.extended_description and sdc_item.extended_description != overview_text:
        html_parts.append('<p>{}</p>'.format(sdc_item.extended_description))
    
    # Add features and benefits - split by semicolons and format as list
    if sdc_item.features_and_benefits:
        html_parts.append('<p><strong>Features and Benefits:</strong></p>')
        # Split by semicolon and strip whitespace from each item
        features_list = [feature.strip() for feature in sdc_item.features_and_benefits.split(';') if feature.strip()]
        if features_list:
            html_parts.append('<ul>')
            for feature in features_list:
                html_parts.append('<li>{}</li>'.format(feature))
            html_parts.append('</ul>')
    
    # Add application summary if available
    if sdc_item.application_summary:
        html_parts.append('<p><strong>Application Summary:</strong></p>')
        html_parts.append('<p>{}</p>'.format(sdc_item.application_summary))
    
    # Add Quick Specs section if available
    quick_specs = _get_sdc_quick_specs(sdc_item)
    if quick_specs:
        html_parts.append(quick_specs)
    
    # Add Important Notes section if available
    important_notes = _get_sdc_important_notes(sdc_item)
    if important_notes:
        html_parts.append('<p><strong>Important Notes:</strong></p>')
        html_parts.append(important_notes)
    
    # Add Instructions section if available
    instruction_link = _get_sdc_instruction_link(sdc_item)
    if instruction_link:
        html_parts.append('<p><strong>Instructions:</strong></p>')
        html_parts.append('<p>{}</p>'.format(instruction_link))
    
    # Add fitment table at the end if fitments are provided
    if fitments:
        fitment_table = _get_sdc_fitment_table(fitments)
        if fitment_table:
            html_parts.append(fitment_table)
    
    return ''.join(html_parts) if html_parts else ''


def _get_sdc_quick_specs(sdc_item: src_models.SDCParts) -> typing.Optional[str]:
    """
    Generate Quick Specs section from SDC part data.
    Returns HTML string or None if no specs are available.
    """
    specs = []
    
    # Section 1: Product Attributes (split by ';')
    if sdc_item.product_attributes:
        # Split by semicolon and format as key: value pairs
        attributes_list = []
        for attribute in sdc_item.product_attributes.split(';'):
            attribute = attribute.strip()
            if not attribute:
                continue
            # Split by colon to separate key and value
            if ':' in attribute:
                parts = attribute.split(':', 1)
                formatted = ': '.join([part.strip() for part in parts])
            else:
                formatted = attribute
            attributes_list.append(formatted)
        
        if attributes_list:
            specs.append('<ul><li>' + '</li><li>'.join(attributes_list) + '</li></ul>')
    
    # Section 2: Additional Fields
    additional_specs = []
    
    # Quantity per Application
    if sdc_item.quantity_per_application:
        additional_specs.append('<li>Quantity per Application: {}</li>'.format(sdc_item.quantity_per_application))
    
    # Country of Origin
    if sdc_item.country_of_origin:
        additional_specs.append('<li>Country of Origin: {}</li>'.format(sdc_item.country_of_origin))
    
    # Warranty
    if sdc_item.warranty:
        additional_specs.append('<li>Warranty: {}</li>'.format(sdc_item.warranty))
    
    # Dimensions (Length x Width x Height)
    dimensions_parts = []
    if sdc_item.length_for_case is not None:
        dimensions_parts.append(str(sdc_item.length_for_case))
    if sdc_item.width_for_case is not None:
        dimensions_parts.append(str(sdc_item.width_for_case))
    if sdc_item.height_for_case is not None:
        dimensions_parts.append(str(sdc_item.height_for_case))
    
    if dimensions_parts:
        dimensions_str = ' x '.join(dimensions_parts)
        additional_specs.append('<li>Length (EA) x Width (EA) x Height (EA): {}</li>'.format(dimensions_str))
    
    # Weight
    if sdc_item.weight_for_case is not None:
        additional_specs.append('<li>Weight (lbs): {}</li>'.format(sdc_item.weight_for_case))
    
    # Vehicle Specific Fitment - we'll need to query fitments separately
    # For now, we'll skip this as it requires additional query
    
    # Add the additional fields to the specs if not empty
    if additional_specs:
        specs.append('<p><strong>Additional Specifications:</strong></p>')
        specs.append('<ul>' + ''.join(additional_specs) + '</ul>')
    
    # Only return content if there are actual specs, and add title at the beginning
    if specs:
        specs.insert(0, '<p><strong>Quick Specs:</strong></p>')
        return ''.join(specs)
    
    return None


def _format_to_list(field_value: typing.Optional[str]) -> typing.Optional[str]:
    """
    Format field value as an HTML list.
    Splits by semicolons and formats as list items.
    Returns None if field_value is empty or None.
    """
    if not field_value or not isinstance(field_value, str):
        return None
    
    field_value = field_value.strip()
    if not field_value:
        return None
    
    # Split by semicolon and strip whitespace from each item
    items = [item.strip() for item in field_value.split(';') if item.strip()]
    
    if not items:
        return None
    
    # Format as HTML list
    return '<ul><li>' + '</li><li>'.join(items) + '</li></ul>'


def _get_sdc_important_notes(sdc_item: src_models.SDCParts) -> typing.Optional[str]:
    """
    Get important notes from SDC part (Associated Comments).
    Returns formatted HTML list or None if not available.
    """
    # Note: "Associated Comments" field may need to be added to SDCParts model
    # For now, checking if there's a similar field or making it flexible
    associated_comments = getattr(sdc_item, 'associated_comments', None) or ''
    return _format_to_list(associated_comments)


def _get_sdc_instruction_link(sdc_item: src_models.SDCParts) -> typing.Optional[str]:
    """
    Get installation instruction link from SDC part.
    Returns HTML link if installation_instructions contains a valid URL, otherwise None.
    """
    instruction_link = sdc_item.installation_instructions
    
    if not instruction_link or not isinstance(instruction_link, str):
        return None
    
    instruction_link = instruction_link.strip()
    
    # Check if it contains a URL
    if 'https' not in instruction_link:
        return None
    
    # Extract filename from URL or use a default
    # Try to get filename from the URL path
    try:
        from urllib.parse import urlparse
        parsed_url = urlparse(instruction_link)
        filename = parsed_url.path.split('/')[-1] if parsed_url.path else 'Installation Instructions'
        # If filename is empty or just a slash, use default
        if not filename or filename == '/':
            filename = 'Installation Instructions'
    except Exception:
        filename = 'Installation Instructions'
    
    return '<a href="{}" target="_blank">{}</a>'.format(instruction_link, filename)


def _get_sdc_images(sdc_item: src_models.SDCParts) -> list:
    """Get images from SDC part. Returns list of image dicts with is_thumbnail and image_url."""
    images = []
    
    def _encode_image_url(url: str) -> str:
        """URL encode the image URL, preserving the URL structure."""
        if not url:
            return url
        try:
            # Parse the URL
            parsed = urlparse(url)
            # Encode the path component
            encoded_path = '/'.join(quote(segment, safe='') for segment in parsed.path.split('/'))
            # Reconstruct the URL with encoded path
            encoded_url = urlunparse((
                parsed.scheme,
                parsed.netloc,
                encoded_path,
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
            return encoded_url
        except Exception:
            # If encoding fails, return original URL
            return url
    
    # Primary image is the thumbnail
    if sdc_item.primary_image:
        encoded_url = _encode_image_url(sdc_item.primary_image)
        images.append({
            'is_thumbnail': True,
            'image_url': encoded_url,
            'description': '',
        })
    
    # Additional images
    if sdc_item.additional_image:
        encoded_url = _encode_image_url(sdc_item.additional_image)
        images.append({
            'is_thumbnail': False,
            'image_url': encoded_url,
            'description': '',
        })
    
    return images


def _get_sdc_inventory(sdc_item: src_models.SDCParts) -> int:
    """Get inventory from SDC part."""
    if sdc_item.inventory is not None:
        try:
            return int(sdc_item.inventory)
        except (ValueError, TypeError):
            pass
    return 0


def _get_sdc_fitment_table(fitments: typing.List[src_models.SDCPartFitment]) -> typing.Optional[str]:
    """
    Create HTML table from fitment data.
    Returns HTML string or None if no fitments.
    """
    if not fitments:
        return None
    
    html_parts = []
    html_parts.append('<p><strong>Vehicle Fitment:</strong></p>')
    html_parts.append('<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">')
    html_parts.append('<thead><tr><th style="text-align: center;">Year</th><th style="text-align: center;">Make</th><th style="text-align: center;">Model</th></tr></thead>')
    html_parts.append('<tbody>')
    
    for fitment in fitments:
        html_parts.append('<tr>')
        html_parts.append('<td style="text-align: center;">{}</td>'.format(fitment.year))
        html_parts.append('<td style="text-align: center;">{}</td>'.format(fitment.make))
        html_parts.append('<td style="text-align: center;">{}</td>'.format(fitment.model))
        html_parts.append('</tr>')
    
    html_parts.append('</tbody>')
    html_parts.append('</table>')
    
    return ''.join(html_parts)


def _get_sdc_fitment_custom_field(fitments: typing.List[src_models.SDCPartFitment]) -> typing.Optional[typing.Dict]:
    """
    Create custom field with fitment data as list of dicts (JSON).
    Returns custom field dict or None if no fitments.
    """
    if not fitments:
        return None
    
    # Convert fitments to list of dicts
    fitment_data = []
    for fitment in fitments:
        fitment_data.append({
            'year': fitment.year,
            'make': fitment.make,
            'model': fitment.model,
        })
    
    # Create custom field with fitment data as JSON string
    return {
        'name': 'Vehicle Fitment',
        'value': json.dumps(fitment_data),
    }


def _map_turn14_to_pcdb_category(
    turn14_category: typing.Optional[str],
    turn14_subcategory: typing.Optional[str]
) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
    if not turn14_category or not turn14_subcategory:
        return (turn14_category, turn14_subcategory)
    
    # Look up in the mapping dictionary
    mapping_key = (turn14_category, turn14_subcategory)
    pcdb_mapping = src_constants.TURN14_TO_PCDB_CATEGORY_MAP.get(mapping_key)
    
    if pcdb_mapping:
        return pcdb_mapping
    
    # If no mapping found, return original values
    return (turn14_category, turn14_subcategory)


def prepare_turn_14_products_for_bigcommerce(brand: src_models.Brands) -> list[src_messages.BigCommercePart]:
    bigcommerce_parts = []
    turn_14_brand = src_models.BrandTurn14BrandMapping.objects.get(brand_id=brand.id)
    turn_14_items = src_models.Turn14Items.objects.filter(
        brand_id=turn_14_brand.turn14_brand_id
    )

    if not turn_14_items:
        logger.info('{} No turn 14 items found for brand {}.'.format(_LOG_PREFIX, brand.name))
        return []

    bigcommerce_brand = src_models.BigCommerceBrands.objects.get(brand_id=brand.id)
    turn_14_item_data = {
        item_data.external_id: item_data for item_data in src_models.Turn14BrandData.objects.filter(brand_id=turn_14_brand.turn14_brand_id)
    }
    turn_14_item_pricing = {
        item_data.external_id: item_data for item_data in src_models.Turn14BrandPricing.objects.filter(brand_id=turn_14_brand.turn14_brand_id)
    }
    turn_14_item_inventory = {
        item_data.external_id: item_data for item_data in src_models.Turn14BrandInventory.objects.filter(brand_id=turn_14_brand.turn14_brand_id)
    }
    for turn_14_item in turn_14_items:
        turn_14_pricing = turn_14_item_pricing.get(turn_14_item.external_id, None)
        if not turn_14_pricing:
            logger.info('{} No pricing found for item {}. Skipping'.format(_LOG_PREFIX, turn_14_item.external_id))
            continue

        turn_14_data = turn_14_item_data.get(turn_14_item.external_id, None)
        if not turn_14_data:
            logger.info('{} No data found for item {}. Skipping'.format(_LOG_PREFIX, turn_14_item.external_id))
            continue

        turn_14_inventory = turn_14_item_inventory.get(turn_14_item.external_id, None)
        if not turn_14_data:
            logger.info('{} No inventory found for item {}. Skipping'.format(_LOG_PREFIX, turn_14_item.external_id))
            continue


        default_price, msrp = _get_turn_14_prices(turn_14_pricing)
        cost = _get_turn_14_cost(turn_14_pricing)
        width, height, depth = _get_turn_14_dimensions(turn_14_item=turn_14_item)
        
        # Map Turn14 categories to PCDB categories
        pcdb_category, pcdb_subcategory = _map_turn14_to_pcdb_category(
            turn14_category=turn_14_item.category,
            turn14_subcategory=turn_14_item.subcategory
        )
        
        bigcommerce_parts.append(
            src_messages.BigCommercePart(
                brand_id=int(bigcommerce_brand.external_id),
                product_title='{} - {}'.format(turn_14_item.part_description, turn_14_item.mfr_part_number),
                sku=turn_14_item.mfr_part_number,
                mpn=turn_14_item.mfr_part_number,
                default_price=default_price,
                cost=cost,
                msrp=msrp,
                weight=_get_turn_14_weight(turn_14_item=turn_14_item),
                width=width,
                height=height,
                depth=depth,
                description=_get_turn_14_description(turn_14_data=turn_14_data),
                images=_get_turn_14_images(turn_14_item=turn_14_item, turn_14_data=turn_14_data),
                inventory=_get_turn_14_inventory(turn_14_inventory=turn_14_inventory),
                custom_fields=[],
                active=turn_14_item.active,
                category=pcdb_category,
                subcategory=pcdb_subcategory,
            )
        )

    return bigcommerce_parts


def _get_turn_14_prices(
    turn_14_pricing: src_models.Turn14BrandPricing,
    jobber_markup: float = 0.30,
) -> typing.Tuple[float, float]:
    default_price = 0.0
    msrp = 0.0

    if not turn_14_pricing.pricelists:
        return default_price, msrp

    map_price = None
    retail_price = None
    msrp_price = None
    jobber_price = None

    for pricelist_item in turn_14_pricing.pricelists:
        if not isinstance(pricelist_item, dict):
            continue

        pricelist_name = pricelist_item.get("name")
        price_value = pricelist_item.get("price")

        if price_value is None:
            continue

        try:
            price_float = float(price_value)

            if pricelist_name == "MAP":
                map_price = price_float
            elif pricelist_name == "Retail":
                retail_price = price_float
            elif pricelist_name == "MSRP":
                msrp_price = price_float
            elif pricelist_name == "Jobber":
                jobber_price = price_float

        except (ValueError, TypeError):
            continue

    # --- DEFAULT PRICE (what you show publicly) ---
    if map_price is not None:
        default_price = map_price
    elif retail_price is not None:
        default_price = retail_price
    elif msrp_price is not None:
        default_price = msrp_price
    elif jobber_price is not None:
        # Jobber-only brand → apply markup
        default_price = round(jobber_price / (1 - jobber_markup), 2)
    else:
        default_price = 0.0

    # --- MSRP / COMPARE-AT PRICE ---
    if retail_price is not None:
        msrp = retail_price
    elif msrp_price is not None:
        msrp = msrp_price
    else:
        msrp = 0.0

    return default_price, msrp



def _get_turn_14_cost(turn_14_pricing: src_models.Turn14BrandPricing) -> float:
    if turn_14_pricing.purchase_cost is not None:
        try:
            return float(turn_14_pricing.purchase_cost)
        except (ValueError, TypeError):
            pass
    return 0.0

def _get_turn_14_weight(turn_14_item: src_models.Turn14Items) -> float:
    weight = 0.0

    if not turn_14_item.dimensions:
        return weight

    weight_in_lbs = turn_14_item.dimensions[0].get('weight', 0)

    return weight_in_lbs * 16

def _get_turn_14_dimensions(turn_14_item: src_models.Turn14Items) -> typing.Tuple[typing.Optional[float], typing.Optional[float], typing.Optional[float]]:
    """
    Extract width, height, and depth (length) from dimensions array.
    Looks for box_number=1, if not found returns None for all.
    Returns: (width, height, depth)
    """
    if not turn_14_item.dimensions or not isinstance(turn_14_item.dimensions, list):
        return (None, None, None)
    
    # Find dimension with box_number=1
    dimension_with_box_1 = None
    for dim in turn_14_item.dimensions:
        if isinstance(dim, dict) and dim.get('box_number') == 1:
            dimension_with_box_1 = dim
            break
    
    if not dimension_with_box_1:
        return (None, None, None)
    
    width = dimension_with_box_1.get('width')
    height = dimension_with_box_1.get('height')
    length = dimension_with_box_1.get('length')  # length is depth in BigCommerce
    
    # Convert to float if they exist, otherwise None
    try:
        width = float(width) if width is not None else None
    except (ValueError, TypeError):
        width = None
    
    try:
        height = float(height) if height is not None else None
    except (ValueError, TypeError):
        height = None
    
    try:
        depth = float(length) if length is not None else None
    except (ValueError, TypeError):
        depth = None
    
    return (width, height, depth)

def _get_turn_14_instruction_link(turn_14_data: src_models.Turn14BrandData) -> typing.Optional[str]:
    """
    Get installation instruction link from Turn 14 files.
    Looks for files with type='Other' and media_content in this order:
    1. 'Installation Instructions'
    2. 'Illustration Guide'
    Returns HTML link if found, otherwise None.
    """
    if not turn_14_data.files or not isinstance(turn_14_data.files, list):
        return None
    
    # Priority order: Installation Instructions -> Illustration Guide
    media_content_options = ['Installation Instructions', 'Illustration Guide']
    
    for media_content_option in media_content_options:
        for file in turn_14_data.files:
            if not isinstance(file, dict):
                continue
            
            file_type = file.get('type', '')
            media_content = file.get('media_content', '')
            
            if file_type == 'Other' and media_content == media_content_option:
                links = file.get('links', [])
                if not links or not isinstance(links, list):
                    continue
                
                # Get the first link's URL
                first_link = links[0] if links else None
                if not first_link or not isinstance(first_link, dict):
                    continue
                
                instruction_url = first_link.get('url', '').strip()
                if not instruction_url:
                    continue
                
                return '<a href="{}" target="_blank">Installation Instructions</a>'.format(instruction_url)
    
    return None


def _get_turn_14_owners_manual_link(turn_14_data: src_models.Turn14BrandData) -> typing.Optional[str]:
    """
    Get Owner's Manual link from Turn 14 files.
    Looks for files with type='Other' and media_content='Owner's Manual'.
    Returns HTML link if found, otherwise None.
    """
    if not turn_14_data.files or not isinstance(turn_14_data.files, list):
        return None
    
    for file in turn_14_data.files:
        if not isinstance(file, dict):
            continue
        
        file_type = file.get('type', '')
        media_content = file.get('media_content', '')
        
        if file_type == 'Other' and media_content == "Owner's Manual":
            links = file.get('links', [])
            if not links or not isinstance(links, list):
                continue
            
            # Get the first link's URL
            first_link = links[0] if links else None
            if not first_link or not isinstance(first_link, dict):
                continue
            
            owners_manual_url = first_link.get('url', '').strip()
            if not owners_manual_url:
                continue
            
            return '<a href="{}" target="_blank">Owner\'s Manual</a>'.format(owners_manual_url)
    
    return None


def _get_turn_14_warranty_link(turn_14_data: src_models.Turn14BrandData) -> typing.Optional[str]:
    """
    Get Warranty link from Turn 14 files.
    Looks for files with type='Other' and media_content='Warranty'.
    Returns HTML link if found, otherwise None.
    """
    if not turn_14_data.files or not isinstance(turn_14_data.files, list):
        return None
    
    for file in turn_14_data.files:
        if not isinstance(file, dict):
            continue
        
        file_type = file.get('type', '')
        media_content = file.get('media_content', '')
        
        if file_type == 'Other' and media_content == 'Warranty':
            links = file.get('links', [])
            if not links or not isinstance(links, list):
                continue
            
            # Get the first link's URL
            first_link = links[0] if links else None
            if not first_link or not isinstance(first_link, dict):
                continue
            
            warranty_url = first_link.get('url', '').strip()
            if not warranty_url:
                continue
            
            return '<a href="{}" target="_blank">Warranty</a>'.format(warranty_url)
    
    return None


def _get_turn_14_description(turn_14_data: src_models.Turn14BrandData) -> str:
    """
    Format descriptions as HTML with Overview section, Features and Benefits list, Important Notes, Installation Instructions, Owner's Manual, and Warranty.
    """
    if not turn_14_data.descriptions or not isinstance(turn_14_data.descriptions, list):
        return ''
    
    overview_parts = []
    features_and_benefits = []
    associated_comments = []
    
    for turn_14_desc in turn_14_data.descriptions:
        if not isinstance(turn_14_desc, dict):
            continue
        
        desc_type = turn_14_desc.get('type', '')
        desc_text = turn_14_desc.get('description', '')
        
        if not desc_text:
            continue
        
        if desc_type == 'Market Description' or desc_type == 'Product Description - Extended':
            overview_parts.append(desc_text)
        elif desc_type == 'Features and Benefits':
            features_and_benefits.append(desc_text)
        elif desc_type == 'Associated Comments':
            associated_comments.append(desc_text)
    
    html_parts = []
    
    # Add Overview section if we have overview content
    if overview_parts:
        overview_html = '<p><strong>Overview:</strong></p>'
        for overview_text in overview_parts:
            overview_html += '<p>{}</p>'.format(overview_text)
        html_parts.append(overview_html)
    
    # Add Features and Benefits list if we have any
    if features_and_benefits:
        features_html = '<p><strong>Features and Benefits:</strong></p><ul>'
        for feature_text in features_and_benefits:
            features_html += '<li>{}</li>'.format(feature_text)
        features_html += '</ul>'
        html_parts.append(features_html)
    
    # Add Important Notes section if we have Associated Comments
    if associated_comments:
        # Collect all items from all Associated Comments
        # Each comment can be a separate item, or split by ';' if it contains semicolons
        important_notes_items = []
        for comment in associated_comments:
            if not isinstance(comment, str):
                continue
            comment = comment.strip()
            if not comment:
                continue
            
            # Split by semicolon if present, otherwise treat as single item
            if ';' in comment:
                # Split by semicolon and add each item
                items = [item.strip() for item in comment.split(';') if item.strip()]
                important_notes_items.extend(items)
            else:
                # Single item, add as-is
                important_notes_items.append(comment)
        
        if important_notes_items:
            important_notes_html = '<p><strong>Important Notes:</strong></p><ul>'
            for note_item in important_notes_items:
                important_notes_html += '<li>{}</li>'.format(note_item)
            important_notes_html += '</ul>'
            html_parts.append(important_notes_html)
    
    # Add Installation Instructions section if available
    instruction_link = _get_turn_14_instruction_link(turn_14_data)
    if instruction_link:
        html_parts.append('<p><strong>Instructions:</strong></p>')
        html_parts.append('<p>{}</p>'.format(instruction_link))
    
    # Add Owner's Manual section if available
    owners_manual_link = _get_turn_14_owners_manual_link(turn_14_data)
    if owners_manual_link:
        html_parts.append('<p><strong>Owner\'s Manual:</strong></p>')
        html_parts.append('<p>{}</p>'.format(owners_manual_link))
    
    # Add Warranty section if available
    warranty_link = _get_turn_14_warranty_link(turn_14_data)
    if warranty_link:
        html_parts.append('<p><strong>Warranty:</strong></p>')
        html_parts.append('<p>{}</p>'.format(warranty_link))
    
    return ''.join(html_parts)

def _get_turn_14_images(turn_14_item: src_models.Turn14Items, turn_14_data: src_models.Turn14BrandData) -> list:
    images = []
    # if turn_14_item.thumbnail:
    #     images.append(
    #         {
    #             'is_thumbnail': True,
    #             'image_url': turn_14_item.thumbnail,
    #             'description': '',
    #         }
    #     )

    if not turn_14_data.files:
        return images

    # Excluded media_content types that should NOT be thumbnails
    excluded_media_content_types = [
        'Photo - Close Up',
        'Photo - Mounted',
        'Photo - Unmounted',
        'Photo - out of package',
        'Logo Image'
    ]

    # First, collect all valid images
    image_candidates = []
    for file in turn_14_data.files:
        if file.get('type') == 'Image':
            if not file.get('links'):
                continue

            media_content = file.get('media_content', '')
            image_url = file.get('links', [])[0].get('url', '')
            
            if not image_url:
                continue
            
            image_candidates.append({
                'image_url': image_url,
                'media_content': media_content,
            })
    
    # Find the first image that should be thumbnail (priority: Photo - Primary, then non-excluded)
    thumbnail_index = None
    
    # First, try to find 'Photo - Primary'
    for idx, candidate in enumerate(image_candidates):
        if candidate['media_content'] == 'Photo - Primary':
            thumbnail_index = idx
            break
    
    # If no 'Photo - Primary' found, find first non-excluded image
    if thumbnail_index is None:
        for idx, candidate in enumerate(image_candidates):
            if candidate['media_content'] not in excluded_media_content_types:
                thumbnail_index = idx
                break
    
    # Build images list, setting thumbnail flag only for the first matching image
    for idx, candidate in enumerate(image_candidates):
        is_thumbnail = (idx == thumbnail_index) if thumbnail_index is not None else False
        
        images.append(
            {
                'is_thumbnail': is_thumbnail,
                'image_url': candidate['image_url'],
                'description': '',
            }
        )

    return images

def _get_turn_14_inventory(turn_14_inventory: src_models.Turn14BrandInventory) -> int:
    if not turn_14_inventory.inventory:
        return 0
    if isinstance(turn_14_inventory.inventory, dict):
        return sum(int(v) for v in turn_14_inventory.inventory.values() if isinstance(v, (int, float, str)))
    return turn_14_inventory.total_inventory or 0

def _get_availability_text(quantity: int) -> str:
    """
    Get availability text based on inventory quantity.
    
    Rules:
    - quantity >= 5: "In Stock"
    - quantity >= 1: "Low (Live-Chat or Call For Stock)"
    - quantity < 1: "Special Order (Live Chat or Call)"
    """
    if quantity >= 5:
        return "In Stock"
    if quantity >= 1:
        return "Low (Live-Chat or Call For Stock)"
    return "Special Order (Live Chat or Call)"


def _categorize_products_for_sync(
    products_for_sync: typing.List[src_messages.BigCommercePart],
    destination: src_models.CompanyDestinations,
    brand: src_models.Brands
) -> typing.Tuple[typing.List[typing.Tuple], typing.List[typing.Tuple]]:
    products_to_update = []
    products_to_create = []

    if not products_for_sync:
        return products_to_update, products_to_create

    # Extract all SKUs for bulk querying
    all_skus = [product_to_sync.sku for product_to_sync in products_for_sync]

    # Bulk fetch all BigCommerceParts in one query
    bigcommerce_parts_dict = {
        part.sku: part
        for part in src_models.BigCommerceParts.objects.filter(
            sku__in=all_skus,
            company_destination=destination
        )
    }

    # Bulk fetch all CompanyDestinationParts in one query
    # Note: Using first() behavior - if multiple exist, we take the first one
    company_destination_parts_dict = {}
    for part in src_models.CompanyDestinationParts.objects.filter(
        part_unique_key__in=all_skus,
        company_destination=destination,
        brand=brand
    ):
        # Only keep the first occurrence of each SKU (matching original .first() behavior)
        if part.part_unique_key not in company_destination_parts_dict:
            company_destination_parts_dict[part.part_unique_key] = part

    # Categorize products using the pre-fetched dictionaries
    for product_to_sync in products_for_sync:
        bigcommerce_part = bigcommerce_parts_dict.get(product_to_sync.sku)
        company_destination_part = company_destination_parts_dict.get(product_to_sync.sku)

        if company_destination_part:
            products_to_update.append((product_to_sync, bigcommerce_part, company_destination_part))
        else:
            products_to_create.append((product_to_sync, company_destination_part))

    return products_to_update, products_to_create


def _is_retryable_error(error: Exception) -> bool:
    """
    Determine if an error is retryable (transient errors that might succeed on retry).
    
    Args:
        error: The exception to check
        
    Returns:
        True if the error is retryable, False otherwise
    """
    # Check for BigCommerce API exceptions with status codes
    if isinstance(error, bigcommerce_exceptions.BigCommerceAPIBadResponseCodeError):
        # Check the status code directly
        status_code = getattr(error, 'code', None)
        if status_code:
            # Retry on server errors (5xx) and rate limits (429)
            if status_code >= 500 and status_code < 600:
                return True
            if status_code == 429:  # Rate limit
                return True
            # Don't retry on client errors (4xx) except rate limits
            if status_code >= 400 and status_code < 500:
                return False
    
    # Check for BigCommerce API exceptions (general)
    if isinstance(error, bigcommerce_exceptions.BigCommerceAPIException):
        # Retry on rate limits, server errors (5xx), and timeouts
        error_str = str(error).lower()
        if 'rate limit' in error_str or '429' in error_str:
            return True
        if 'timeout' in error_str or 'timed out' in error_str:
            return True
        if any(code in error_str for code in ['500', '502', '503', '504']):
            return True
        # Don't retry on client errors (4xx) except rate limits
        if '400' in error_str or '401' in error_str or '403' in error_str or '404' in error_str:
            return False
    
    # Retry on connection errors, timeouts, and other transient network issues
    error_type = type(error).__name__
    retryable_types = ['ConnectionError', 'TimeoutError', 'HTTPError', 'URLError']
    if any(retryable_type in error_type for retryable_type in retryable_types):
        return True
    
    # Default to not retryable for unknown errors
    return False


def _process_product_update_with_retry(
    product_to_sync: src_messages.BigCommercePart,
    bigcommerce_part: src_models.BigCommerceParts,
    company_destination_part: typing.Optional[src_models.CompanyDestinationParts],
    destination: src_models.CompanyDestinations,
    brand: src_models.Brands,
    api_client: bigcommerce_client.BigCommerceApiClient,
    execution_run: src_models.CompanyDestinationExecutionRun,
    counters: typing.Dict
) -> bool:
    """
    Process product update with retry logic and thread-safe counter updates.
    Retries on transient API errors (rate limits, timeouts, etc.).
    """
    # Add small delay to stagger parallel requests and avoid rate limiting
    delay = _PARALLEL_REQUEST_DELAY + random.uniform(0, _PARALLEL_REQUEST_DELAY_JITTER)
    time.sleep(delay)
    
    last_exception = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            success = _update_product_on_bigcommerce(
                product_to_sync=product_to_sync,
                bigcommerce_part=bigcommerce_part,
                company_destination_part=company_destination_part,
                destination=destination,
                brand=brand,
                api_client=api_client,
                execution_run=execution_run
            )
            
            # Update counters thread-safely
            with counters['lock']:
                counters['processed'] += 1
                if success:
                    counters['updated'] += 1
                else:
                    counters['failed'] += 1
            
            return success
            
        except (bigcommerce_exceptions.BigCommerceAPIException, Exception) as e:
            last_exception = e
            # Check if error is retryable (rate limit, timeout, server error)
            is_retryable = _is_retryable_error(e)
            
            if attempt < _MAX_RETRIES and is_retryable:
                delay = min(_RETRY_BASE_DELAY * (2 ** attempt), _RETRY_MAX_DELAY)
                
                # Add extra delay for 500 errors (server overload)
                if isinstance(e, bigcommerce_exceptions.BigCommerceAPIBadResponseCodeError):
                    status_code = getattr(e, 'code', None)
                    if status_code and status_code >= 500 and status_code < 600:
                        delay += _SERVER_ERROR_RETRY_DELAY
                
                logger.warning('{} Retry attempt {}/{} for product update (sku={}) after {}s. Error: {}.'.format(
                    _LOG_PREFIX, attempt + 1, _MAX_RETRIES, product_to_sync.sku, delay, str(e)
                ))
                time.sleep(delay)
            else:
                # Non-retryable error or max retries exceeded
                logger.error('{} Failed to update product (sku={}) after {} attempts. Error: {}.'.format(
                    _LOG_PREFIX, product_to_sync.sku, attempt + 1, str(e)
                ))
                with counters['lock']:
                    counters['processed'] += 1
                    counters['failed'] += 1
                return False
    
    # Should not reach here, but handle it just in case
    logger.error('{} Unexpected error in retry loop for product update (sku={}).'.format(
        _LOG_PREFIX, product_to_sync.sku
    ))
    with counters['lock']:
        counters['processed'] += 1
        counters['failed'] += 1
    return False


def _process_product_create_with_retry(
    product_to_sync: src_messages.BigCommercePart,
    company_destination_part: typing.Optional[src_models.CompanyDestinationParts],
    destination: src_models.CompanyDestinations,
    brand: src_models.Brands,
    api_client: bigcommerce_client.BigCommerceApiClient,
    execution_run: src_models.CompanyDestinationExecutionRun,
    counters: typing.Dict
) -> bool:
    """
    Process product create with retry logic and thread-safe counter updates.
    Retries on transient API errors (rate limits, timeouts, etc.).
    """
    # Add small delay to stagger parallel requests and avoid rate limiting
    delay = _PARALLEL_REQUEST_DELAY + random.uniform(0, _PARALLEL_REQUEST_DELAY_JITTER)
    time.sleep(delay)
    
    last_exception = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            success = _create_product_on_bigcommerce(
                product_to_sync=product_to_sync,
                company_destination_part=company_destination_part,
                destination=destination,
                brand=brand,
                api_client=api_client,
                execution_run=execution_run
            )
            
            # Update counters thread-safely
            with counters['lock']:
                counters['processed'] += 1
                if success:
                    counters['created'] += 1
                else:
                    counters['failed'] += 1
            
            return success
            
        except (bigcommerce_exceptions.BigCommerceAPIException, Exception) as e:
            last_exception = e
            # Check if error is retryable (rate limit, timeout, server error)
            is_retryable = _is_retryable_error(e)
            
            if attempt < _MAX_RETRIES and is_retryable:
                delay = min(_RETRY_BASE_DELAY * (2 ** attempt), _RETRY_MAX_DELAY)
                
                # Add extra delay for 500 errors (server overload)
                if isinstance(e, bigcommerce_exceptions.BigCommerceAPIBadResponseCodeError):
                    status_code = getattr(e, 'code', None)
                    if status_code and status_code >= 500 and status_code < 600:
                        delay += _SERVER_ERROR_RETRY_DELAY
                
                logger.warning('{} Retry attempt {}/{} for product create (sku={}) after {}s. Error: {}.'.format(
                    _LOG_PREFIX, attempt + 1, _MAX_RETRIES, product_to_sync.sku, delay, str(e)
                ))
                time.sleep(delay)
            else:
                # Non-retryable error or max retries exceeded
                logger.error('{} Failed to create product (sku={}) after {} attempts. Error: {}.'.format(
                    _LOG_PREFIX, product_to_sync.sku, attempt + 1, str(e)
                ))
                with counters['lock']:
                    counters['processed'] += 1
                    counters['failed'] += 1
                return False
    
    # Should not reach here, but handle it just in case
    logger.error('{} Unexpected error in retry loop for product create (sku={}).'.format(
        _LOG_PREFIX, product_to_sync.sku
    ))
    with counters['lock']:
        counters['processed'] += 1
        counters['failed'] += 1
    return False


def _get_shop_all_category_id(
    destination: src_models.CompanyDestinations
) -> typing.Optional[int]:
    """
    Get the "Shop All" category ID from the database.
    Returns None if not found.
    """
    try:
        shop_all_category = src_models.BigCommerceCategories.objects.get(
            name='Shop All',
            company_destination=destination,
            tree_id=1
        )
        return shop_all_category.external_id
    except src_models.BigCommerceCategories.DoesNotExist:
        logger.warning('{} "Shop All" category not found in database for destination: {}.'.format(
            _LOG_PREFIX, destination.id
        ))
        return None
    except src_models.BigCommerceCategories.MultipleObjectsReturned:
        # If multiple exist, take the first one
        shop_all_category = src_models.BigCommerceCategories.objects.filter(
            name='Shop All',
            company_destination=destination,
            tree_id=1
        ).first()
        if shop_all_category:
            return shop_all_category.external_id
        return None


def _get_vehicles_category_id(
    destination: src_models.CompanyDestinations,
    api_client: bigcommerce_client.BigCommerceApiClient
) -> typing.Optional[int]:
    """
    Get or create the "Vehicles" category (parent category for vehicle hierarchy).
    Returns the category external_id (BigCommerce category ID) or None if creation fails.
    """
    return _get_or_create_bigcommerce_category(
        category_name='Vehicles',
        parent_id=0,
        destination=destination,
        api_client=api_client,
        tree_id=1
    )


def _build_vehicle_hierarchy_from_fitments(
    fitments: typing.List[typing.Dict],
    destination: src_models.CompanyDestinations,
    api_client: bigcommerce_client.BigCommerceApiClient
) -> typing.List[int]:
    """
    Build vehicle category hierarchy from fitments and return Model category IDs.
    
    Hierarchy: Vehicles -> Year -> Make -> Model
    Product should be assigned to Model categories (deepest level).
    
    Args:
        fitments: List of fitment dicts with 'year', 'make', 'model' keys
        destination: Company destination
        api_client: BigCommerce API client
        
    Returns:
        List of Model category IDs (one for each unique year/make/model combination)
    """
    if not fitments:
        return []
    
    # Get or create Vehicles parent category
    vehicles_category_id = _get_vehicles_category_id(destination, api_client)
    if not vehicles_category_id:
        logger.warning('{} Failed to get or create Vehicles category. Skipping fitment hierarchy.'.format(
            _LOG_PREFIX
        ))
        return []
    
    model_category_ids = []
    processed_combinations = set()
    
    for fitment in fitments:
        if not isinstance(fitment, dict):
            continue
            
        year = fitment.get('year')
        make = fitment.get('make')
        model = fitment.get('model')
        
        # Skip if any required field is missing
        if not year or not make or not model:
            continue
        
        # Convert to strings and normalize
        year_str = str(year).strip()
        make_str = str(make).strip()
        model_str = str(model).strip()
        
        if not year_str or not make_str or not model_str:
            continue
        
        # Create unique key for this combination
        combination_key = (year_str, make_str, model_str)
        if combination_key in processed_combinations:
            continue
        
        processed_combinations.add(combination_key)
        
        try:
            # Get or create Year category (child of Vehicles)
            year_category_id = _get_or_create_bigcommerce_category(
                category_name=year_str,
                parent_id=vehicles_category_id,
                destination=destination,
                api_client=api_client,
                tree_id=1
            )
            if not year_category_id:
                logger.warning('{} Failed to get or create Year category: {}. Skipping fitment.'.format(
                    _LOG_PREFIX, year_str
                ))
                continue
            
            # Get or create Make category (child of Year)
            make_category_id = _get_or_create_bigcommerce_category(
                category_name=make_str,
                parent_id=year_category_id,
                destination=destination,
                api_client=api_client,
                tree_id=1
            )
            if not make_category_id:
                logger.warning('{} Failed to get or create Make category: {} (Year: {}). Skipping fitment.'.format(
                    _LOG_PREFIX, make_str, year_str
                ))
                continue
            
            # Get or create Model category (child of Make) - this is where products are assigned
            model_category_id = _get_or_create_bigcommerce_category(
                category_name=model_str,
                parent_id=make_category_id,
                destination=destination,
                api_client=api_client,
                tree_id=1
            )
            if model_category_id:
                model_category_ids.append(model_category_id)
            else:
                logger.warning('{} Failed to get or create Model category: {} (Make: {}, Year: {}). Skipping fitment.'.format(
                    _LOG_PREFIX, model_str, make_str, year_str
                ))
        except Exception as e:
            logger.warning('{} Error building vehicle hierarchy for fitment (Year: {}, Make: {}, Model: {}). Error: {}. Skipping.'.format(
                _LOG_PREFIX, year_str, make_str, model_str, str(e)
            ))
            continue
    
    return model_category_ids


def _get_or_create_bigcommerce_category(
    category_name: str,
    parent_id: int,
    destination: src_models.CompanyDestinations,
    api_client: bigcommerce_client.BigCommerceApiClient,
    tree_id: int = 1
) -> typing.Optional[int]:
    """
    Get or create a BigCommerce category.
    Returns the category external_id (BigCommerce category ID) or None if creation fails.
    """
    if not category_name:
        return None
    
    # Check if category exists in database
    existing_category = src_models.BigCommerceCategories.objects.filter(
        name=category_name,
        parent_id=parent_id,
        company_destination=destination,
        tree_id=tree_id
    ).first()
    
    if existing_category:
        return existing_category.external_id
    
    # Category doesn't exist, create it via API
    try:
        category_data = [{
            'name': category_name,
            'parent_id': parent_id,
            'tree_id': tree_id,
            'is_visible': True,
        }]
        
        category_response = api_client.create_category(category_data=category_data)
        # Response is an array from the 'data' field, get first item
        if category_response and len(category_response) > 0:
            category_result = category_response[0]
            # BigCommerce returns 'category_id' not 'id'
            external_id = category_result.get('category_id')
            
            if external_id:
                # Extract other fields from response
                response_name = category_result.get('name', category_name)
                response_parent_id = category_result.get('parent_id', parent_id)
                response_tree_id = category_result.get('tree_id', tree_id)
                
                # Save to database
                src_models.BigCommerceCategories.objects.create(
                    external_id=external_id,
                    name=response_name,
                    parent_id=response_parent_id,
                    tree_id=response_tree_id,
                    company_destination=destination,
                )
                logger.info('{} Created new BigCommerce category: {} (id: {}, parent_id: {})'.format(
                    _LOG_PREFIX, response_name, external_id, response_parent_id
                ))
                return external_id
            else:
                logger.error('{} Failed to create BigCommerce category: {}. No category_id returned.'.format(
                    _LOG_PREFIX, category_name
                ))
                return None
        else:
            logger.error('{} Failed to create BigCommerce category: {}. Empty response.'.format(
                _LOG_PREFIX, category_name
            ))
            return None
    except Exception as e:
        logger.error('{} Error creating BigCommerce category: {}. Error: {}.'.format(
            _LOG_PREFIX, category_name, str(e)
        ))
        return None


def _update_product_on_bigcommerce(
    product_to_sync: src_messages.BigCommercePart,
    bigcommerce_part: src_models.BigCommerceParts,
    company_destination_part: typing.Optional[src_models.CompanyDestinationParts],
    destination: src_models.CompanyDestinations,
    brand: src_models.Brands,
    api_client: bigcommerce_client.BigCommerceApiClient,
    execution_run: src_models.CompanyDestinationExecutionRun
) -> bool:
    try:
        logger.info('{} Updating product on BigCommerce (sku={}, external_id={}).'.format(
            _LOG_PREFIX, product_to_sync.sku, bigcommerce_part.external_id
        ))

        product_id = int(bigcommerce_part.external_id)

        # Get old and new custom fields for comparison
        old_custom_fields = []
        if company_destination_part and company_destination_part.destination_data:
            old_custom_fields = company_destination_part.destination_data.get('custom_fields', [])
        
        new_custom_fields = product_to_sync.custom_fields if product_to_sync.custom_fields else []

        # Build maps for comparison (key by name)
        old_fields_map = {}
        for old_field in old_custom_fields:
            if isinstance(old_field, dict):
                field_name = old_field.get('name', '').strip()
                if field_name:
                    old_fields_map[field_name] = old_field
        
        new_fields_map = {}
        for new_field in new_custom_fields:
            if isinstance(new_field, dict):
                field_name = new_field.get('name', '').strip()
                if field_name:
                    new_fields_map[field_name] = new_field
        
        # Prepare custom fields for update payload
        # Include fields that exist in new (for create/update via main payload)
        # Fields that need IDs from old will be merged
        custom_fields_for_payload = []
        for field_name, new_field in new_fields_map.items():
            field_data = {
                'name': field_name,
                'value': new_field.get('value', ''),
            }
            # If field exists in old, include the ID for update
            if field_name in old_fields_map:
                old_field = old_fields_map[field_name]
                field_id = old_field.get('id')
                if field_id:
                    field_data['id'] = field_id
            custom_fields_for_payload.append(field_data)
        
        # Temporarily set custom_fields for the payload
        original_custom_fields = product_to_sync.custom_fields
        product_to_sync.custom_fields = custom_fields_for_payload

        # Get or create categories
        category_ids = []
        if product_to_sync.category:
            category_id = _get_or_create_bigcommerce_category(
                category_name=product_to_sync.category,
                parent_id=0,
                destination=destination,
                api_client=api_client,
                tree_id=1
            )
            if category_id:
                category_ids.append(category_id)
                
                # If subcategory exists, create it as child of category
                if product_to_sync.subcategory:
                    subcategory_id = _get_or_create_bigcommerce_category(
                        category_name=product_to_sync.subcategory,
                        parent_id=category_id,
                        destination=destination,
                        api_client=api_client,
                        tree_id=1
                    )
                    if subcategory_id:
                        category_ids.append(subcategory_id)
        
        # Build vehicle hierarchy from fitments and add Model category IDs
        if product_to_sync.fitments:
            fitment_model_category_ids = _build_vehicle_hierarchy_from_fitments(
                fitments=product_to_sync.fitments,
                destination=destination,
                api_client=api_client
            )
            for model_category_id in fitment_model_category_ids:
                if model_category_id not in category_ids:
                    category_ids.append(model_category_id)
        
        # Always add "Shop All" category
        shop_all_category_id = _get_shop_all_category_id(destination)
        if shop_all_category_id and shop_all_category_id not in category_ids:
            category_ids.append(shop_all_category_id)

        try:
            # Include custom_fields in the main update payload (for create/update)
            product_api_data = _transform_bigcommerce_part_to_api_format(
                product_to_sync, 
                include_images=False,
                include_custom_fields=True,
                category_ids=category_ids if category_ids else None
            )
        except Exception as e:
            logger.error('{} Error transforming product data for update (sku={}). Error: {}.'.format(
                _LOG_PREFIX, product_to_sync.sku, str(e)
            ))
            return False

        # try:
        product_response = api_client.update_product(
            product_id=product_id,
            product_data=product_api_data
        )
        external_id = str(product_response.get('id', bigcommerce_part.external_id))
        # except bigcommerce_exceptions.BigCommerceAPIException as e:
        #     logger.error('{} Error updating product on BigCommerce API (sku={}). Error: {}.'.format(
        #         _LOG_PREFIX, product_to_sync.sku, str(e)
        #     ))
        #     return False

        if product_to_sync.images:
            try:
                new_image_urls = set()
                for img in product_to_sync.images:
                    image_url = img.get('image_url', '').strip()
                    if image_url:
                        new_image_urls.add(image_url)

                existing_image_urls = set()
                if company_destination_part and company_destination_part.destination_data:
                    destination_data = company_destination_part.destination_data
                    existing_images = destination_data.get('images', [])
                    for existing_img in existing_images:
                        if isinstance(existing_img, dict):
                            image_url = existing_img.get('image_url', '').strip()
                            if image_url:
                                existing_image_urls.add(image_url)

                images_to_delete = existing_image_urls - new_image_urls
                images_to_create = new_image_urls - existing_image_urls

                if images_to_delete:
                    existing_images_api = api_client.get_product_images(product_id)
                    existing_image_map = {}
                    for existing_image in existing_images_api:
                        image_id = existing_image.get('id')
                        if not image_id:
                            continue
                        image_url = (
                            existing_image.get('url_standard') or
                            existing_image.get('url_thumbnail') or
                            existing_image.get('url') or
                            ''
                        ).strip()
                        if image_url:
                            existing_image_map[image_url] = image_id

                    for image_url in images_to_delete:
                        image_id = existing_image_map.get(image_url)
                        if not image_id:
                            continue
                        try:
                            api_client.delete_product_image(product_id, image_id)
                            logger.debug('{} Deleted image (sku={}, image_id={}, image_url={}).'.format(
                                _LOG_PREFIX, product_to_sync.sku, image_id, image_url
                            ))
                        except bigcommerce_exceptions.BigCommerceAPIException as e:
                            logger.warning('{} Error deleting existing image (sku={}, image_id={}). Error: {}.'.format(
                                _LOG_PREFIX, product_to_sync.sku, image_id, str(e)
                            ))

                for img in product_to_sync.images:
                    image_url = img.get('image_url', '').strip()
                    if not image_url or image_url not in images_to_create:
                        continue
                    try:
                        api_client.create_product_image(
                            product_id=product_id,
                            image_data={
                                'image_url': image_url,
                                'is_thumbnail': img.get('is_thumbnail', False),
                            }
                        )
                        logger.debug('{} Created image (sku={}, image_url={}).'.format(
                            _LOG_PREFIX, product_to_sync.sku, image_url
                        ))
                    except bigcommerce_exceptions.BigCommerceAPIException as e:
                        logger.warning('{} Error creating image (sku={}, image_url={}). Error: {}.'.format(
                            _LOG_PREFIX, product_to_sync.sku, image_url, str(e)
                        ))

                if images_to_delete or images_to_create:
                    try:
                        product_response = api_client.get_product(product_id)
                    except bigcommerce_exceptions.BigCommerceAPIException as e:
                        logger.warning('{} Error fetching updated product after image changes (sku={}). Error: {}.'.format(
                            _LOG_PREFIX, product_to_sync.sku, str(e)
                        ))
            except Exception as e:
                logger.warning('{} Error managing images for product (sku={}). Error: {}.'.format(
                    _LOG_PREFIX, product_to_sync.sku, str(e)
                ))

        # Restore original custom_fields
        product_to_sync.custom_fields = original_custom_fields

        # Handle custom fields deletion separately (only for fields that exist in old but not in new)
        try:
            # Delete removed fields (exist only in old)
            for field_name, old_field in old_fields_map.items():
                if field_name not in new_fields_map:
                    field_id = old_field.get('id')
                    if field_id:
                        try:
                            api_client.delete_product_custom_field(product_id, field_id)
                            logger.debug('{} Deleted custom field (sku={}, field_id={}, name={}).'.format(
                                _LOG_PREFIX, product_to_sync.sku, field_id, field_name
                            ))
                        except bigcommerce_exceptions.BigCommerceAPIException as e:
                            logger.warning('{} Error deleting custom field (sku={}, field_id={}, name={}). Error: {}.'.format(
                                _LOG_PREFIX, product_to_sync.sku, field_id, field_name, str(e)
                            ))
        except Exception as e:
            logger.warning('{} Error deleting custom fields for product (sku={}). Error: {}.'.format(
                _LOG_PREFIX, product_to_sync.sku, str(e)
            ))

        company_destination_part = _upsert_company_destination_part(
            product_to_sync=product_to_sync,
            company_destination_part=company_destination_part,
            destination=destination,
            brand=brand,
            external_id=external_id,
            bigcommerce_response=product_response
        )

        bigcommerce_part.external_id = external_id
        bigcommerce_part.raw_data = product_response
        bigcommerce_part.save()

        _mark_history_as_synced(company_destination_part, execution_run)

        logger.info('{} Successfully updated product on BigCommerce (sku={}, external_id={}).'.format(
            _LOG_PREFIX, product_to_sync.sku, external_id
        ))
        return True

    except bigcommerce_exceptions.BigCommerceAPIException as e:
        logger.error('{} Error updating product on BigCommerce (sku={}). Error: {}.'.format(
            _LOG_PREFIX, product_to_sync.sku, str(e)
        ))
        return False
    except Exception as e:
        logger.exception('{} Error updating product on BigCommerce (sku={}). Error: {}.'.format(
            _LOG_PREFIX, product_to_sync.sku, str(e)
        ))
        return False


def _create_product_on_bigcommerce(
    product_to_sync: src_messages.BigCommercePart,
    company_destination_part: typing.Optional[src_models.CompanyDestinationParts],
    destination: src_models.CompanyDestinations,
    brand: src_models.Brands,
    api_client: bigcommerce_client.BigCommerceApiClient,
    execution_run: src_models.CompanyDestinationExecutionRun
) -> bool:
    try:
        logger.info('{} Creating product on BigCommerce (sku={}).'.format(
            _LOG_PREFIX, product_to_sync.sku
        ))

        # Get or create categories
        category_ids = []
        if product_to_sync.category:
            category_id = _get_or_create_bigcommerce_category(
                category_name=product_to_sync.category,
                parent_id=0,
                destination=destination,
                api_client=api_client,
                tree_id=1
            )
            if category_id:
                category_ids.append(category_id)
                
                # If subcategory exists, create it as child of category
                if product_to_sync.subcategory:
                    subcategory_id = _get_or_create_bigcommerce_category(
                        category_name=product_to_sync.subcategory,
                        parent_id=category_id,
                        destination=destination,
                        api_client=api_client,
                        tree_id=1
                    )
                    if subcategory_id:
                        category_ids.append(subcategory_id)
        
        # Build vehicle hierarchy from fitments and add Model category IDs
        if product_to_sync.fitments:
            fitment_model_category_ids = _build_vehicle_hierarchy_from_fitments(
                fitments=product_to_sync.fitments,
                destination=destination,
                api_client=api_client
            )
            for model_category_id in fitment_model_category_ids:
                if model_category_id not in category_ids:
                    category_ids.append(model_category_id)
        
        # Always add "Shop All" category
        shop_all_category_id = _get_shop_all_category_id(destination)
        if shop_all_category_id and shop_all_category_id not in category_ids:
            category_ids.append(shop_all_category_id)

        try:
            product_api_data = _transform_bigcommerce_part_to_api_format(
                product_to_sync,
                category_ids=category_ids if category_ids else None
            )
        except Exception as e:
            logger.error('{} Error transforming product data for create (sku={}). Error: {}.'.format(
                _LOG_PREFIX, product_to_sync.sku, str(e)
            ))
            return False

        # try:
        product_response = api_client.create_product(product_data=product_api_data)
        external_id = str(product_response.get('id', ''))
        # except bigcommerce_exceptions.BigCommerceAPIException as e:
        #     logger.error('{} Error creating product on BigCommerce API (sku={}). Error: {}.'.format(
        #         _LOG_PREFIX, product_to_sync.sku, str(e)
        #     ))
        #     return False

        if not external_id:
            logger.error('{} No product ID returned from BigCommerce API (sku={}).'.format(
                _LOG_PREFIX, product_to_sync.sku
            ))
            return False

        company_destination_part = _upsert_company_destination_part(
            product_to_sync=product_to_sync,
            company_destination_part=company_destination_part,
            destination=destination,
            brand=brand,
            external_id=external_id,
            bigcommerce_response=product_response
        )

        src_models.BigCommerceParts.objects.create(
            external_id=external_id,
            sku=product_to_sync.sku,
            raw_data=product_response,
            company_destination=destination,
        )

        _mark_history_as_synced(company_destination_part, execution_run)

        logger.info('{} Successfully created product on BigCommerce (sku={}, external_id={}).'.format(
            _LOG_PREFIX, product_to_sync.sku, external_id
        ))
        return True

    except bigcommerce_exceptions.BigCommerceAPIException as e:
        logger.error('{} Error creating product on BigCommerce (sku={}). Error: {}.'.format(
            _LOG_PREFIX, product_to_sync.sku, str(e)
        ))
        return False
    except Exception as e:
        logger.exception('{} Error creating product on BigCommerce (sku={}). Error: {}.'.format(
            _LOG_PREFIX, product_to_sync.sku, str(e)
        ))
        return False


def _upsert_company_destination_part(
    product_to_sync: src_messages.BigCommercePart,
    company_destination_part: typing.Optional[src_models.CompanyDestinationParts],
    destination: src_models.CompanyDestinations,
    brand: src_models.Brands,
    external_id: str,
    bigcommerce_response: typing.Dict
) -> src_models.CompanyDestinationParts:
    destination_data = _convert_bigcommerce_response_to_part_format(bigcommerce_response, destination=destination)
    source_data = _get_source_data_for_product(product_to_sync, brand)
    
    # Add fitments from source_data to destination_data for comparison purposes
    # (fitments are not stored in BigCommerce, but we need them in destination_data to compare changes)
    if 'fitments' in source_data:
        destination_data['fitments'] = source_data['fitments']

    if company_destination_part:
        company_destination_part.destination_data = destination_data
        company_destination_part.destination_external_id = external_id
        company_destination_part.source_data = source_data
        company_destination_part.save()
    else:
        company_destination_part = src_models.CompanyDestinationParts.objects.create(
            company_destination=destination,
            part_unique_key=product_to_sync.sku,
            source_data=source_data,
            source_external_id=product_to_sync.sku,
            destination_data=destination_data,
            destination_external_id=external_id,
            brand=brand,
        )

    return company_destination_part


def _mark_history_as_synced(
    company_destination_part: src_models.CompanyDestinationParts,
    execution_run: src_models.CompanyDestinationExecutionRun
) -> None:
    src_models.CompanyDestinationPartsHistory.objects.filter(
        destination_part=company_destination_part,
        synced=False
    ).update(synced=True, execution_run=execution_run)


def _transform_bigcommerce_part_to_api_format(
    part: src_messages.BigCommercePart,
    include_images: bool = True,
    include_custom_fields: bool = True,
    category_ids: typing.Optional[typing.List[int]] = None
) -> typing.Dict:
    price_value = part.default_price
    if isinstance(price_value, (dict, list)):
        price = 0.0
    else:
        try:
            price = float(price_value) if price_value is not None else 0.0
        except (ValueError, TypeError):
            price = 0.0

    weight_value = part.weight
    if isinstance(weight_value, (dict, list)):
        weight = 0.0
    else:
        try:
            weight = float(weight_value) if weight_value is not None else 0.0
        except (ValueError, TypeError):
            weight = 0.0

    cost_value = part.cost
    if isinstance(cost_value, (dict, list)):
        cost = 0.0
    else:
        try:
            cost = float(cost_value) if cost_value is not None else 0.0
        except (ValueError, TypeError):
            cost = 0.0

    msrp_value = part.msrp
    if isinstance(msrp_value, (dict, list)):
        msrp = 0.0
    else:
        try:
            msrp = float(msrp_value) if msrp_value is not None else 0.0
        except (ValueError, TypeError):
            msrp = 0.0

    # Extract width, height, depth
    width_value = part.width
    width = None
    if width_value is not None:
        try:
            width = float(width_value)
        except (ValueError, TypeError):
            width = None
    
    height_value = part.height
    height = None
    if height_value is not None:
        try:
            height = float(height_value)
        except (ValueError, TypeError):
            height = None
    
    depth_value = part.depth
    depth = None
    if depth_value is not None:
        try:
            depth = float(depth_value)
        except (ValueError, TypeError):
            depth = None

    # Calculate availability description based on inventory
    inventory_quantity = int(part.inventory) if part.inventory else 0
    availability_text = _get_availability_text(inventory_quantity)

    product_data = {
        'name': part.product_title,
        'type': 'physical',
        'sku': part.sku,
        'description': part.description,
        'weight': weight,
        'price': price,
        'brand_id': int(part.brand_id),
        'inventory_level': inventory_quantity,
        'inventory_tracking': 'product',
        'is_visible': bool(part.active),
        'cost_price': cost,
        'retail_price': msrp,
        'availability_description': availability_text,
    }
    
    # Only include width, height, depth if they have values
    if width is not None:
        product_data['width'] = width
    if height is not None:
        product_data['height'] = height
    if depth is not None:
        product_data['depth'] = depth

    if part.mpn:
        product_data['mpn'] = part.mpn

    if include_images and part.images:
        product_data['images'] = [
            {
                'image_url': img.get('image_url', ''),
                'is_thumbnail': img.get('is_thumbnail', False),
            }
            for img in part.images
            if img.get('image_url')
        ]

    # Include custom_fields if requested (skip if we're clearing them via DELETE calls)
    if include_custom_fields:
        product_data['custom_fields'] = part.custom_fields if part.custom_fields is not None else []
    
    # Add categories if provided
    if category_ids:
        product_data['categories'] = category_ids

    return product_data


def _get_source_data_for_product(product: src_messages.BigCommercePart, brand: src_models.Brands) -> typing.Dict:
    product_dict = dataclasses.asdict(product)
    return {
        **product_dict,
        'brand_id': brand.id,
        'brand_name': brand.name,
    }


def select_products_for_syncing_into_bigcommerce(
        products_candidates_for_sync: list[src_messages.BigCommercePart],
        execution_run: src_models.CompanyDestinationExecutionRun
) -> list[src_messages.BigCommercePart]:
    products_for_syncing = []
    candidates_skus = [
        product.sku for product in products_candidates_for_sync
    ]

    company_destination_parts = src_models.CompanyDestinationParts.objects.filter(
        part_unique_key__in=candidates_skus
    )
    
    candidates_to_sync_immediately = set(candidates_skus) - set(list(company_destination_parts.values_list('part_unique_key', flat=True)))
    for product in products_candidates_for_sync:
        if product.sku in candidates_to_sync_immediately:
            products_for_syncing.append(product)

    product_candidates_dict = {product.sku: product for product in products_candidates_for_sync}

    for company_destination_part in company_destination_parts:
        product_candidate = product_candidates_dict.get(company_destination_part.part_unique_key)
        if not product_candidate:
            continue

        if _company_destination_part_changed(
            company_destination_part=company_destination_part,
            product_candidate=product_candidate,
            execution_run=execution_run
        ):
            products_for_syncing.append(product_candidate)

    return products_for_syncing


def _company_destination_part_changed(
    company_destination_part: src_models.CompanyDestinationParts,
    product_candidate: src_messages.BigCommercePart,
    execution_run: src_models.CompanyDestinationExecutionRun
) -> bool:
    destination_data = company_destination_part.destination_data
    if not destination_data:
        return True

    candidate_dict = _bigcommerce_part_to_dict(product_candidate)
    changes = _compare_bigcommerce_parts(destination_data, candidate_dict)

    if not changes:
        return False

    src_models.CompanyDestinationPartsHistory.objects.create(
        destination_part=company_destination_part,
        execution_run=execution_run,
        data=candidate_dict,
        changes=changes,
        synced=False,
    )

    return True


def _bigcommerce_part_to_dict(part: src_messages.BigCommercePart) -> typing.Dict:
    """
    Convert BigCommercePart to dictionary, including derived fields like availability_description.
    """
    part_dict = dataclasses.asdict(part)
    # Calculate and add availability_description based on inventory
    inventory_quantity = int(part.inventory) if part.inventory else 0
    part_dict['availability_description'] = _get_availability_text(inventory_quantity)
    return part_dict


def _convert_bigcommerce_response_to_part_format(
    bigcommerce_response: typing.Dict,
    destination: typing.Optional[src_models.CompanyDestinations] = None
) -> typing.Dict:
    images = []
    if 'images' in bigcommerce_response:
        images_data = bigcommerce_response['images']
        if isinstance(images_data, dict) and 'data' in images_data:
            images_data = images_data['data']
        if isinstance(images_data, list):
            for img in images_data:
                if isinstance(img, dict):
                    image_url = img.get('url_standard') or img.get('url_thumbnail') or img.get('url') or ''
                    if image_url:
                        images.append({
                            'image_url': image_url,
                            'is_thumbnail': img.get('is_thumbnail', False),
                        })

    elif 'primary_image' in bigcommerce_response and bigcommerce_response['primary_image']:
        primary_img = bigcommerce_response['primary_image']
        if isinstance(primary_img, dict):
            image_url = primary_img.get('url_standard') or primary_img.get('url_thumbnail') or primary_img.get('url') or ''
            if image_url:
                images.append({
                    'image_url': image_url,
                    'is_thumbnail': True,
                })

    custom_fields = []
    if 'custom_fields' in bigcommerce_response:
        custom_fields_data = bigcommerce_response['custom_fields']
        if isinstance(custom_fields_data, dict) and 'data' in custom_fields_data:
            custom_fields_data = custom_fields_data['data']
        if isinstance(custom_fields_data, list):
            custom_fields = custom_fields_data

    cost = 0.0
    try:
        cost = float(bigcommerce_response.get('cost_price', 0.0))
    except (ValueError, TypeError):
        pass

    msrp = 0.0
    try:
        msrp = float(bigcommerce_response.get('retail_price', 0.0))
    except (ValueError, TypeError):
        pass

    # Extract width, height, depth
    width = None
    try:
        width_val = bigcommerce_response.get('width')
        if width_val is not None:
            width = float(width_val)
    except (ValueError, TypeError):
        pass
    
    height = None
    try:
        height_val = bigcommerce_response.get('height')
        if height_val is not None:
            height = float(height_val)
    except (ValueError, TypeError):
        pass
    
    depth = None
    try:
        depth_val = bigcommerce_response.get('depth')
        if depth_val is not None:
            depth = float(depth_val)
    except (ValueError, TypeError):
        pass

    inventory_quantity = int(bigcommerce_response.get('inventory_level', 0))
    availability_text = _get_availability_text(inventory_quantity)

    # Extract categories from response
    category = None
    subcategory = None
    category_ids = bigcommerce_response.get('categories', [])
    if category_ids and destination:
        # Look up category names from database
        # Filter by destination to ensure we get the right categories
        categories_query = src_models.BigCommerceCategories.objects.filter(
            external_id__in=category_ids,
            company_destination=destination
        )
        categories_list = list(categories_query.order_by('parent_id'))
        
        # Category is the one with parent_id=0, subcategory is the one with parent_id=category_id
        parent_category = None
        for cat in categories_list:
            if cat.parent_id == 0:
                if cat.name == 'Shop All':
                    continue

                category = cat.name
                parent_category = cat
            elif parent_category and cat.parent_id == parent_category.external_id:
                # This is a child of the parent category
                subcategory = cat.name

    return {
        'brand_id': int(bigcommerce_response.get('brand_id', 0)),
        'product_title': bigcommerce_response.get('name', ''),
        'sku': bigcommerce_response.get('sku', ''),
        'mpn': bigcommerce_response.get('mpn', ''),
        'default_price': float(bigcommerce_response.get('price', 0.0)),
        'cost': cost,
        'msrp': msrp,
        'weight': float(bigcommerce_response.get('weight', 0.0)),
        'width': width,
        'height': height,
        'depth': depth,
        'description': bigcommerce_response.get('description', ''),
        'images': images,
        'inventory': inventory_quantity,
        'custom_fields': custom_fields,
        'active': bool(bigcommerce_response.get('is_visible', False)),
        'availability_description': availability_text,
        'category': category,
        'subcategory': subcategory,
    }


def _compare_bigcommerce_parts(
    old_data: typing.Dict,
    new_data: typing.Dict
) -> typing.Dict:
    changes = {}

    old_brand_id = old_data.get('brand_id')
    new_brand_id = new_data.get('brand_id')
    if _values_different(old_brand_id, new_brand_id):
        changes['brand_id'] = {'old': old_brand_id, 'new': new_brand_id}

    old_product_title = old_data.get('product_title')
    new_product_title = new_data.get('product_title')
    if _values_different(old_product_title, new_product_title):
        changes['product_title'] = {'old': old_product_title, 'new': new_product_title}

    old_sku = old_data.get('sku')
    new_sku = new_data.get('sku')
    if _values_different(old_sku, new_sku):
        changes['sku'] = {'old': old_sku, 'new': new_sku}

    old_mpn = old_data.get('mpn')
    new_mpn = new_data.get('mpn')
    if _values_different(old_mpn, new_mpn):
        changes['mpn'] = {'old': old_mpn, 'new': new_mpn}

    old_default_price = old_data.get('default_price')
    new_default_price = new_data.get('default_price')
    if _values_different(old_default_price, new_default_price):
        changes['default_price'] = {'old': old_default_price, 'new': new_default_price}

    old_cost = old_data.get('cost')
    new_cost = new_data.get('cost')
    if _values_different(old_cost, new_cost):
        changes['cost'] = {'old': old_cost, 'new': new_cost}

    old_msrp = old_data.get('msrp')
    new_msrp = new_data.get('msrp')
    if _values_different(old_msrp, new_msrp):
        changes['msrp'] = {'old': old_msrp, 'new': new_msrp}

    old_weight = old_data.get('weight')
    new_weight = new_data.get('weight')
    if _values_different(old_weight, new_weight):
        changes['weight'] = {'old': old_weight, 'new': new_weight}

    old_width = old_data.get('width')
    new_width = new_data.get('width')
    # Treat None and 0.0 as the same for dimensions
    if _dimension_values_different(old_width, new_width):
        changes['width'] = {'old': old_width, 'new': new_width}

    old_height = old_data.get('height')
    new_height = new_data.get('height')
    # Treat None and 0.0 as the same for dimensions
    if _dimension_values_different(old_height, new_height):
        changes['height'] = {'old': old_height, 'new': new_height}

    old_depth = old_data.get('depth')
    new_depth = new_data.get('depth')
    # Treat None and 0.0 as the same for dimensions
    if _dimension_values_different(old_depth, new_depth):
        changes['depth'] = {'old': old_depth, 'new': new_depth}

    old_description = old_data.get('description')
    new_description = new_data.get('description')
    if _values_different(old_description, new_description):
        changes['description'] = {'old': old_description, 'new': new_description}

    # old_images = old_data.get('images')
    # new_images = new_data.get('images')
    # if _images_different(old_images, new_images):
    #     changes['images'] = {'old': old_images, 'new': new_images}

    old_inventory = old_data.get('inventory')
    new_inventory = new_data.get('inventory')
    if _values_different(old_inventory, new_inventory):
        changes['inventory'] = {'old': old_inventory, 'new': new_inventory}
    
    # Availability description is derived from inventory, so compare it too
    old_availability = old_data.get('availability_description')
    new_availability = new_data.get('availability_description')
    if _values_different(old_availability, new_availability):
        changes['availability_description'] = {'old': old_availability, 'new': new_availability}

    old_custom_fields = old_data.get('custom_fields')
    new_custom_fields = new_data.get('custom_fields')
    if _values_different(old_custom_fields, new_custom_fields):
        changes['custom_fields'] = {'old': old_custom_fields, 'new': new_custom_fields}

    old_active = old_data.get('active')
    new_active = new_data.get('active')
    if _values_different(old_active, new_active):
        changes['active'] = {'old': old_active, 'new': new_active}

    old_category = old_data.get('category')
    new_category = new_data.get('category')
    if _values_different(old_category, new_category):
        changes['category'] = {'old': old_category, 'new': new_category}

    old_subcategory = old_data.get('subcategory')
    new_subcategory = new_data.get('subcategory')
    if _values_different(old_subcategory, new_subcategory):
        changes['subcategory'] = {'old': old_subcategory, 'new': new_subcategory}

    old_fitments = old_data.get('fitments')
    new_fitments = new_data.get('fitments')
    if _values_different(old_fitments, new_fitments):
        changes['fitments'] = {'old': old_fitments, 'new': new_fitments}

    return changes


def _images_different(old_images: typing.Any, new_images: typing.Any) -> bool:
    if old_images == new_images:
        return False

    if old_images is None or new_images is None:
        return old_images != new_images

    if not isinstance(old_images, list) or not isinstance(new_images, list):
        return old_images != new_images

    if len(old_images) != len(new_images):
        return True

    if not old_images and not new_images:
        return False

    def normalize_image(img: typing.Dict) -> str:
        return img.get('image_url', '').strip()

    old_normalized = sorted([normalize_image(img) for img in old_images if isinstance(img, dict)])
    new_normalized = sorted([normalize_image(img) for img in new_images if isinstance(img, dict)])

    return old_normalized != new_normalized


def _values_different(old_value: typing.Any, new_value: typing.Any) -> bool:
    if old_value == new_value:
        return False

    if old_value is None or new_value is None:
        return old_value != new_value

    if isinstance(old_value, list) and isinstance(new_value, list):
        if len(old_value) != len(new_value):
            return True
        if not old_value and not new_value:
            return False
        if old_value and new_value and isinstance(old_value[0], dict) and isinstance(new_value[0], dict):
            # Check if this is fitments data (has year, make, model keys)
            first_old = old_value[0]
            first_new = new_value[0]
            if 'year' in first_old and 'make' in first_old and 'model' in first_old:
                # Sort fitments by year, make, model
                old_sorted = sorted(old_value, key=lambda x: (str(x.get('year', '')), str(x.get('make', '')), str(x.get('model', ''))))
                new_sorted = sorted(new_value, key=lambda x: (str(x.get('year', '')), str(x.get('make', '')), str(x.get('model', ''))))
                return old_sorted != new_sorted
            elif 'image_url' in first_old:
                # Sort images by image_url and is_thumbnail
                old_sorted = sorted(old_value, key=lambda x: (x.get('image_url', ''), x.get('is_thumbnail', False)))
                new_sorted = sorted(new_value, key=lambda x: (x.get('image_url', ''), x.get('is_thumbnail', False)))
                return old_sorted != new_sorted
            else:
                # Generic dict comparison - sort by all keys
                old_sorted = sorted(old_value, key=lambda x: tuple(sorted(x.items())))
                new_sorted = sorted(new_value, key=lambda x: tuple(sorted(x.items())))
                return old_sorted != new_sorted
        return old_value != new_value

    if isinstance(old_value, float) and isinstance(new_value, float):
        return abs(old_value - new_value) > 0.01

    if isinstance(old_value, int) and isinstance(new_value, float):
        return abs(float(old_value) - new_value) > 0.01

    if isinstance(old_value, float) and isinstance(new_value, int):
        return abs(old_value - float(new_value)) > 0.01

    return old_value != new_value


def _dimension_values_different(old_value: typing.Any, new_value: typing.Any) -> bool:
    """
    Compare dimension values (width, height, depth).
    Treats None and 0.0 as the same since 0.0 effectively means no dimension.
    """
    # If both are None, they're the same
    if old_value is None and new_value is None:
        return False
    
    # If one is None and the other is 0.0 (or vice versa), they're the same
    if old_value is None:
        try:
            return float(new_value) != 0.0
        except (ValueError, TypeError):
            return True
    
    if new_value is None:
        try:
            return float(old_value) != 0.0
        except (ValueError, TypeError):
            return True
    
    # Both are not None, use standard comparison
    return _values_different(old_value, new_value)