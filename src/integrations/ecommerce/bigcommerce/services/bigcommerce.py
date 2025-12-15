import logging
import typing
import pgbulk

from src import enums as src_enums
from src import models as src_models
from src.integrations.ecommerce.bigcommerce.gateways import client as bigcommerce_client
from src.integrations.ecommerce.bigcommerce.gateways import exceptions as bigcommerce_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = '[BIGCOMMERCE-SERVICES]'


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
    '''
        1. Get all products for that brand and company
        2. Check if product needs to be synced 
        3. Sync product  
    '''
