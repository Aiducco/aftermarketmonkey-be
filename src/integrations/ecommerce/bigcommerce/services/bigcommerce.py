import dataclasses
import logging
import typing
import pgbulk
from django.db.models.functions import TruncWeek
from django.utils import timezone

from src import enums as src_enums
from src import messages as src_messages
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

        for product_to_sync, bigcommerce_part, company_destination_part in products_to_update:
            success = _update_product_on_bigcommerce(
                product_to_sync=product_to_sync,
                bigcommerce_part=bigcommerce_part,
                company_destination_part=company_destination_part,
                destination=destination,
                brand=brand,
                api_client=api_client,
                execution_run=execution_run
            )
            execution_run.products_processed += 1
            if success:
                execution_run.products_updated += 1
            else:
                execution_run.products_failed += 1

        for product_to_sync, company_destination_part in products_to_create:
            success = _create_product_on_bigcommerce(
                product_to_sync=product_to_sync,
                company_destination_part=company_destination_part,
                destination=destination,
                brand=brand,
                api_client=api_client,
                execution_run=execution_run
            )
            execution_run.products_processed += 1
            if success:
                execution_run.products_created += 1
            else:
                execution_run.products_failed += 1

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
    '''
        TODO:   BUILD CONFIGURATION BASED ON PREFERENCES

    '''
    brand_providers = src_models.BrandProviders.objects.filter(
        brand=brand
    )
    if not brand_providers:
        logger.error('{} No brand providers found for brand {}.'.format(
            _LOG_PREFIX, brand.name
        ))
        raise Exception('{} No brand providers found for brand {}.'.format(_LOG_PREFIX, brand.name))

    brand_parts = {}
    for brand_provider in brand_providers:
        if brand_provider.provider.kind_name == src_enums.BrandProviderKind.TURN_14.name:
            try:
                parts = prepare_turn_14_products_for_bigcommerce(brand=brand)
            except Exception as e:
                logger.exception('{} Error while preparing turn14 products for brand {}.'.format(_LOG_PREFIX, brand))
                continue

            brand_parts[src_enums.BrandProviderKind.TURN_14] = parts


    return brand_parts[src_enums.BrandProviderKind.TURN_14]


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
        
        bigcommerce_parts.append(
            src_messages.BigCommercePart(
                brand_id=int(bigcommerce_brand.external_id),
                product_title='{} - {}'.format(turn_14_item.part_description, turn_14_item.part_number),
                sku=turn_14_item.part_number,
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
            )
        )

    return bigcommerce_parts


def _get_turn_14_prices(turn_14_pricing: src_models.Turn14BrandPricing) -> typing.Tuple[float, float]:
    default_price = 0.0
    msrp = 0.0
    
    if not turn_14_pricing.pricelists:
        return default_price, msrp

    map_price = None
    retail_price = None
    msrp_price = None
    
    for pricelist_item in turn_14_pricing.pricelists:
        if not isinstance(pricelist_item, dict):
            continue
            
        pricelist_name = pricelist_item.get('name')
        price_value = pricelist_item.get('price')
        
        if price_value is None:
            continue
            
        try:
            price_float = float(price_value)
            
            if pricelist_name == 'MAP':
                map_price = price_float
            elif pricelist_name == 'Retail':
                retail_price = price_float
            elif pricelist_name == 'MSRP':
                msrp_price = price_float
        except (ValueError, TypeError):
            continue

    # For default_price: use MAP if available, otherwise Retail, otherwise MSRP
    default_price = map_price if map_price is not None else (retail_price if retail_price is not None else (msrp_price if msrp_price is not None else 0.0))
    
    # For retail_price (MSRP): use Retail if available, otherwise MSRP
    msrp = retail_price if retail_price is not None else (msrp_price if msrp_price is not None else 0.0)
    
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

def _get_turn_14_description(turn_14_data: src_models.Turn14BrandData) -> str:
    """
    Format descriptions as HTML with Overview section and Features and Benefits list.
    """
    if not turn_14_data.descriptions or not isinstance(turn_14_data.descriptions, list):
        return ''
    
    overview_parts = []
    features_and_benefits = []
    
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

    count = 0
    is_thumbnail = False
    for file in turn_14_data.files:
        if file.get('type') == 'Image':
            if not file.get('links'):
                continue

            if count == 0:
                is_thumbnail = True

            count += 1
            images.append(
                {
                    'is_thumbnail': is_thumbnail,
                    'image_url': file.get('links', [])[0].get('url', ''),
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

    for product_to_sync in products_for_sync:
        bigcommerce_part = src_models.BigCommerceParts.objects.filter(
            sku=product_to_sync.sku,
            company_destination=destination
        ).first()

        company_destination_part = src_models.CompanyDestinationParts.objects.filter(
            part_unique_key=product_to_sync.sku,
            company_destination=destination,
            brand=brand
        ).first()

        if bigcommerce_part:
            products_to_update.append((product_to_sync, bigcommerce_part, company_destination_part))
        else:
            products_to_create.append((product_to_sync, company_destination_part))

    return products_to_update, products_to_create


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

        try:
            # Include custom_fields in the main update payload (for create/update)
            product_api_data = _transform_bigcommerce_part_to_api_format(
                product_to_sync, 
                include_images=False,
                include_custom_fields=True
            )
        except Exception as e:
            logger.error('{} Error transforming product data for update (sku={}). Error: {}.'.format(
                _LOG_PREFIX, product_to_sync.sku, str(e)
            ))
            return False

        try:
            product_response = api_client.update_product(
                product_id=product_id,
                product_data=product_api_data
            )
            external_id = str(product_response.get('id', bigcommerce_part.external_id))
        except bigcommerce_exceptions.BigCommerceAPIException as e:
            logger.error('{} Error updating product on BigCommerce API (sku={}). Error: {}.'.format(
                _LOG_PREFIX, product_to_sync.sku, str(e)
            ))
            return False

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

        try:
            product_api_data = _transform_bigcommerce_part_to_api_format(product_to_sync)
        except Exception as e:
            logger.error('{} Error transforming product data for create (sku={}). Error: {}.'.format(
                _LOG_PREFIX, product_to_sync.sku, str(e)
            ))
            return False

        try:
            product_response = api_client.create_product(product_data=product_api_data)
            external_id = str(product_response.get('id', ''))
        except bigcommerce_exceptions.BigCommerceAPIException as e:
            logger.error('{} Error creating product on BigCommerce API (sku={}). Error: {}.'.format(
                _LOG_PREFIX, product_to_sync.sku, str(e)
            ))
            return False

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
    destination_data = _convert_bigcommerce_response_to_part_format(bigcommerce_response)
    source_data = _get_source_data_for_product(product_to_sync, brand)

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


def _transform_bigcommerce_part_to_api_format(part: src_messages.BigCommercePart, include_images: bool = True, include_custom_fields: bool = True) -> typing.Dict:
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


def _convert_bigcommerce_response_to_part_format(bigcommerce_response: typing.Dict) -> typing.Dict:
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
    if _values_different(old_width, new_width):
        changes['width'] = {'old': old_width, 'new': new_width}

    old_height = old_data.get('height')
    new_height = new_data.get('height')
    if _values_different(old_height, new_height):
        changes['height'] = {'old': old_height, 'new': new_height}

    old_depth = old_data.get('depth')
    new_depth = new_data.get('depth')
    if _values_different(old_depth, new_depth):
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
            old_sorted = sorted(old_value, key=lambda x: (x.get('image_url', ''), x.get('is_thumbnail', False)))
            new_sorted = sorted(new_value, key=lambda x: (x.get('image_url', ''), x.get('is_thumbnail', False)))
            return old_sorted != new_sorted
        return old_value != new_value

    if isinstance(old_value, float) and isinstance(new_value, float):
        return abs(old_value - new_value) > 0.01

    if isinstance(old_value, int) and isinstance(new_value, float):
        return abs(float(old_value) - new_value) > 0.01

    if isinstance(old_value, float) and isinstance(new_value, int):
        return abs(old_value - float(new_value)) > 0.01

    return old_value != new_value