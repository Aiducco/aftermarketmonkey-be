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


        bigcommerce_parts.append(
            src_messages.BigCommercePart(
                brand_id=int(bigcommerce_brand.external_id),
                product_title='{}{}'.format(turn_14_item.part_description, turn_14_item.part_number),
                sku=turn_14_item.part_number,
                mpn=turn_14_item.mfr_part_number,
                default_price= _get_turn_14_price(turn_14_pricing),
                weight=_get_turn_14_weight(turn_14_item=turn_14_item),
                description=_get_turn_14_description(turn_14_data=turn_14_data),
                images=_get_turn_14_images(turn_14_item=turn_14_item, turn_14_data=turn_14_data),
                inventory=_get_turn_14_inventory(turn_14_inventory=turn_14_inventory),
                custom_fields=[],
                active=turn_14_item.active,
            )
        )

    return bigcommerce_parts


def _get_turn_14_price(turn_14_pricing: src_models.Turn14BrandPricing) -> float:
    price = 0.0
    if not turn_14_pricing.pricelists:
        return price

    for pricelist_item in turn_14_pricing.pricelists:
        if isinstance(pricelist_item, dict) and pricelist_item.get('name') == 'MAP':
            price_value = pricelist_item.get('price')
            if price_value is not None:
                try:
                    price = float(price_value)
                    break
                except (ValueError, TypeError):
                    continue

    return price

def _get_turn_14_weight(turn_14_item: src_models.Turn14Items) -> float:
    weight = 0.0

    if not turn_14_item.dimensions:
        return weight

    weight_in_lbs = turn_14_item.dimensions[0].get('weight', 0)

    return weight_in_lbs * 16

def _get_turn_14_description(turn_14_data: src_models.Turn14BrandData) -> str:
    description = ''
    for turn_14_desc in turn_14_data.descriptions:
        if turn_14_desc.get('type') == 'Market Description':
            description += turn_14_desc.get('description')

        if turn_14_desc.get('type') == 'Product Description - Extended':
            description += turn_14_desc.get('description')

    return description

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

        try:
            product_api_data = _transform_bigcommerce_part_to_api_format(product_to_sync)
        except Exception as e:
            logger.error('{} Error transforming product data for update (sku={}). Error: {}.'.format(
                _LOG_PREFIX, product_to_sync.sku, str(e)
            ))
            return False

        try:
            product_response = api_client.update_product(
                product_id=int(bigcommerce_part.external_id),
                product_data=product_api_data
            )
            external_id = str(product_response.get('id', bigcommerce_part.external_id))
        except bigcommerce_exceptions.BigCommerceAPIException as e:
            logger.error('{} Error updating product on BigCommerce API (sku={}). Error: {}.'.format(
                _LOG_PREFIX, product_to_sync.sku, str(e)
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


def _transform_bigcommerce_part_to_api_format(part: src_messages.BigCommercePart) -> typing.Dict:
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

    product_data = {
        'name': part.product_title,
        'type': 'physical',
        'sku': part.sku,
        'description': part.description,
        'weight': weight,
        'price': price,
        'brand_id': int(part.brand_id),
        'inventory_level': int(part.inventory) if part.inventory else 0,
        'inventory_tracking': 'product',
        'is_visible': bool(part.active),
    }

    if part.mpn:
        product_data['mpn'] = part.mpn

    if part.images:
        product_data['images'] = [
            {
                'image_url': img.get('image_url', ''),
                'is_thumbnail': img.get('is_thumbnail', False),
            }
            for img in part.images
            if img.get('image_url')
        ]

    if part.custom_fields:
        product_data['custom_fields'] = part.custom_fields

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
    return dataclasses.asdict(part)


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

    return {
        'brand_id': int(bigcommerce_response.get('brand_id', 0)),
        'product_title': bigcommerce_response.get('name', ''),
        'sku': bigcommerce_response.get('sku', ''),
        'mpn': bigcommerce_response.get('mpn', ''),
        'default_price': float(bigcommerce_response.get('price', 0.0)),
        'weight': float(bigcommerce_response.get('weight', 0.0)),
        'description': bigcommerce_response.get('description', ''),
        'images': images,
        'inventory': int(bigcommerce_response.get('inventory_level', 0)),
        'custom_fields': custom_fields,
        'active': bool(bigcommerce_response.get('is_visible', False)),
    }


def _compare_bigcommerce_parts(
    old_data: typing.Dict,
    new_data: typing.Dict
) -> typing.Dict:
    changes = {}

    fields_to_compare = [
        'brand_id', 'product_title', 'sku', 'mpn', 'default_price',
        'weight', 'description', 'images', 'inventory', 'custom_fields', 'active'
    ]

    for field in fields_to_compare:
        old_value = old_data.get(field)
        new_value = new_data.get(field)

        if _values_different(old_value, new_value):
            changes[field] = {
                'old': old_value,
                'new': new_value,
            }

    return changes


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