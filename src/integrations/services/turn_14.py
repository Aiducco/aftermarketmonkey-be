import logging
import typing
from datetime import datetime
from decimal import Decimal

import pgbulk

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.turn_14 import client as turn_14_client
from src.integrations.clients.turn_14 import exceptions as turn_14_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = '[TURN-14-SERVICES]'


def fetch_and_save_turn_14_brands() -> None:
    logger.info('{} Started fetching and saving turn 14 brands.'.format(_LOG_PREFIX))
    
    primary_provider = src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.TURN_14.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        primary=True
    ).first()

    if not primary_provider:
        logger.info('{} No turn 14 active provider found.'.format(_LOG_PREFIX))
        return

    credentials = primary_provider.credentials
    logger.debug('{} Initializing Turn 14 API client for company: {}'.format(
        _LOG_PREFIX, primary_provider.company.name
    ))
    
    try:
        api_client = turn_14_client.Turn14ApiClient(credentials=credentials)
    except ValueError as e:
        logger.error('{} Invalid credentials or configuration: {}'.format(_LOG_PREFIX, str(e)))
        raise
    
    logger.info('{} Fetching brands from Turn 14 API.'.format(_LOG_PREFIX))
    try:
        brands_data = api_client.get_brands()
    except turn_14_exceptions.Turn14APIException as e:
        logger.error('{} Turn 14 API error occurred: {}'.format(_LOG_PREFIX, str(e)))
        raise
    
    if not brands_data:
        logger.warning('{} No brands data returned from Turn 14 API.'.format(_LOG_PREFIX))
        return
    
    logger.info('{} Successfully fetched {} brands from Turn 14 API.'.format(
        _LOG_PREFIX, len(brands_data)
    ))
    
    brand_instances = _transform_brands_data(brands_data)
    
    if not brand_instances:
        logger.warning('{} No valid brand instances created after transformation.'.format(_LOG_PREFIX))
        return
    
    logger.info('{} Prepared {} brand instances for upsert.'.format(
        _LOG_PREFIX, len(brand_instances)
    ))
    
    logger.info('{} Starting bulk upsert operation.'.format(_LOG_PREFIX))
    try:
        upserted_brands = pgbulk.upsert(
            src_models.Turn14Brand,
            brand_instances,
            unique_fields=['external_id'],
            update_fields=['name', 'dropship', 'price_groups', 'logo', 'aaia_code'],
            returning=True,
        )
    except Exception as e:
        logger.error('{} Error during bulk upsert operation: {}'.format(_LOG_PREFIX, str(e)))
        raise
    
    logger.info('{} Successfully upserted {} Turn 14 brands.'.format(
        _LOG_PREFIX, len(upserted_brands) if upserted_brands else 0
    ))


def _transform_brands_data(brands_data: typing.List[typing.Dict]) -> typing.List[src_models.Turn14Brand]:
    brand_instances = []
    
    for brand_data in brands_data:
        try:
            external_id = str(brand_data.get('id', ''))
            
            if not external_id:
                logger.warning('{} Skipping brand with missing external_id: {}'.format(
                    _LOG_PREFIX, brand_data
                ))
                continue
            
            attributes = brand_data.get('attributes', {})
            
            name = str(attributes.get('name', '')).strip() or f'Brand {external_id}'
            dropship = bool(attributes.get('dropship', False))
            price_groups = attributes.get('pricegroups') if attributes.get('pricegroups') else None
            logo = attributes.get('logo') if attributes.get('logo') else None
            
            aaia_list = attributes.get('AAIA', [])
            aaia_code = ','.join(aaia_list) if isinstance(aaia_list, list) and aaia_list else ''
            
            brand_instance = src_models.Turn14Brand(
                external_id=external_id,
                name=name,
                dropship=dropship,
                price_groups=price_groups,
                logo=logo,
                aaia_code=aaia_code,
            )
            
            brand_instances.append(brand_instance)
            
        except Exception as e:
            logger.warning('{} Error transforming brand data {}: {}. Skipping.'.format(
                _LOG_PREFIX, brand_data, str(e)
            ))
            continue
    
    return brand_instances


def fetch_and_save_all_turn_14_brand_items() -> None:
    logger.info('{} Fetching all Turn 14 brand items.'.format(_LOG_PREFIX))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value
    ).first()
    if not turn_14_provider:
        logger.info('{} No Turn 14 provider found.'.format(_LOG_PREFIX))
        return

    all_brands = src_models.BrandProviders.objects.filter(
        provider=turn_14_provider,
    )
    if not all_brands.exists():
        logger.info('{} No brands found for Turn 14 provider.'.format(_LOG_PREFIX))
        return

    for brand_provider in all_brands:
        brand = brand_provider.brand
        
        brand_mapping = src_models.BrandTurn14BrandMapping.objects.filter(
            brand=brand
        ).first()
        
        if not brand_mapping:
            logger.warning('{} No Turn14Brand mapping found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, brand.name
            ))
            continue
        
        turn_14_brand = brand_mapping.turn14_brand
        
        company_brand = src_models.CompanyBrands.objects.filter(
            brand=brand
        ).first()
        
        if not company_brand:
            logger.warning('{} No company found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, turn_14_brand.name
            ))
            continue
        
        company = company_brand.company
        
        company_provider = src_models.CompanyProviders.objects.filter(
            company=company,
            provider=turn_14_provider
        ).first()
        
        if not company_provider:
            logger.warning('{} No company provider found for company: {} and brand: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name
            ))
            continue
        
        credentials = company_provider.credentials
        
        try:
            api_client = turn_14_client.Turn14ApiClient(credentials=credentials)
        except ValueError as e:
            logger.error('{} Invalid credentials for company: {} and brand: {}. Error: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name, str(e)
            ))
            continue
        
        brand_id = int(turn_14_brand.external_id)
        page = 1
        
        logger.info('{} Fetching items for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, turn_14_brand.name, brand_id
        ))
        
        while page is not None:
            try:
                items_data, next_page = api_client.get_items_for_brand(brand_id=brand_id, page=page)
            except turn_14_exceptions.Turn14APIException as e:
                logger.error('{} Turn 14 API error for brand: {} (external_id: {}), page: {}. Error: {}. Skipping brand.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page, str(e)
                ))
                break
            
            if not items_data:
                logger.warning('{} No items data returned for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page
                ))
                page = next_page
                continue
            
            logger.info('{} Fetched {} items for brand: {} (external_id: {}), page: {}.'.format(
                _LOG_PREFIX, len(items_data), turn_14_brand.name, brand_id, page
            ))
            
            item_instances = _transform_items_data(items_data, turn_14_brand)
            
            if not item_instances:
                logger.warning('{} No valid item instances created for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page
                ))
                page = next_page
                continue
            
            try:
                upserted_items = pgbulk.upsert(
                    src_models.Turn14Items,
                    item_instances,
                    unique_fields=['external_id'],
                    update_fields=[
                        'brand', 'product_name', 'part_number', 'mfr_part_number', 'part_description',
                        'category', 'subcategory', 'external_brand_id', 'brand_name', 'price_group_id',
                        'price_group', 'active', 'born_on_date', 'regular_stock',
                        'powersports_indicator', 'dropship_controller_id', 'air_freight_prohibited',
                        'not_carb_approved', 'carb_acknowledgement_required', 'ltl_freight_required',
                        'prop_65', 'epa', 'units_per_sku', 'clearance_item', 'thumbnail',
                        'barcode', 'dimensions', 'warehouse_availability'
                    ],
                    returning=True,
                )
                
                logger.info('{} Successfully upserted {} items for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, len(upserted_items) if upserted_items else 0, turn_14_brand.name, brand_id, page
                ))
            except Exception as e:
                logger.error('{} Error during bulk upsert for brand: {} (external_id: {}), page: {}. Error: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page, str(e)
                ))
                page = next_page
                continue
            
            page = next_page
        
        logger.info('{} Completed fetching items for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, turn_14_brand.name, brand_id
        ))


def _transform_items_data(items_data: typing.List[typing.Dict], turn_14_brand: src_models.Turn14Brand) -> typing.List[src_models.Turn14Items]:
    item_instances = []
    
    for item_data in items_data:
        try:
            external_id = str(item_data.get('id', ''))
            
            if not external_id:
                logger.warning('{} Skipping item with missing external_id: {}'.format(
                    _LOG_PREFIX, item_data
                ))
                continue
            
            attributes = item_data.get('attributes', {})
            
            born_on_date = None
            if attributes.get('born_on_date'):
                try:
                    born_on_date = datetime.strptime(attributes.get('born_on_date'), '%Y-%m-%d').date()
                except Exception:
                    pass
            
            item_instance = src_models.Turn14Items(
                external_id=external_id,
                brand=turn_14_brand,
                product_name=attributes.get('product_name'),
                part_number=attributes.get('part_number'),
                mfr_part_number=attributes.get('mfr_part_number'),
                part_description=attributes.get('part_description'),
                category=attributes.get('category'),
                subcategory=attributes.get('subcategory'),
                external_brand_id=attributes.get('brand_id'),
                brand_name=attributes.get('brand'),
                price_group_id=attributes.get('price_group_id'),
                price_group=attributes.get('price_group'),
                active=bool(attributes.get('active', False)),
                born_on_date=born_on_date,
                regular_stock=bool(attributes.get('regular_stock', False)),
                powersports_indicator=bool(attributes.get('powersports_indicator', False)),
                dropship_controller_id=attributes.get('dropship_controller_id'),
                air_freight_prohibited=bool(attributes.get('air_freight_prohibited', False)),
                not_carb_approved=bool(attributes.get('not_carb_approved', False)),
                carb_acknowledgement_required=bool(attributes.get('carb_acknowledgement_required', False)),
                ltl_freight_required=bool(attributes.get('ltl_freight_required', False)),
                prop_65=attributes.get('prop_65'),
                epa=attributes.get('epa'),
                units_per_sku=attributes.get('units_per_sku'),
                clearance_item=bool(attributes.get('clearance_item', False)),
                thumbnail=attributes.get('thumbnail'),
                barcode=attributes.get('barcode'),
                dimensions=attributes.get('dimensions'),
                warehouse_availability=attributes.get('warehouse_availability'),
            )
            
            item_instances.append(item_instance)
            
        except Exception as e:
            logger.warning('{} Error transforming item data {}: {}. Skipping.'.format(
                _LOG_PREFIX, item_data, str(e)
            ))
            continue
    
    return item_instances


def fetch_and_save_all_turn_14_brand_data() -> None:
    logger.info('{} Fetching all Turn 14 brand data.'.format(_LOG_PREFIX))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value
    ).first()
    if not turn_14_provider:
        logger.info('{} No Turn 14 provider found.'.format(_LOG_PREFIX))
        return

    all_brands = src_models.BrandProviders.objects.filter(
        provider=turn_14_provider,
    )
    if not all_brands.exists():
        logger.info('{} No brands found for Turn 14 provider.'.format(_LOG_PREFIX))
        return

    for brand_provider in all_brands:
        brand = brand_provider.brand
        
        brand_mapping = src_models.BrandTurn14BrandMapping.objects.filter(
            brand=brand
        ).first()
        
        if not brand_mapping:
            logger.warning('{} No Turn14Brand mapping found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, brand.name
            ))
            continue
        
        turn_14_brand = brand_mapping.turn14_brand
        
        company_brand = src_models.CompanyBrands.objects.filter(
            brand=brand
        ).first()
        
        if not company_brand:
            logger.warning('{} No company found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, turn_14_brand.name
            ))
            continue
        
        company = company_brand.company
        
        company_provider = src_models.CompanyProviders.objects.filter(
            company=company,
            provider=turn_14_provider
        ).first()
        
        if not company_provider:
            logger.warning('{} No company provider found for company: {} and brand: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name
            ))
            continue
        
        credentials = company_provider.credentials
        
        try:
            api_client = turn_14_client.Turn14ApiClient(credentials=credentials)
        except ValueError as e:
            logger.error('{} Invalid credentials for company: {} and brand: {}. Error: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name, str(e)
            ))
            continue
        
        brand_id = str(turn_14_brand.external_id)
        page = 1
        
        logger.info('{} Fetching brand data for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, turn_14_brand.name, brand_id
        ))
        
        while page is not None:
            try:
                data_items, next_page = api_client.get_brand_media(brand_id=brand_id, page=page)
            except turn_14_exceptions.Turn14APIException as e:
                logger.error('{} Turn 14 API error for brand: {} (external_id: {}), page: {}. Error: {}. Skipping brand.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page, str(e)
                ))
                break
            
            if not data_items:
                logger.warning('{} No brand data returned for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page
                ))
                page = next_page
                continue
            
            logger.info('{} Fetched {} brand data items for brand: {} (external_id: {}), page: {}.'.format(
                _LOG_PREFIX, len(data_items), turn_14_brand.name, brand_id, page
            ))
            
            data_instances = _transform_brand_data(data_items, turn_14_brand)
            
            if not data_instances:
                logger.warning('{} No valid brand data instances created for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page
                ))
                page = next_page
                continue
            
            try:
                upserted_data = pgbulk.upsert(
                    src_models.Turn14BrandData,
                    data_instances,
                    unique_fields=['external_id'],
                    update_fields=[
                        'brand', 'type', 'files', 'descriptions', 'relationships'
                    ],
                    returning=True,
                )
                
                logger.info('{} Successfully upserted {} brand data items for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, len(upserted_data) if upserted_data else 0, turn_14_brand.name, brand_id, page
                ))
            except Exception as e:
                logger.error('{} Error during bulk upsert for brand: {} (external_id: {}), page: {}. Error: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page, str(e)
                ))
                page = next_page
                continue
            
            page = next_page
        
        logger.info('{} Completed fetching brand data for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, turn_14_brand.name, brand_id
        ))


def _transform_brand_data(data_items: typing.List[typing.Dict], turn_14_brand: src_models.Turn14Brand) -> typing.List[src_models.Turn14BrandData]:
    data_instances = []
    
    for data_item in data_items:
        try:
            external_id = str(data_item.get('id', ''))
            
            if not external_id:
                logger.warning('{} Skipping brand data item with missing external_id: {}'.format(
                    _LOG_PREFIX, data_item
                ))
                continue
            
            data_instance = src_models.Turn14BrandData(
                external_id=external_id,
                brand=turn_14_brand,
                type=data_item.get('type'),
                files=data_item.get('files'),
                descriptions=data_item.get('descriptions'),
                relationships=data_item.get('relationships'),
            )
            
            data_instances.append(data_instance)
            
        except Exception as e:
            logger.warning('{} Error transforming brand data item {}: {}. Skipping.'.format(
                _LOG_PREFIX, data_item, str(e)
            ))
            continue
    
    return data_instances


def fetch_and_save_all_turn_14_brand_pricing() -> None:
    logger.info('{} Fetching all Turn 14 brand pricing.'.format(_LOG_PREFIX))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value
    ).first()
    if not turn_14_provider:
        logger.info('{} No Turn 14 provider found.'.format(_LOG_PREFIX))
        return

    all_brands = src_models.BrandProviders.objects.filter(
        provider=turn_14_provider,
    )
    if not all_brands.exists():
        logger.info('{} No brands found for Turn 14 provider.'.format(_LOG_PREFIX))
        return

    for brand_provider in all_brands:
        brand = brand_provider.brand
        
        brand_mapping = src_models.BrandTurn14BrandMapping.objects.filter(
            brand=brand
        ).first()
        
        if not brand_mapping:
            logger.warning('{} No Turn14Brand mapping found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, brand.name
            ))
            continue
        
        turn_14_brand = brand_mapping.turn14_brand
        
        company_brand = src_models.CompanyBrands.objects.filter(
            brand=brand
        ).first()
        
        if not company_brand:
            logger.warning('{} No company found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, turn_14_brand.name
            ))
            continue
        
        company = company_brand.company
        
        company_provider = src_models.CompanyProviders.objects.filter(
            company=company,
            provider=turn_14_provider
        ).first()
        
        if not company_provider:
            logger.warning('{} No company provider found for company: {} and brand: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name
            ))
            continue
        
        credentials = company_provider.credentials
        
        try:
            api_client = turn_14_client.Turn14ApiClient(credentials=credentials)
        except ValueError as e:
            logger.error('{} Invalid credentials for company: {} and brand: {}. Error: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name, str(e)
            ))
            continue
        
        brand_id = int(turn_14_brand.external_id)
        page = 1
        
        logger.info('{} Fetching brand pricing for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, turn_14_brand.name, brand_id
        ))
        
        while page is not None:
            try:
                pricing_data, next_page = api_client.get_pricelists(brand_id=brand_id, page=page)
            except turn_14_exceptions.Turn14APIException as e:
                logger.error('{} Turn 14 API error for brand: {} (external_id: {}), page: {}. Error: {}. Skipping brand.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page, str(e)
                ))
                break
            
            if not pricing_data:
                logger.warning('{} No pricing data returned for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page
                ))
                page = next_page
                continue
            
            logger.info('{} Fetched {} pricing items for brand: {} (external_id: {}), page: {}.'.format(
                _LOG_PREFIX, len(pricing_data), turn_14_brand.name, brand_id, page
            ))
            
            pricing_instances = _transform_pricing_data(pricing_data, turn_14_brand)
            
            if not pricing_instances:
                logger.warning('{} No valid pricing instances created for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page
                ))
                page = next_page
                continue
            
            try:
                upserted_pricing = pgbulk.upsert(
                    src_models.Turn14BrandPricing,
                    pricing_instances,
                    unique_fields=['external_id'],
                    update_fields=[
                        'brand', 'type', 'purchase_cost', 'has_map', 'can_purchase', 'pricelists'
                    ],
                    returning=True,
                )
                
                logger.info('{} Successfully upserted {} pricing items for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, len(upserted_pricing) if upserted_pricing else 0, turn_14_brand.name, brand_id, page
                ))
            except Exception as e:
                logger.error('{} Error during bulk upsert for brand: {} (external_id: {}), page: {}. Error: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page, str(e)
                ))
                page = next_page
                continue
            
            page = next_page
        
        logger.info('{} Completed fetching pricing for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, turn_14_brand.name, brand_id
        ))


def _transform_pricing_data(pricing_data: typing.List[typing.Dict], turn_14_brand: src_models.Turn14Brand) -> typing.List[src_models.Turn14BrandPricing]:
    pricing_instances = []
    
    for pricing_item in pricing_data:
        try:
            external_id = str(pricing_item.get('id', ''))
            
            if not external_id:
                logger.warning('{} Skipping pricing item with missing external_id: {}'.format(
                    _LOG_PREFIX, pricing_item
                ))
                continue
            
            attributes = pricing_item.get('attributes', {})
            
            purchase_cost = None
            if attributes.get('purchase_cost') is not None:
                try:
                    purchase_cost = Decimal(str(attributes.get('purchase_cost')))
                except Exception:
                    pass
            
            pricing_instance = src_models.Turn14BrandPricing(
                external_id=external_id,
                brand=turn_14_brand,
                type=pricing_item.get('type'),
                purchase_cost=purchase_cost,
                has_map=bool(attributes.get('has_map', False)),
                can_purchase=bool(attributes.get('can_purchase', False)),
                pricelists=attributes.get('pricelists'),
            )
            
            pricing_instances.append(pricing_instance)
            
        except Exception as e:
            logger.warning('{} Error transforming pricing data {}: {}. Skipping.'.format(
                _LOG_PREFIX, pricing_item, str(e)
            ))
            continue
    
    return pricing_instances


def fetch_and_save_all_turn_14_brand_inventory() -> None:
    logger.info('{} Fetching all Turn 14 brand inventory.'.format(_LOG_PREFIX))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value
    ).first()
    if not turn_14_provider:
        logger.info('{} No Turn 14 provider found.'.format(_LOG_PREFIX))
        return

    all_brands = src_models.BrandProviders.objects.filter(
        provider=turn_14_provider,
    )
    if not all_brands.exists():
        logger.info('{} No brands found for Turn 14 provider.'.format(_LOG_PREFIX))
        return

    for brand_provider in all_brands:
        brand = brand_provider.brand
        
        brand_mapping = src_models.BrandTurn14BrandMapping.objects.filter(
            brand=brand
        ).first()
        
        if not brand_mapping:
            logger.warning('{} No Turn14Brand mapping found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, brand.name
            ))
            continue
        
        turn_14_brand = brand_mapping.turn14_brand
        
        company_brand = src_models.CompanyBrands.objects.filter(
            brand=brand
        ).first()
        
        if not company_brand:
            logger.warning('{} No company found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, turn_14_brand.name
            ))
            continue
        
        company = company_brand.company
        
        company_provider = src_models.CompanyProviders.objects.filter(
            company=company,
            provider=turn_14_provider
        ).first()
        
        if not company_provider:
            logger.warning('{} No company provider found for company: {} and brand: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name
            ))
            continue
        
        credentials = company_provider.credentials
        
        try:
            api_client = turn_14_client.Turn14ApiClient(credentials=credentials)
        except ValueError as e:
            logger.error('{} Invalid credentials for company: {} and brand: {}. Error: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name, str(e)
            ))
            continue
        
        brand_id = int(turn_14_brand.external_id)
        page = 1
        
        logger.info('{} Fetching brand inventory for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, turn_14_brand.name, brand_id
        ))
        
        while page is not None:
            try:
                inventory_data, next_page = api_client.get_inventory_items_for_brand(brand_id=brand_id, page=page)
            except turn_14_exceptions.Turn14APIException as e:
                logger.error('{} Turn 14 API error for brand: {} (external_id: {}), page: {}. Error: {}. Skipping brand.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page, str(e)
                ))
                break
            
            if not inventory_data:
                logger.warning('{} No inventory data returned for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page
                ))
                page = next_page
                continue
            
            logger.info('{} Fetched {} inventory items for brand: {} (external_id: {}), page: {}.'.format(
                _LOG_PREFIX, len(inventory_data), turn_14_brand.name, brand_id, page
            ))
            
            inventory_instances = _transform_inventory_data(inventory_data, turn_14_brand)
            
            if not inventory_instances:
                logger.warning('{} No valid inventory instances created for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page
                ))
                page = next_page
                continue
            
            try:
                upserted_inventory = pgbulk.upsert(
                    src_models.Turn14BrandInventory,
                    inventory_instances,
                    unique_fields=['external_id'],
                    update_fields=[
                        'brand', 'type', 'inventory', 'manufacturer', 'eta', 'relationships', 'total_inventory'
                    ],
                    returning=True,
                )
                
                logger.info('{} Successfully upserted {} inventory items for brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, len(upserted_inventory) if upserted_inventory else 0, turn_14_brand.name, brand_id, page
                ))
            except Exception as e:
                logger.error('{} Error during bulk upsert for brand: {} (external_id: {}), page: {}. Error: {}.'.format(
                    _LOG_PREFIX, turn_14_brand.name, brand_id, page, str(e)
                ))
                page = next_page
                continue
            
            page = next_page
        
        logger.info('{} Completed fetching inventory for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, turn_14_brand.name, brand_id
        ))


def _transform_inventory_data(inventory_data: typing.List[typing.Dict], turn_14_brand: src_models.Turn14Brand) -> typing.List[src_models.Turn14BrandInventory]:
    inventory_instances = []
    
    for inventory_item in inventory_data:
        try:
            external_id = str(inventory_item.get('id', ''))
            
            if not external_id:
                logger.warning('{} Skipping inventory item with missing external_id: {}'.format(
                    _LOG_PREFIX, inventory_item
                ))
                continue
            
            attributes = inventory_item.get('attributes', {})
            inventory = attributes.get('inventory', {})
            manufacturer = attributes.get('manufacturer', {})
            
            total_inventory = 0
            
            if isinstance(inventory, dict):
                for location_id, quantity in inventory.items():
                    if isinstance(quantity, (int, float)):
                        total_inventory += int(quantity)
            
            if isinstance(manufacturer, dict):
                manufacturer_stock = manufacturer.get('stock')
                if isinstance(manufacturer_stock, (int, float)):
                    total_inventory += int(manufacturer_stock)
            
            inventory_instance = src_models.Turn14BrandInventory(
                external_id=external_id,
                brand=turn_14_brand,
                type=inventory_item.get('type'),
                inventory=inventory,
                manufacturer=manufacturer,
                eta=attributes.get('eta'),
                relationships=inventory_item.get('relationships'),
                total_inventory=total_inventory if total_inventory > 0 else None,
            )
            
            inventory_instances.append(inventory_instance)
            
        except Exception as e:
            logger.warning('{} Error transforming inventory data {}: {}. Skipping.'.format(
                _LOG_PREFIX, inventory_item, str(e)
            ))
            continue

    return inventory_instances


def fetch_and_save_turn_14_items_updates() -> None:
    logger.info('{} Fetching Turn 14 items updates.'.format(_LOG_PREFIX))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value
    ).first()
    if not turn_14_provider:
        logger.info('{} No Turn 14 provider found.'.format(_LOG_PREFIX))
        return

    primary_provider = src_models.CompanyProviders.objects.filter(
        provider=turn_14_provider,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        primary=True
    ).first()

    if not primary_provider:
        logger.info('{} No turn 14 active primary provider found.'.format(_LOG_PREFIX))
        return

    credentials = primary_provider.credentials
    
    try:
        api_client = turn_14_client.Turn14ApiClient(credentials=credentials)
    except ValueError as e:
        logger.error('{} Invalid credentials or configuration: {}'.format(_LOG_PREFIX, str(e)))
        raise

    items_with_brands = src_models.Turn14Items.objects.filter(
        brand__isnull=False
    ).select_related('brand').values_list('brand__external_id', flat=True).distinct()
    
    items_with_external_brand_id = src_models.Turn14Items.objects.filter(
        external_brand_id__isnull=False
    ).values_list('external_brand_id', flat=True).distinct()
    
    existing_brand_ids = set()
    for brand_id in items_with_brands:
        if brand_id:
            existing_brand_ids.add(str(brand_id))
    for brand_id in items_with_external_brand_id:
        if brand_id:
            existing_brand_ids.add(str(brand_id))
    
    if not existing_brand_ids:
        logger.warning('{} No brands with items found. Skipping items updates.'.format(_LOG_PREFIX))
        return

    logger.info('{} Found {} brands with items.'.format(
        _LOG_PREFIX, len(existing_brand_ids)
    ))

    brand_id_to_turn14_brand = {
        str(brand.external_id): brand
        for brand in src_models.Turn14Brand.objects.filter(external_id__in=existing_brand_ids)
    }

    page = 1
    days = 1
    total_processed = 0
    total_skipped = 0

    while page is not None:
        try:
            items_updates, next_page = api_client.get_items_updates(page=page, days=days)
        except turn_14_exceptions.Turn14APIException as e:
            logger.error('{} Turn 14 API error for items updates, page: {}. Error: {}. Stopping.'.format(
                _LOG_PREFIX, page, str(e)
            ))
            break

        if not items_updates:
            logger.warning('{} No items updates returned for page: {}.'.format(
                _LOG_PREFIX, page
            ))
            page = next_page
            continue

        logger.info('{} Fetched {} items update items for page: {}.'.format(
            _LOG_PREFIX, len(items_updates), page
        ))

        filtered_updates = []
        for item in items_updates:
            attributes = item.get('attributes', {})
            brand_id = attributes.get('brand_id')
            if brand_id and str(brand_id) in existing_brand_ids:
                filtered_updates.append(item)
            else:
                total_skipped += 1

        if not filtered_updates:
            logger.info('{} No updates for existing brands on page: {}. Skipping.'.format(
                _LOG_PREFIX, page
            ))
            page = next_page
            continue

        logger.info('{} Filtered to {} updates for existing brands on page: {}.'.format(
            _LOG_PREFIX, len(filtered_updates), page
        ))

        item_instances = _transform_items_update_data(filtered_updates, brand_id_to_turn14_brand)

        if not item_instances:
            logger.warning('{} No valid item instances created for page: {}.'.format(
                _LOG_PREFIX, page
            ))
            page = next_page
            continue

        try:
            upserted_items = pgbulk.upsert(
                src_models.Turn14Items,
                item_instances,
                unique_fields=['external_id'],
                update_fields=[
                    'brand', 'product_name', 'part_number', 'mfr_part_number', 'part_description',
                    'category', 'subcategory', 'external_brand_id', 'brand_name', 'price_group_id',
                    'price_group', 'active', 'born_on_date', 'regular_stock',
                    'powersports_indicator', 'dropship_controller_id', 'air_freight_prohibited',
                    'not_carb_approved', 'carb_acknowledgement_required', 'ltl_freight_required',
                    'prop_65', 'epa', 'units_per_sku', 'clearance_item', 'thumbnail',
                    'barcode', 'dimensions', 'warehouse_availability'
                ],
                returning=True,
            )

            processed_count = len(upserted_items) if upserted_items else 0
            total_processed += processed_count

            logger.info('{} Successfully upserted {} items updates for page: {}.'.format(
                _LOG_PREFIX, processed_count, page
            ))
        except Exception as e:
            logger.error('{} Error during bulk upsert for page: {}. Error: {}.'.format(
                _LOG_PREFIX, page, str(e)
            ))
            page = next_page
            continue

        page = next_page

    logger.info('{} Completed fetching items updates. Processed: {}, Skipped: {}.'.format(
        _LOG_PREFIX, total_processed, total_skipped
    ))


def _transform_items_update_data(items_data: typing.List[typing.Dict], brand_id_to_turn14_brand: typing.Dict[str, src_models.Turn14Brand]) -> typing.List[src_models.Turn14Items]:
    item_instances = []
    
    for item_data in items_data:
        try:
            external_id = str(item_data.get('id', ''))
            
            if not external_id:
                logger.warning('{} Skipping item update with missing external_id: {}'.format(
                    _LOG_PREFIX, item_data
                ))
                continue
            
            attributes = item_data.get('attributes', {})
            
            brand_id = attributes.get('brand_id')
            turn_14_brand = None
            if brand_id:
                turn_14_brand = brand_id_to_turn14_brand.get(str(brand_id))
            
            if not turn_14_brand:
                logger.warning('{} Skipping item update with unknown brand_id: {}'.format(
                    _LOG_PREFIX, brand_id
                ))
                continue
            
            born_on_date = None
            if attributes.get('born_on_date'):
                try:
                    born_on_date = datetime.strptime(attributes.get('born_on_date'), '%Y-%m-%d').date()
                except Exception:
                    pass
            
            item_instance = src_models.Turn14Items(
                external_id=external_id,
                brand=turn_14_brand,
                product_name=attributes.get('product_name'),
                part_number=attributes.get('part_number'),
                mfr_part_number=attributes.get('mfr_part_number'),
                part_description=attributes.get('part_description'),
                category=attributes.get('category'),
                subcategory=attributes.get('subcategory'),
                external_brand_id=brand_id,
                brand_name=attributes.get('brand'),
                price_group_id=attributes.get('price_group_id'),
                price_group=attributes.get('price_group'),
                active=bool(attributes.get('active', False)),
                born_on_date=born_on_date,
                regular_stock=bool(attributes.get('regular_stock', False)),
                powersports_indicator=bool(attributes.get('powersports_indicator', False)),
                dropship_controller_id=attributes.get('dropship_controller_id'),
                air_freight_prohibited=bool(attributes.get('air_freight_prohibited', False)),
                not_carb_approved=bool(attributes.get('not_carb_approved', False)),
                carb_acknowledgement_required=bool(attributes.get('carb_acknowledgement_required', False)),
                ltl_freight_required=bool(attributes.get('ltl_freight_required', False)),
                prop_65=attributes.get('prop_65'),
                epa=attributes.get('epa'),
                units_per_sku=attributes.get('units_per_sku'),
                clearance_item=bool(attributes.get('clearance_item', False)),
                thumbnail=attributes.get('thumbnail'),
                barcode=attributes.get('barcode'),
                dimensions=attributes.get('dimensions'),
                warehouse_availability=attributes.get('warehouse_availability'),
            )
            
            item_instances.append(item_instance)
            
        except Exception as e:
            logger.warning('{} Error transforming item update data {}: {}. Skipping.'.format(
                _LOG_PREFIX, item_data, str(e)
            ))
            continue
    
    return item_instances


def fetch_and_save_turn_14_inventory_updates() -> None:
    logger.info('{} Fetching Turn 14 inventory updates.'.format(_LOG_PREFIX))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value
    ).first()
    if not turn_14_provider:
        logger.info('{} No Turn 14 provider found.'.format(_LOG_PREFIX))
        return

    primary_provider = src_models.CompanyProviders.objects.filter(
        provider=turn_14_provider,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        primary=True
    ).first()

    if not primary_provider:
        logger.info('{} No turn 14 active primary provider found.'.format(_LOG_PREFIX))
        return

    credentials = primary_provider.credentials
    
    try:
        api_client = turn_14_client.Turn14ApiClient(credentials=credentials)
    except ValueError as e:
        logger.error('{} Invalid credentials or configuration: {}'.format(_LOG_PREFIX, str(e)))
        raise

    existing_item_ids = set(
        src_models.Turn14Items.objects.values_list('external_id', flat=True)
    )
    
    if not existing_item_ids:
        logger.warning('{} No existing items found in Turn14Items. Skipping inventory updates.'.format(_LOG_PREFIX))
        return

    logger.info('{} Found {} existing items in Turn14Items.'.format(
        _LOG_PREFIX, len(existing_item_ids)
    ))

    page = 1
    minutes = 30
    total_processed = 0
    total_skipped = 0

    while page is not None:
        try:
            inventory_updates, next_page = api_client.get_inventory_items_updates(page=page, minutes=minutes)
        except turn_14_exceptions.Turn14APIException as e:
            logger.error('{} Turn 14 API error for inventory updates, page: {}. Error: {}. Stopping.'.format(
                _LOG_PREFIX, page, str(e)
            ))
            break

        if not inventory_updates:
            logger.warning('{} No inventory updates returned for page: {}.'.format(
                _LOG_PREFIX, page
            ))
            page = next_page
            continue

        logger.info('{} Fetched {} inventory update items for page: {}.'.format(
            _LOG_PREFIX, len(inventory_updates), page
        ))

        filtered_updates = [
            item for item in inventory_updates
            if str(item.get('id', '')) in existing_item_ids
        ]

        total_skipped += len(inventory_updates) - len(filtered_updates)

        if not filtered_updates:
            logger.info('{} No updates for existing items on page: {}. Skipping.'.format(
                _LOG_PREFIX, page
            ))
            page = next_page
            continue

        logger.info('{} Filtered to {} updates for existing items on page: {}.'.format(
            _LOG_PREFIX, len(filtered_updates), page
        ))

        inventory_instances = _transform_inventory_update_data(filtered_updates)

        if not inventory_instances:
            logger.warning('{} No valid inventory instances created for page: {}.'.format(
                _LOG_PREFIX, page
            ))
            page = next_page
            continue

        try:
            upserted_inventory = pgbulk.upsert(
                src_models.Turn14BrandInventory,
                inventory_instances,
                unique_fields=['external_id'],
                update_fields=[
                    'type', 'inventory', 'manufacturer', 'eta', 'relationships', 'total_inventory'
                ],
                returning=True,
            )

            processed_count = len(upserted_inventory) if upserted_inventory else 0
            total_processed += processed_count

            logger.info('{} Successfully upserted {} inventory updates for page: {}.'.format(
                _LOG_PREFIX, processed_count, page
            ))
        except Exception as e:
            logger.error('{} Error during bulk upsert for page: {}. Error: {}.'.format(
                _LOG_PREFIX, page, str(e)
            ))
            page = next_page
            continue

        page = next_page

    logger.info('{} Completed fetching inventory updates. Processed: {}, Skipped: {}.'.format(
        _LOG_PREFIX, total_processed, total_skipped
    ))


def _transform_inventory_update_data(inventory_data: typing.List[typing.Dict]) -> typing.List[src_models.Turn14BrandInventory]:
    inventory_instances = []

    for inventory_item in inventory_data:
        try:
            external_id = str(inventory_item.get('id', ''))

            if not external_id:
                logger.warning('{} Skipping inventory update item with missing external_id: {}'.format(
                    _LOG_PREFIX, inventory_item
                ))
                continue

            attributes = inventory_item.get('attributes', {})
            inventory = attributes.get('inventory', {})
            manufacturer = attributes.get('manufacturer', {})
            eta = attributes.get('eta')

            total_inventory = 0

            if isinstance(inventory, dict):
                for location_id, quantity in inventory.items():
                    if isinstance(quantity, (int, float)):
                        total_inventory += int(quantity)

            if isinstance(manufacturer, dict):
                manufacturer_stock = manufacturer.get('stock')
                if isinstance(manufacturer_stock, (int, float)):
                    total_inventory += int(manufacturer_stock)

            inventory_instance = src_models.Turn14BrandInventory(
                external_id=external_id,
                brand=None,
                type=inventory_item.get('type'),
                inventory=inventory,
                manufacturer=manufacturer,
                eta=eta,
                relationships=inventory_item.get('relationships'),
                total_inventory=total_inventory if total_inventory > 0 else None,
            )

            inventory_instances.append(inventory_instance)

        except Exception as e:
            logger.warning('{} Error transforming inventory update data {}: {}. Skipping.'.format(
                _LOG_PREFIX, inventory_item, str(e)
            ))
            continue

    return inventory_instances
