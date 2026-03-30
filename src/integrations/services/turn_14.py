import logging
import time
import typing
from datetime import datetime
from decimal import Decimal
from django.db.models.functions import Upper
from django.utils import timezone

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
            update_fields=['name', 'dropship', 'price_groups', 'logo', 'aaia_code', 'updated_at'],
            returning=True,
        )
    except Exception as e:
        logger.error('{} Error during bulk upsert operation: {}'.format(_LOG_PREFIX, str(e)))
        raise
    
    logger.info('{} Successfully upserted {} Turn 14 brands.'.format(
        _LOG_PREFIX, len(upserted_brands) if upserted_brands else 0
    ))


def _normalize_aaia_codes(aaia_code: typing.Optional[str]) -> typing.List[str]:
    """Split aaia_code by comma and return non-empty stripped parts."""
    if not aaia_code or not str(aaia_code).strip():
        return []
    return [p.strip() for p in str(aaia_code).split(',') if p and p.strip()]


def _turn14_brand_name_upper_for_sync(turn14_brand: src_models.Turn14Brand) -> str:
    name_upper = (turn14_brand.name or '').strip().upper()
    if not name_upper:
        name_upper = 'BRAND_{}'.format(turn14_brand.external_id)
    return name_upper


def sync_unmapped_turn_14_brands_to_brands() -> typing.List[src_models.Turn14Brand]:
    """
    For each Turn14Brand that does not yet have a BrandTurn14BrandMapping:
    find or create a Brand (match by aaia_code then name; create with uppercase name if new),
    then add BrandTurn14BrandMapping, BrandProviders (Turn 14), and CompanyBrands (TICK_PERFORMANCE).
    Returns the list of Turn14Brand instances that were synced (for use by fetch_and_save_turn_14_items_for_turn14_brands).
    """
    logger.info('{} Syncing unmapped Turn 14 brands to Brands.'.format(_LOG_PREFIX))

    turn14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value,
    ).first()
    if not turn14_provider:
        logger.warning('{} Turn 14 provider not found. Skipping sync.'.format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name='TICK_PERFORMANCE').first()
    if not tick_company:
        logger.warning('{} Company TICK_PERFORMANCE not found. Skipping sync.'.format(_LOG_PREFIX))
        return []

    mapped_turn14_ids = set(
        src_models.BrandTurn14BrandMapping.objects.values_list('turn14_brand_id', flat=True).distinct()
    )
    unmapped_turn14_brands = list(
        src_models.Turn14Brand.objects.exclude(id__in=mapped_turn14_ids).order_by('id')
    )

    if not unmapped_turn14_brands:
        logger.info('{} No unmapped Turn 14 brands. Nothing to sync.'.format(_LOG_PREFIX))
        return []

    logger.info('{} Found {} unmapped Turn 14 brands.'.format(_LOG_PREFIX, len(unmapped_turn14_brands)))

    all_aaia: typing.Set[str] = set()
    for t14 in unmapped_turn14_brands:
        for code in _normalize_aaia_codes(t14.aaia_code):
            all_aaia.add(code)

    aaia_to_brand: typing.Dict[str, src_models.Brands] = {}
    if all_aaia:
        for b in src_models.Brands.objects.filter(aaia_code__in=all_aaia).order_by('id'):
            if b.aaia_code and b.aaia_code not in aaia_to_brand:
                aaia_to_brand[b.aaia_code] = b

    name_upper_keys: typing.Set[str] = set()
    for t14 in unmapped_turn14_brands:
        hit = False
        for code in _normalize_aaia_codes(t14.aaia_code):
            if code in aaia_to_brand:
                hit = True
                break
        if not hit and (t14.name or '').strip():
            name_upper_keys.add((t14.name or '').strip().upper())

    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper('name'))
            .filter(_name_u__in=name_upper_keys)
            .order_by('id')
        ):
            key = (b.name or '').strip().upper()
            if key not in brands_by_upper_name:
                brands_by_upper_name[key] = b

    resolved_by_turn14_id: typing.Dict[int, src_models.Brands] = {}
    new_brand_specs: typing.Dict[str, typing.Optional[str]] = {}
    for t14 in sorted(unmapped_turn14_brands, key=lambda x: x.id):
        brand = None
        for code in _normalize_aaia_codes(t14.aaia_code):
            brand = aaia_to_brand.get(code)
            if brand:
                break
        if not brand:
            nm = (t14.name or '').strip().upper()
            if nm:
                brand = brands_by_upper_name.get(nm)
        if brand:
            resolved_by_turn14_id[t14.id] = brand
            continue
        nu = _turn14_brand_name_upper_for_sync(t14)
        if nu not in new_brand_specs:
            parts = _normalize_aaia_codes(t14.aaia_code)
            new_brand_specs[nu] = parts[0] if parts else None

    created_brands = 0
    if new_brand_specs:
        existing_names = set(
            src_models.Brands.objects.filter(
                name__in=list(new_brand_specs.keys()),
            ).values_list('name', flat=True)
        )
        new_brand_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=aaia_val,
            )
            for name, aaia_val in new_brand_specs.items()
            if name not in existing_names
        ]
        if new_brand_rows:
            src_models.Brands.objects.bulk_create(new_brand_rows, ignore_conflicts=True)
            created_brands = len(new_brand_rows)
        by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_brand_specs.keys()))
        }
        for t14 in unmapped_turn14_brands:
            if t14.id not in resolved_by_turn14_id:
                nu = _turn14_brand_name_upper_for_sync(t14)
                resolved_by_turn14_id[t14.id] = by_name[nu]

    mapping_models = [
        src_models.BrandTurn14BrandMapping(
            brand_id=resolved_by_turn14_id[t14.id].id,
            turn14_brand_id=t14.id,
        )
        for t14 in unmapped_turn14_brands
    ]
    try:
        pgbulk.upsert(
            src_models.BrandTurn14BrandMapping,
            mapping_models,
            unique_fields=['brand', 'turn14_brand'],
            update_fields=['updated_at'],
        )
    except Exception as e:
        logger.error('{} Error upserting BrandTurn14BrandMapping: {}'.format(_LOG_PREFIX, str(e)))
        raise

    brand_ids = {resolved_by_turn14_id[t14.id].id for t14 in unmapped_turn14_brands}
    existing_bp_ids = set(
        src_models.BrandProviders.objects.filter(
            provider=turn14_provider,
            brand_id__in=brand_ids,
        ).values_list('brand_id', flat=True)
    )
    bp_to_create = [
        src_models.BrandProviders(brand_id=bid, provider_id=turn14_provider.id)
        for bid in brand_ids
        if bid not in existing_bp_ids
    ]
    if bp_to_create:
        src_models.BrandProviders.objects.bulk_create(bp_to_create, ignore_conflicts=True)
    created_brand_providers = len(bp_to_create)

    existing_cb_ids = set(
        src_models.CompanyBrands.objects.filter(
            company=tick_company,
            brand_id__in=brand_ids,
        ).values_list('brand_id', flat=True)
    )
    active_val = src_enums.CompanyBrandStatus.ACTIVE.value
    active_name = src_enums.CompanyBrandStatus.ACTIVE.name
    cb_to_create = [
        src_models.CompanyBrands(
            company_id=tick_company.id,
            brand_id=bid,
            status=active_val,
            status_name=active_name,
        )
        for bid in brand_ids
        if bid not in existing_cb_ids
    ]
    if cb_to_create:
        src_models.CompanyBrands.objects.bulk_create(cb_to_create, ignore_conflicts=True)
    created_company_brands = len(cb_to_create)

    logger.info(
        '{} Sync complete. Brands created: {}, BrandTurn14BrandMapping: upserted {}, '
        'BrandProviders: {}, CompanyBrands: {}.'.format(
            _LOG_PREFIX, created_brands, len(mapping_models), created_brand_providers, created_company_brands
        )
    )
    return unmapped_turn14_brands


def fetch_and_save_turn_14_locations() -> None:
    """Fetch Turn14 locations from GET /v1/locations and upsert into Turn14Location."""
    logger.info('{} Started fetching and saving Turn 14 locations.'.format(_LOG_PREFIX))

    primary_provider = src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.TURN_14.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        primary=True,
    ).first()

    if not primary_provider:
        logger.info('{} No Turn 14 active provider found.'.format(_LOG_PREFIX))
        return

    credentials = primary_provider.credentials
    try:
        api_client = turn_14_client.Turn14ApiClient(credentials=credentials)
    except ValueError as e:
        logger.error('{} Invalid credentials: {}'.format(_LOG_PREFIX, str(e)))
        raise

    try:
        locations_data = api_client.get_locations()
    except turn_14_exceptions.Turn14APIException as e:
        logger.error('{} Turn 14 API error: {}'.format(_LOG_PREFIX, str(e)))
        raise

    if not locations_data:
        logger.warning('{} No locations returned from Turn 14 API.'.format(_LOG_PREFIX))
        return

    instances = _transform_locations_data(locations_data)
    if not instances:
        logger.warning('{} No valid location instances after transformation.'.format(_LOG_PREFIX))
        return

    try:
        pgbulk.upsert(
            src_models.Turn14Location,
            instances,
            unique_fields=['external_id'],
            update_fields=['name', 'street', 'city', 'state', 'country', 'zip_code', 'updated_at'],
        )
    except Exception as e:
        logger.error('{} Error during locations upsert: {}'.format(_LOG_PREFIX, str(e)))
        raise

    logger.info('{} Successfully upserted {} Turn 14 locations.'.format(_LOG_PREFIX, len(instances)))


def _transform_locations_data(locations_data: typing.List[typing.Dict]) -> typing.List[src_models.Turn14Location]:
    instances = []
    for item in locations_data:
        try:
            external_id = str(item.get('id', '')).strip()
            if not external_id:
                continue
            attrs = item.get('attributes', {}) or {}
            instances.append(
                src_models.Turn14Location(
                    external_id=external_id,
                    name=str(attrs.get('Name', '')).strip() or external_id,
                    street=str(attrs.get('Street', '')).strip(),
                    city=str(attrs.get('City', '')).strip(),
                    state=str(attrs.get('State', '')).strip(),
                    country=str(attrs.get('Country', '')).strip(),
                    zip_code=str(attrs.get('ZipCode', '')).strip(),
                )
            )
        except Exception as e:
            logger.warning('{} Error transforming location {}: {}. Skipping.'.format(
                _LOG_PREFIX, item, str(e)
            ))
    return instances


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
            
            name = (
                str(attributes.get('name', '')).strip() or f'Brand {external_id}'
            ).upper()
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
        provider=turn_14_provider
    )
    if not all_brands.exists():
        logger.info('{} No brands found for Turn 14 provider.'.format(_LOG_PREFIX))
        return

    for brand_provider in all_brands:
        brand = brand_provider.brand

        if brand.status_name != src_enums.BrandProviderStatus.ACTIVE.name:
            logger.info('{} Brand {} status is not active'.format(_LOG_PREFIX, brand.name))
            continue
        
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
                        'barcode', 'dimensions', 'warehouse_availability', 'updated_at'
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


def fetch_and_save_turn_14_items_for_turn14_brands(
    turn14_brands: typing.List[src_models.Turn14Brand],
) -> None:
    """
    Fetch and save Turn 14 items for a given list of Turn14Brand instances (e.g. newly synced brands).
    Uses the same per-brand logic as fetch_and_save_all_turn_14_brand_items: resolves company via
    CompanyBrands (brand + TICK_PERFORMANCE), credentials via CompanyProviders, then fetches items
    and upserts into Turn14Items.
    """
    if not turn14_brands:
        logger.info('{} No Turn14 brands provided. Skipping items fetch.'.format(_LOG_PREFIX))
        return

    logger.info('{} Fetching items for {} Turn 14 brand(s).'.format(_LOG_PREFIX, len(turn14_brands)))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value,
    ).first()
    if not turn_14_provider:
        logger.warning('{} No Turn 14 provider found. Skipping.'.format(_LOG_PREFIX))
        return

    tick_company = src_models.Company.objects.filter(name='TICK_PERFORMANCE').first()
    if not tick_company:
        logger.warning('{} Company TICK_PERFORMANCE not found. Skipping.'.format(_LOG_PREFIX))
        return

    for turn_14_brand in turn14_brands:
        brand_mapping = src_models.BrandTurn14BrandMapping.objects.filter(
            turn14_brand=turn_14_brand,
        ).select_related('brand').first()
        if not brand_mapping:
            logger.warning('{} No BrandTurn14BrandMapping for Turn14Brand: {} (id={}). Skipping.'.format(
                _LOG_PREFIX, turn_14_brand.name, turn_14_brand.id
            ))
            continue

        brand = brand_mapping.brand
        if brand.status_name != src_enums.BrandProviderStatus.ACTIVE.name:
            logger.info('{} Brand {} status is not active. Skipping.'.format(_LOG_PREFIX, brand.name))
            continue

        company_brand = src_models.CompanyBrands.objects.filter(
            company=tick_company,
            brand=brand,
        ).first()
        if not company_brand:
            logger.warning('{} No CompanyBrands (TICK_PERFORMANCE) for brand: {}. Skipping.'.format(
                _LOG_PREFIX, turn_14_brand.name
            ))
            continue

        company_provider = src_models.CompanyProviders.objects.filter(
            company=tick_company,
            provider=turn_14_provider,
        ).first()
        if not company_provider:
            logger.warning('{} No CompanyProviders for TICK_PERFORMANCE and Turn 14. Skipping brand: {}.'.format(
                _LOG_PREFIX, turn_14_brand.name
            ))
            continue

        credentials = company_provider.credentials
        try:
            api_client = turn_14_client.Turn14ApiClient(credentials=credentials)
        except ValueError as e:
            logger.error('{} Invalid credentials for company: {} and brand: {}. Error: {}. Skipping.'.format(
                _LOG_PREFIX, tick_company.name, turn_14_brand.name, str(e)
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
                        'barcode', 'dimensions', 'warehouse_availability', 'updated_at'
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

    logger.info('{} Completed fetching items for {} Turn 14 brand(s).'.format(_LOG_PREFIX, len(turn14_brands)))


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
                updated_at=timezone.now(),  # Explicitly set updated_at for bulk operations
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

        if brand.status_name != src_enums.BrandProviderStatus.ACTIVE.name:
            logger.info('{} Brand {} status is not active'.format(_LOG_PREFIX, brand.name))
            continue

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

        counter = 1
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
                break
            
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
                        'brand', 'type', 'files', 'descriptions', 'relationships', 'updated_at',
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


def fetch_and_save_turn_14_brand_data_for_turn14_brands(
    turn14_brands: typing.List[src_models.Turn14Brand],
) -> None:
    """
    Fetch and save Turn 14 brand data (media) for a given list of Turn14Brand instances (e.g. newly synced brands).
    """
    if not turn14_brands:
        logger.info('{} No Turn14 brands provided. Skipping brand data fetch.'.format(_LOG_PREFIX))
        return

    logger.info('{} Fetching brand data for {} Turn 14 brand(s).'.format(_LOG_PREFIX, len(turn14_brands)))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value,
    ).first()
    if not turn_14_provider:
        logger.warning('{} No Turn 14 provider found. Skipping.'.format(_LOG_PREFIX))
        return

    tick_company = src_models.Company.objects.filter(name='TICK_PERFORMANCE').first()
    if not tick_company:
        logger.warning('{} Company TICK_PERFORMANCE not found. Skipping.'.format(_LOG_PREFIX))
        return

    for turn_14_brand in turn14_brands:
        brand_mapping = src_models.BrandTurn14BrandMapping.objects.filter(
            turn14_brand=turn_14_brand,
        ).select_related('brand').first()
        if not brand_mapping:
            logger.warning('{} No BrandTurn14BrandMapping for Turn14Brand: {} (id={}). Skipping.'.format(
                _LOG_PREFIX, turn_14_brand.name, turn_14_brand.id
            ))
            continue

        brand = brand_mapping.brand
        if brand.status_name != src_enums.BrandProviderStatus.ACTIVE.name:
            logger.info('{} Brand {} status is not active. Skipping.'.format(_LOG_PREFIX, brand.name))
            continue

        company_brand = src_models.CompanyBrands.objects.filter(
            company=tick_company,
            brand=brand,
        ).first()
        if not company_brand:
            logger.warning('{} No CompanyBrands (TICK_PERFORMANCE) for brand: {}. Skipping.'.format(
                _LOG_PREFIX, turn_14_brand.name
            ))
            continue

        company_provider = src_models.CompanyProviders.objects.filter(
            company=tick_company,
            provider=turn_14_provider,
        ).first()
        if not company_provider:
            logger.warning('{} No CompanyProviders for TICK_PERFORMANCE and Turn 14. Skipping brand: {}.'.format(
                _LOG_PREFIX, turn_14_brand.name
            ))
            continue

        credentials = company_provider.credentials
        try:
            api_client = turn_14_client.Turn14ApiClient(credentials=credentials)
        except ValueError as e:
            logger.error('{} Invalid credentials for company: {} and brand: {}. Error: {}. Skipping.'.format(
                _LOG_PREFIX, tick_company.name, turn_14_brand.name, str(e)
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
                break

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
                        'brand', 'type', 'files', 'descriptions', 'relationships', 'updated_at',
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

    logger.info('{} Completed fetching brand data for {} Turn 14 brand(s).'.format(_LOG_PREFIX, len(turn14_brands)))


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
    logger.info('{} Fetching all Turn 14 brand pricing (per company credentials).'.format(_LOG_PREFIX))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value
    ).first()
    if not turn_14_provider:
        logger.info('{} No Turn 14 provider found.'.format(_LOG_PREFIX))
        return

    company_providers = src_models.CompanyProviders.objects.filter(
        provider=turn_14_provider,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related('company')

    if not company_providers.exists():
        logger.info('{} No active Turn 14 company providers found.'.format(_LOG_PREFIX))
        return

    all_brands = src_models.BrandProviders.objects.filter(
        provider=turn_14_provider,
    ).select_related('brand')
    if not all_brands.exists():
        logger.info('{} No brands found for Turn 14 provider.'.format(_LOG_PREFIX))
        return

    for company_provider in company_providers:
        company = company_provider.company
        try:
            api_client = turn_14_client.Turn14ApiClient(credentials=company_provider.credentials)
        except ValueError as e:
            logger.error('{} Invalid Turn 14 credentials for company: {}. Error: {}. Skipping company.'.format(
                _LOG_PREFIX, company.name, str(e)
            ))
            continue

        for brand_provider in all_brands:
            brand = brand_provider.brand

            if brand.status_name != src_enums.BrandProviderStatus.ACTIVE.name:
                logger.info('{} Brand {} status is not active'.format(_LOG_PREFIX, brand.name))
                continue

            if not src_models.CompanyBrands.objects.filter(
                company=company,
                brand=brand,
                status_name=src_enums.CompanyBrandStatus.ACTIVE.name,
            ).exists():
                continue

            brand_mapping = src_models.BrandTurn14BrandMapping.objects.filter(
                brand=brand
            ).first()

            if not brand_mapping:
                logger.warning('{} No Turn14Brand mapping for brand: {} (company: {}). Skipping.'.format(
                    _LOG_PREFIX, brand.name, company.name
                ))
                continue

            turn_14_brand = brand_mapping.turn14_brand

            brand_id = int(turn_14_brand.external_id)
            page = 1

            logger.info('{} Fetching brand pricing company={} brand: {} (external_id: {}).'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name, brand_id
            ))

            while page is not None:
                try:
                    pricing_data, next_page = api_client.get_pricelists(brand_id=brand_id, page=page)
                except turn_14_exceptions.Turn14APIException as e:
                    logger.error('{} Turn 14 API error company={} brand: {} (external_id: {}), page: {}. Error: {}.'.format(
                        _LOG_PREFIX, company.name, turn_14_brand.name, brand_id, page, str(e)
                    ))
                    break

                if not pricing_data:
                    logger.warning('{} No pricing data company={} brand: {} (external_id: {}), page: {}.'.format(
                        _LOG_PREFIX, company.name, turn_14_brand.name, brand_id, page
                    ))
                    page = next_page
                    break

                logger.info('{} Fetched {} pricing items company={} brand: {} (external_id: {}), page: {}.'.format(
                    _LOG_PREFIX, len(pricing_data), company.name, turn_14_brand.name, brand_id, page
                ))

                pricing_instances = _transform_pricing_data(pricing_data, turn_14_brand, company)

                if not pricing_instances:
                    logger.warning('{} No valid pricing instances company={} brand: {} (external_id: {}), page: {}.'.format(
                        _LOG_PREFIX, company.name, turn_14_brand.name, brand_id, page
                    ))
                    page = next_page
                    continue

                try:
                    upserted_pricing = pgbulk.upsert(
                        src_models.Turn14BrandPricing,
                        pricing_instances,
                        unique_fields=['company', 'external_id'],
                        update_fields=[
                            'brand',
                            'type',
                            'purchase_cost',
                            'has_map',
                            'can_purchase',
                            'pricelists',
                            'updated_at',
                        ],
                        returning=True,
                    )

                    logger.info('{} Upserted {} pricing rows company={} brand: {} page: {}.'.format(
                        _LOG_PREFIX, len(upserted_pricing) if upserted_pricing else 0,
                        company.name, turn_14_brand.name, page,
                    ))
                except Exception as e:
                    logger.error('{} Bulk upsert error company={} brand: {} page: {}. Error: {}.'.format(
                        _LOG_PREFIX, company.name, turn_14_brand.name, page, str(e)
                    ))
                    page = next_page
                    continue

                page = next_page

            logger.info('{} Completed pricing for company={} brand: {} (external_id: {}).'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name, brand_id
            ))


def fetch_and_save_turn_14_brand_pricing_for_turn14_brands(
    turn14_brands: typing.List[src_models.Turn14Brand],
) -> None:
    """
    Fetch and save Turn 14 brand pricing for a given list of Turn14Brand instances (e.g. newly synced brands).
    """
    if not turn14_brands:
        logger.info('{} No Turn14 brands provided. Skipping brand pricing fetch.'.format(_LOG_PREFIX))
        return

    logger.info('{} Fetching brand pricing for {} Turn 14 brand(s).'.format(_LOG_PREFIX, len(turn14_brands)))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value,
    ).first()
    if not turn_14_provider:
        logger.warning('{} No Turn 14 provider found. Skipping.'.format(_LOG_PREFIX))
        return

    company_providers = src_models.CompanyProviders.objects.filter(
        provider=turn_14_provider,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related('company')

    if not company_providers.exists():
        logger.warning('{} No active Turn 14 company providers. Skipping.'.format(_LOG_PREFIX))
        return

    for turn_14_brand in turn14_brands:
        brand_mapping = src_models.BrandTurn14BrandMapping.objects.filter(
            turn14_brand=turn_14_brand,
        ).select_related('brand').first()
        if not brand_mapping:
            logger.warning('{} No BrandTurn14BrandMapping for Turn14Brand: {} (id={}). Skipping.'.format(
                _LOG_PREFIX, turn_14_brand.name, turn_14_brand.id
            ))
            continue

        brand = brand_mapping.brand
        if brand.status_name != src_enums.BrandProviderStatus.ACTIVE.name:
            logger.info('{} Brand {} status is not active. Skipping.'.format(_LOG_PREFIX, brand.name))
            continue

        brand_id = int(turn_14_brand.external_id)

        for company_provider in company_providers:
            company = company_provider.company
            if not src_models.CompanyBrands.objects.filter(
                company=company,
                brand=brand,
                status_name=src_enums.CompanyBrandStatus.ACTIVE.name,
            ).exists():
                continue

            try:
                api_client = turn_14_client.Turn14ApiClient(credentials=company_provider.credentials)
            except ValueError as e:
                logger.error('{} Invalid Turn 14 credentials company={} brand: {}. Error: {}. Skipping.'.format(
                    _LOG_PREFIX, company.name, turn_14_brand.name, str(e)
                ))
                continue

            page = 1
            logger.info('{} Fetching pricing company={} brand: {} (external_id: {}).'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name, brand_id
            ))

            while page is not None:
                try:
                    pricing_data, next_page = api_client.get_pricelists(brand_id=brand_id, page=page)
                except turn_14_exceptions.Turn14APIException as e:
                    logger.error('{} Turn 14 API error company={} brand: {} page: {}. Error: {}.'.format(
                        _LOG_PREFIX, company.name, turn_14_brand.name, page, str(e)
                    ))
                    break

                if not pricing_data:
                    logger.warning('{} No pricing data company={} brand: {} page: {}.'.format(
                        _LOG_PREFIX, company.name, turn_14_brand.name, page
                    ))
                    page = next_page
                    break

                logger.info('{} Fetched {} pricing rows company={} brand: {} page: {}.'.format(
                    _LOG_PREFIX, len(pricing_data), company.name, turn_14_brand.name, page
                ))

                pricing_instances = _transform_pricing_data(pricing_data, turn_14_brand, company)

                if not pricing_instances:
                    page = next_page
                    continue

                try:
                    upserted_pricing = pgbulk.upsert(
                        src_models.Turn14BrandPricing,
                        pricing_instances,
                        unique_fields=['company', 'external_id'],
                        update_fields=[
                            'brand',
                            'type',
                            'purchase_cost',
                            'has_map',
                            'can_purchase',
                            'pricelists',
                            'updated_at',
                        ],
                        returning=True,
                    )

                    logger.info('{} Upserted {} pricing rows company={} brand: {} page: {}.'.format(
                        _LOG_PREFIX, len(upserted_pricing) if upserted_pricing else 0,
                        company.name, turn_14_brand.name, page,
                    ))
                except Exception as e:
                    logger.error('{} Bulk upsert company={} brand: {} page: {}. Error: {}.'.format(
                        _LOG_PREFIX, company.name, turn_14_brand.name, page, str(e)
                    ))
                    page = next_page
                    continue

                page = next_page

            logger.info('{} Completed pricing company={} brand: {} (external_id: {}).'.format(
                _LOG_PREFIX, company.name, turn_14_brand.name, brand_id
            ))

    logger.info('{} Completed fetching brand pricing for {} Turn 14 brand(s).'.format(_LOG_PREFIX, len(turn14_brands)))


def _transform_pricing_data(
    pricing_data: typing.List[typing.Dict],
    turn_14_brand: src_models.Turn14Brand,
    company: src_models.Company,
) -> typing.List[src_models.Turn14BrandPricing]:
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
                company=company,
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

        if brand.status_name != src_enums.BrandProviderStatus.ACTIVE.name:
            logger.info('{} Brand {} status is not active'.format(_LOG_PREFIX, brand.name))
            continue

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
                        'brand',
                        'type',
                        'inventory',
                        'manufacturer',
                        'eta',
                        'relationships',
                        'total_inventory',
                        'updated_at',
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


def fetch_and_save_turn_14_brand_inventory_for_turn14_brands(
    turn14_brands: typing.List[src_models.Turn14Brand],
) -> None:
    """
    Fetch and save Turn 14 brand inventory for a given list of Turn14Brand instances (e.g. newly synced brands).
    """
    if not turn14_brands:
        logger.info('{} No Turn14 brands provided. Skipping brand inventory fetch.'.format(_LOG_PREFIX))
        return

    logger.info('{} Fetching brand inventory for {} Turn 14 brand(s).'.format(_LOG_PREFIX, len(turn14_brands)))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value,
    ).first()
    if not turn_14_provider:
        logger.warning('{} No Turn 14 provider found. Skipping.'.format(_LOG_PREFIX))
        return

    tick_company = src_models.Company.objects.filter(name='TICK_PERFORMANCE').first()
    if not tick_company:
        logger.warning('{} Company TICK_PERFORMANCE not found. Skipping.'.format(_LOG_PREFIX))
        return

    for turn_14_brand in turn14_brands:
        brand_mapping = src_models.BrandTurn14BrandMapping.objects.filter(
            turn14_brand=turn_14_brand,
        ).select_related('brand').first()
        if not brand_mapping:
            logger.warning('{} No BrandTurn14BrandMapping for Turn14Brand: {} (id={}). Skipping.'.format(
                _LOG_PREFIX, turn_14_brand.name, turn_14_brand.id
            ))
            continue

        brand = brand_mapping.brand
        if brand.status_name != src_enums.BrandProviderStatus.ACTIVE.name:
            logger.info('{} Brand {} status is not active. Skipping.'.format(_LOG_PREFIX, brand.name))
            continue

        company_brand = src_models.CompanyBrands.objects.filter(
            company=tick_company,
            brand=brand,
        ).first()
        if not company_brand:
            logger.warning('{} No CompanyBrands (TICK_PERFORMANCE) for brand: {}. Skipping.'.format(
                _LOG_PREFIX, turn_14_brand.name
            ))
            continue

        company_provider = src_models.CompanyProviders.objects.filter(
            company=tick_company,
            provider=turn_14_provider,
        ).first()
        if not company_provider:
            logger.warning('{} No CompanyProviders for TICK_PERFORMANCE and Turn 14. Skipping brand: {}.'.format(
                _LOG_PREFIX, turn_14_brand.name
            ))
            continue

        credentials = company_provider.credentials
        try:
            api_client = turn_14_client.Turn14ApiClient(credentials=credentials)
        except ValueError as e:
            logger.error('{} Invalid credentials for company: {} and brand: {}. Error: {}. Skipping.'.format(
                _LOG_PREFIX, tick_company.name, turn_14_brand.name, str(e)
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
                        'brand',
                        'type',
                        'inventory',
                        'manufacturer',
                        'eta',
                        'relationships',
                        'total_inventory',
                        'updated_at',
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

    logger.info('{} Completed fetching brand inventory for {} Turn 14 brand(s).'.format(_LOG_PREFIX, len(turn14_brands)))


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
    brands_with_updated_items = {}  # id -> Turn14Brand for brands that had items updated this run
    # Rate limiting is handled at the client level (token caching + rate limit decorators)
    # Retry logic for 429 errors as a safety measure
    MAX_RETRIES = 3
    INITIAL_RETRY_DELAY = 5  # seconds

    while page is not None:
        retry_count = 0
        items_updates = None
        next_page = None
        
        while retry_count <= MAX_RETRIES:
            try:
                items_updates, next_page = api_client.get_items_updates(page=page, days=days)
                break  # Success, exit retry loop
            except turn_14_exceptions.Turn14APIBadResponseCodeError as e:
                # Check if it's a rate limit error (429)
                if e.code == 429:
                    if retry_count < MAX_RETRIES:
                        # Exponential backoff: 5s, 10s, 20s
                        retry_delay = INITIAL_RETRY_DELAY * (2 ** retry_count)
                        logger.warning(
                            '{} Rate limit hit (429) for items updates, page: {}. '
                            'Retrying in {} seconds (attempt {}/{}).'.format(
                                _LOG_PREFIX, page, retry_delay, retry_count + 1, MAX_RETRIES
                            )
                        )
                        time.sleep(retry_delay)
                        retry_count += 1
                        continue
                    else:
                        logger.error(
                            '{} Rate limit exceeded (429) for items updates, page: {}. '
                            'Max retries reached. Stopping.'.format(_LOG_PREFIX, page)
                        )
                        return
                else:
                    # Other bad response code, stop
                    logger.error(
                        '{} Turn 14 API error for items updates, page: {}. '
                        'Status code: {}. Error: {}. Stopping.'.format(
                            _LOG_PREFIX, page, e.code, str(e)
                        )
                    )
                    return
            except turn_14_exceptions.Turn14APIException as e:
                # Other API exceptions, stop
                logger.error(
                    '{} Turn 14 API error for items updates, page: {}. Error: {}. Stopping.'.format(
                        _LOG_PREFIX, page, str(e)
                    )
                )
                return
        
        if items_updates is None:
            # Failed after all retries
            logger.error(
                '{} Failed to fetch items updates after {} retries, page: {}. Stopping.'.format(
                    _LOG_PREFIX, MAX_RETRIES, page
                )
            )
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
                    'barcode', 'dimensions', 'warehouse_availability', 'updated_at'
                ],
                returning=True,
            )

            processed_count = len(upserted_items) if upserted_items else 0
            total_processed += processed_count
            for inst in item_instances:
                if inst.brand_id and inst.brand:
                    brands_with_updated_items[inst.brand.id] = inst.brand

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

    # if brands_with_updated_items:
    #     brands_list = list(brands_with_updated_items.values())
    #     logger.info('{} Syncing brand data, pricing, and inventory for {} brand(s) with updated items.'.format(
    #         _LOG_PREFIX, len(brands_list)
    #     ))
    #     fetch_and_save_turn_14_brand_data_for_turn14_brands(brands_list)
    #     fetch_and_save_turn_14_brand_pricing_for_turn14_brands(brands_list)
    #     fetch_and_save_turn_14_brand_inventory_for_turn14_brands(brands_list)
    #     logger.info('{} Completed sync of brand data, pricing, and inventory for brands with updated items.'.format(
    #         _LOG_PREFIX
    #     ))


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
                updated_at=timezone.now(),  # Explicitly set updated_at for bulk operations
            )
            
            item_instances.append(item_instance)
            
        except Exception as e:
            logger.warning('{} Error transforming item update data {}: {}. Skipping.'.format(
                _LOG_PREFIX, item_data, str(e)
            ))
            continue
    
    return item_instances


def fetch_and_save_turn_14_pricing_changes(start_date: str, end_date: str) -> None:
    """
    Fetch pricing changes from GET /v1/pricing/changes for the given date range,
    collect distinct Turn14Brand for affected items, then sync brand pricing for those brands only.
    start_date and end_date should be YYYY-MM-DD.
    """
    logger.info('{} Fetching Turn 14 pricing changes from {} to {}.'.format(
        _LOG_PREFIX, start_date, end_date
    ))

    turn_14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value
    ).first()
    if not turn_14_provider:
        logger.info('{} No Turn 14 provider found.'.format(_LOG_PREFIX))
        return

    company_providers = src_models.CompanyProviders.objects.filter(
        provider=turn_14_provider,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related('company')

    if not company_providers.exists():
        logger.info('{} No active Turn 14 company providers found.'.format(_LOG_PREFIX))
        return

    item_ids = set()
    for company_provider in company_providers:
        try:
            api_client = turn_14_client.Turn14ApiClient(credentials=company_provider.credentials)
        except ValueError as e:
            logger.warning('{} Invalid Turn 14 credentials for company {}: {}. Skipping for pricing changes discovery.'.format(
                _LOG_PREFIX, company_provider.company.name, str(e)
            ))
            continue

        page = 1
        while page is not None:
            try:
                data, next_page = api_client.get_pricing_changes(
                    start_date=start_date,
                    end_date=end_date,
                    page=page,
                )
            except turn_14_exceptions.Turn14APIException as e:
                logger.error('{} Turn 14 pricing changes API error company={} page: {}. Error: {}.'.format(
                    _LOG_PREFIX, company_provider.company.name, page, str(e)
                ))
                raise

            for item in data or []:
                attrs = item.get('attributes', {})
                item_id = attrs.get('itemcode') or item.get('id')
                if item_id is not None:
                    item_ids.add(str(item_id))

            page = next_page

    if not item_ids:
        logger.info('{} No pricing change item IDs returned for {} to {}. Nothing to sync.'.format(
            _LOG_PREFIX, start_date, end_date
        ))
        return

    logger.info('{} Found {} unique item IDs from pricing changes.'.format(_LOG_PREFIX, len(item_ids)))

    turn14_brands = list(
        src_models.Turn14Brand.objects.filter(
            items__external_id__in=item_ids
        ).distinct()
    )

    if not turn14_brands:
        logger.warning('{} No Turn14Brand found in DB for the pricing change item IDs. Skipping sync.'.format(
            _LOG_PREFIX
        ))
        return

    logger.info('{} Syncing brand pricing for {} Turn14 brand(s) with pricing changes.'.format(
        _LOG_PREFIX, len(turn14_brands)
    ))
    fetch_and_save_turn_14_brand_pricing_for_turn14_brands(turn14_brands)
    logger.info('{} Completed pricing sync for brands with pricing changes.'.format(_LOG_PREFIX))


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
                    'type', 'inventory', 'manufacturer', 'eta', 'relationships', 'total_inventory', 'updated_at'
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
                updated_at=timezone.now(),  # Explicitly set updated_at for bulk operations
            )

            inventory_instances.append(inventory_instance)

        except Exception as e:
            logger.warning('{} Error transforming inventory update data {}: {}. Skipping.'.format(
                _LOG_PREFIX, inventory_item, str(e)
            ))
            continue

    return inventory_instances
