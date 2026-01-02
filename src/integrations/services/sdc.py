import logging
import typing
from decimal import Decimal
import pandas as pd
import io
from django.utils import timezone

import pgbulk

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.sdc import client as sdc_client
from src.integrations.clients.sdc import exceptions as sdc_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = '[SDC-SERVICES]'


def fetch_and_save_all_sdc_brand_items() -> None:
    logger.info('{} Fetching all SDC brand items.'.format(_LOG_PREFIX))

    sdc_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.SDC.value
    ).first()
    if not sdc_provider:
        logger.info('{} No SDC provider found.'.format(_LOG_PREFIX))
        return

    all_brands = src_models.BrandProviders.objects.filter(
        provider=sdc_provider,
    )
    if not all_brands.exists():
        logger.info('{} No brands found for SDC provider.'.format(_LOG_PREFIX))
        return

    for brand_provider in all_brands:
        brand = brand_provider.brand

        if brand.status_name != src_enums.BrandProviderStatus.ACTIVE.name:
            logger.info('{} Brand {} status is not active'.format(_LOG_PREFIX, brand.name))
            continue
        
        brand_mapping = src_models.BrandSDCBrandMapping.objects.filter(
            brand=brand
        ).first()
        
        if not brand_mapping:
            logger.warning('{} No SDCBrand mapping found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, brand.name
            ))
            continue
        
        sdc_brand = brand_mapping.sdc_brand
        
        company_brand = src_models.CompanyBrands.objects.filter(
            brand=brand
        ).first()
        
        if not company_brand:
            logger.warning('{} No company found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, sdc_brand.name
            ))
            continue
        
        company = company_brand.company
        
        company_provider = src_models.CompanyProviders.objects.filter(
            company=company,
            provider=sdc_provider
        ).first()
        
        if not company_provider:
            logger.warning('{} No company provider found for company: {} and brand: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, sdc_brand.name
            ))
            continue
        
        credentials = company_provider.credentials
        
        try:
            sftp_client = sdc_client.SDCSFTPClient(credentials=credentials)
        except ValueError as e:
            logger.error('{} Invalid credentials for company: {} and brand: {}. Error: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, sdc_brand.name, str(e)
            ))
            continue
        
        brand_id = str(sdc_brand.external_id)
        
        logger.info('{} Fetching product file for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, sdc_brand.name, brand_id
        ))
        
        try:
            file_result = sftp_client.get_latest_product_file(brand_id=brand_id)
            if not file_result:
                logger.warning('{} No product file found for brand: {} (external_id: {}). Skipping.'.format(
                    _LOG_PREFIX, sdc_brand.name, brand_id
                ))
                continue
            
            filename, file_content = file_result
            logger.info('{} Found product file: {} for brand: {} (external_id: {}).'.format(
                _LOG_PREFIX, filename, sdc_brand.name, brand_id
            ))
        except sdc_exceptions.SDCException as e:
            logger.error('{} SDC SFTP error for brand: {} (external_id: {}). Error: {}. Skipping brand.'.format(
                _LOG_PREFIX, sdc_brand.name, brand_id, str(e)
            ))
            continue
        
        try:
            # Parse pipe-delimited text file
            text_file = io.StringIO(file_content.decode('utf-8'))
            
            # Read pipe-delimited file
            df = pd.read_csv(text_file, sep='|', header=0, dtype=str, keep_default_na=False)
            
            # Clean up the DataFrame - remove any rows where all values are empty
            df = df.dropna(how='all')
            
            logger.info('{} Parsed pipe-delimited file with {} rows and {} columns for brand: {} (external_id: {}).'.format(
                _LOG_PREFIX, len(df), len(df.columns), sdc_brand.name, brand_id
            ))
            
            # Convert DataFrame to list of dictionaries
            items_data = df.to_dict('records')
            
            part_instances = _transform_product_data(items_data, sdc_brand)
            
            if not part_instances:
                logger.warning('{} No valid part instances created for brand: {} (external_id: {}).'.format(
                    _LOG_PREFIX, sdc_brand.name, brand_id
                ))
                continue
            
            try:
                upserted_parts = pgbulk.upsert(
                    src_models.SDCParts,
                    part_instances,
                    unique_fields=['part_number', 'brand'],
                    update_fields=[
                        'brand_label', 'gtin', 'category_pcdb', 'life_cycle_status', 'country_of_origin',
                        'warranty', 'long_description', 'extended_description', 'application_summary',
                        'features_and_benefits', 'marketing_description', 'title', 'keywords',
                        'product_attributes', 'jobber_usd', 'retail_usd', 'map_usd', 'unilateral_usd',
                        'primary_image', 'additional_image', 'installation_instructions', 'logo',
                        'video_random', 'video_installation', 'length_for_case', 'width_for_case',
                'height_for_case', 'weight_for_case', 'inventory', 'external_brand_id',
                'part_terminology_label', 'quantity_per_application', 'hazardous_material', 'condition',
                        'updated_at'
                    ],
                    returning=True,
                )
                
                logger.info('{} Successfully upserted {} parts for brand: {} (external_id: {}).'.format(
                    _LOG_PREFIX, len(upserted_parts) if upserted_parts else 0, sdc_brand.name, brand_id
                ))
            except Exception as e:
                logger.error('{} Error during bulk upsert for brand: {} (external_id: {}). Error: {}.'.format(
                    _LOG_PREFIX, sdc_brand.name, brand_id, str(e)
                ))
                continue
                
        except Exception as e:
            logger.error('{} Error parsing pipe-delimited file for brand: {} (external_id: {}). Error: {}.'.format(
                _LOG_PREFIX, sdc_brand.name, brand_id, str(e)
            ))
            continue
        
        logger.info('{} Completed fetching items for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, sdc_brand.name, brand_id
        ))


def _transform_product_data(items_data: typing.List[typing.Dict], sdc_brand: src_models.SDCBrands) -> typing.List[src_models.SDCParts]:
    part_instances = []
    
    # Helper to get value from dict with case-insensitive and whitespace-tolerant matching
    def get_value(data: dict, key: str):
        # Try exact match first
        if key in data:
            return data[key]
        # Try with stripped keys
        for k, v in data.items():
            if k.strip().lower() == key.strip().lower():
                return v
        return None
    
    for item_data in items_data:
        try:
            # Column names are case-sensitive and may have spaces
            part_number = str(get_value(item_data, 'Part Number') or '').strip()
            
            if not part_number:
                logger.warning('{} Skipping item with missing Part Number: {}'.format(
                    _LOG_PREFIX, item_data
                ))
                continue
            
            # Helper function to safely get and convert decimal values
            def get_decimal(value):
                if pd.isna(value) or value == '' or value is None:
                    return None
                try:
                    return Decimal(str(value))
                except (ValueError, TypeError):
                    return None
            
            # Helper function to safely get integer values
            def get_int(value):
                if pd.isna(value) or value == '' or value is None:
                    return None
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return None
            
            # Helper function to safely get string values
            def get_str(value):
                if pd.isna(value) or value is None:
                    return None
                return str(value).strip() if str(value).strip() else None
            
            part_instance = src_models.SDCParts(
                part_number=part_number,
                brand=sdc_brand,
                brand_label=get_str(get_value(item_data, 'Brand Label')),
                gtin=get_str(get_value(item_data, 'GTIN')),
                category_pcdb=get_str(get_value(item_data, 'Category (PCDB)')),
                life_cycle_status=get_str(get_value(item_data, 'Life Cycle Status')),
                country_of_origin=get_str(get_value(item_data, 'Country of Origin')),
                warranty=get_str(get_value(item_data, 'Warranty')),
                long_description=get_str(get_value(item_data, 'Long Description')),
                extended_description=get_str(get_value(item_data, 'Extended Description')),
                application_summary=get_str(get_value(item_data, 'Application Summary')),
                features_and_benefits=get_str(get_value(item_data, 'Features and Benefits')),
                marketing_description=get_str(get_value(item_data, 'Marketing Description')),
                title=get_str(get_value(item_data, 'Title')),
                keywords=get_str(get_value(item_data, 'Keywords')),
                product_attributes=get_str(get_value(item_data, 'Product Attributes (In One Field)')),
                jobber_usd=get_decimal(get_value(item_data, 'Jobber (USD)')),
                retail_usd=get_decimal(get_value(item_data, 'Retail (USD)')),
                map_usd=get_decimal(get_value(item_data, 'MAP (USD)')),
                unilateral_usd=get_decimal(get_value(item_data, 'Unilateral (USD)')),
                primary_image=get_str(get_value(item_data, 'Primary')),
                additional_image=get_str(get_value(item_data, 'Additional Image')),
                installation_instructions=get_str(get_value(item_data, 'Installation Instructions')),
                logo=get_str(get_value(item_data, 'Logo')),
                video_random=get_str(get_value(item_data, 'Video - Random')),
                video_installation=get_str(get_value(item_data, 'Video - Installation')),
                length_for_case=get_decimal(get_value(item_data, 'Length (For Case)')),
                width_for_case=get_decimal(get_value(item_data, 'Width (For Case)')),
                height_for_case=get_decimal(get_value(item_data, 'Height (For Case)')),
                weight_for_case=get_decimal(get_value(item_data, 'Weight (For Case)')),
                inventory=get_int(get_value(item_data, 'Inventory')),  # Try both with and without trailing space
                external_brand_id=get_str(get_value(item_data, 'Brand ID')),
                part_terminology_label=get_str(get_value(item_data, 'Part Terminology Label')),
                quantity_per_application=get_str(get_value(item_data, 'Quantity per Application')),
                hazardous_material=get_str(get_value(item_data, 'Hazardous Material')),
                condition=get_str(get_value(item_data, 'Condition')),
                updated_at=timezone.now(),  # Explicitly set updated_at for bulk operations
            )
            
            part_instances.append(part_instance)
            
        except Exception as e:
            logger.warning('{} Error transforming product data {}: {}. Skipping.'.format(
                _LOG_PREFIX, item_data, str(e)
            ))
            continue
    
    return part_instances


def fetch_and_save_all_sdc_brand_fitments() -> None:
    logger.info('{} Fetching all SDC brand fitments.'.format(_LOG_PREFIX))

    sdc_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.SDC.value
    ).first()
    if not sdc_provider:
        logger.info('{} No SDC provider found.'.format(_LOG_PREFIX))
        return

    all_brands = src_models.BrandProviders.objects.filter(
        provider=sdc_provider,
    )
    if not all_brands.exists():
        logger.info('{} No brands found for SDC provider.'.format(_LOG_PREFIX))
        return

    for brand_provider in all_brands:
        brand = brand_provider.brand

        if brand.status_name != src_enums.BrandProviderStatus.ACTIVE.name:
            logger.info('{} Brand {} status is not active'.format(_LOG_PREFIX, brand.name))
            continue
        
        brand_mapping = src_models.BrandSDCBrandMapping.objects.filter(
            brand=brand
        ).first()
        
        if not brand_mapping:
            logger.warning('{} No SDCBrand mapping found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, brand.name
            ))
            continue
        
        sdc_brand = brand_mapping.sdc_brand
        
        company_brand = src_models.CompanyBrands.objects.filter(
            brand=brand
        ).first()
        
        if not company_brand:
            logger.warning('{} No company found for brand: {}. Skipping.'.format(
                _LOG_PREFIX, sdc_brand.name
            ))
            continue
        
        company = company_brand.company
        
        company_provider = src_models.CompanyProviders.objects.filter(
            company=company,
            provider=sdc_provider
        ).first()
        
        if not company_provider:
            logger.warning('{} No company provider found for company: {} and brand: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, sdc_brand.name
            ))
            continue
        
        credentials = company_provider.credentials
        
        try:
            sftp_client = sdc_client.SDCSFTPClient(credentials=credentials)
        except ValueError as e:
            logger.error('{} Invalid credentials for company: {} and brand: {}. Error: {}. Skipping.'.format(
                _LOG_PREFIX, company.name, sdc_brand.name, str(e)
            ))
            continue
        
        brand_id = str(sdc_brand.external_id)
        
        logger.info('{} Fetching fitment file for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, sdc_brand.name, brand_id
        ))
        
        try:
            file_result = sftp_client.get_latest_fitment_file(brand_id=brand_id)
            if not file_result:
                logger.warning('{} No fitment file found for brand: {} (external_id: {}). Skipping.'.format(
                    _LOG_PREFIX, sdc_brand.name, brand_id
                ))
                continue
            
            filename, file_content = file_result
            logger.info('{} Found fitment file: {} for brand: {} (external_id: {}).'.format(
                _LOG_PREFIX, filename, sdc_brand.name, brand_id
            ))
        except sdc_exceptions.SDCException as e:
            logger.error('{} SDC SFTP error for brand: {} (external_id: {}). Error: {}. Skipping brand.'.format(
                _LOG_PREFIX, sdc_brand.name, brand_id, str(e)
            ))
            continue
        
        try:
            # Parse pipe-delimited text file
            text_file = io.StringIO(file_content.decode('utf-8'))
            
            # Read pipe-delimited file
            df = pd.read_csv(text_file, sep='|', header=0, dtype=str, keep_default_na=False)
            
            # Clean up the DataFrame - remove any rows where all values are empty
            df = df.dropna(how='all')
            
            logger.info('{} Parsed pipe-delimited file with {} rows and {} columns for brand: {} (external_id: {}).'.format(
                _LOG_PREFIX, len(df), len(df.columns), sdc_brand.name, brand_id
            ))
            
            # Convert DataFrame to list of dictionaries
            items_data = df.to_dict('records')
            
            fitment_instances = _transform_fitment_data(items_data, sdc_brand)
            
            if not fitment_instances:
                logger.warning('{} No valid fitment instances created for brand: {} (external_id: {}).'.format(
                    _LOG_PREFIX, sdc_brand.name, brand_id
                ))
                continue
            
            # Deduplicate fitment_instances before upsert
            # Keep only the first occurrence of each unique combination
            unique_combinations = {}
            deduplicated_instances = []
            brand_id = None
            
            for fitment in fitment_instances:
                # Use brand's id (which will be the same for all instances in this batch)
                if brand_id is None:
                    brand_id = fitment.brand.id if fitment.brand else None
                
                key = (fitment.sku, brand_id, fitment.year, fitment.make, fitment.model)
                if key not in unique_combinations:
                    unique_combinations[key] = True
                    deduplicated_instances.append(fitment)
            
            duplicate_count = len(fitment_instances) - len(deduplicated_instances)
            if duplicate_count > 0:
                logger.info('{} Found {} duplicate fitment rows (same sku/brand/year/make/model) out of {} total instances for brand: {} (external_id: {}). Deduplicated to {} unique instances.'.format(
                    _LOG_PREFIX, duplicate_count, len(fitment_instances), sdc_brand.name, brand_id, len(deduplicated_instances)
                ))
            
            try:
                upserted_fitments = pgbulk.upsert(
                    src_models.SDCPartFitment,
                    deduplicated_instances,
                    unique_fields=['sku', 'brand', 'year', 'make', 'model'],
                    update_fields=['category_pcdb', 'subcategory_pcdb', 'updated_at'],
                    returning=True,
                )
                
                logger.info('{} Successfully upserted {} fitments (from {} deduplicated instances, {} original) for brand: {} (external_id: {}).'.format(
                    _LOG_PREFIX, len(upserted_fitments) if upserted_fitments else 0, len(deduplicated_instances), len(fitment_instances), sdc_brand.name, brand_id
                ))
            except Exception as e:
                logger.error('{} Error during bulk upsert for brand: {} (external_id: {}). Error: {}.'.format(
                    _LOG_PREFIX, sdc_brand.name, brand_id, str(e)
                ))
                continue
                
        except Exception as e:
            logger.error('{} Error parsing pipe-delimited file for brand: {} (external_id: {}). Error: {}.'.format(
                _LOG_PREFIX, sdc_brand.name, brand_id, str(e)
            ))
            continue
        
        logger.info('{} Completed fetching fitments for brand: {} (external_id: {}).'.format(
            _LOG_PREFIX, sdc_brand.name, brand_id
        ))


def _transform_fitment_data(items_data: typing.List[typing.Dict], sdc_brand: src_models.SDCBrands) -> typing.List[src_models.SDCPartFitment]:
    fitment_instances = []
    
    # Counters for tracking skipped rows
    skipped_no_part_number = 0
    skipped_no_year = 0
    skipped_no_make = 0
    skipped_no_model = 0
    skipped_exception = 0
    
    # Helper to get value from dict with case-insensitive and whitespace-tolerant matching
    def get_value(data: dict, key: str):
        # Try exact match first
        if key in data:
            return data[key]
        # Try with stripped keys
        for k, v in data.items():
            if k.strip().lower() == key.strip().lower():
                return v
        return None
    
    for item_data in items_data:
        try:
            part_number = str(get_value(item_data, 'Part Number') or '').strip()
            
            if not part_number:
                skipped_no_part_number += 1
                if skipped_no_part_number <= 5:  # Log first 5 examples
                    logger.warning('{} Skipping fitment with missing Part Number: {}'.format(
                        _LOG_PREFIX, item_data
                    ))
                continue
            
            # Helper function to safely get integer values
            def get_int(value):
                if pd.isna(value) or value == '' or value is None:
                    return None
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return None
            
            # Helper function to safely get string values
            def get_str(value):
                if pd.isna(value) or value is None:
                    return None
                return str(value).strip() if str(value).strip() else None
            
            year = get_int(get_value(item_data, 'Year'))
            if year is None:
                skipped_no_year += 1
                if skipped_no_year <= 5:  # Log first 5 examples
                    logger.warning('{} Skipping fitment with missing Year for Part Number: {}'.format(
                        _LOG_PREFIX, part_number
                    ))
                continue
            
            make = get_str(get_value(item_data, 'Make'))
            if not make:
                skipped_no_make += 1
                if skipped_no_make <= 5:  # Log first 5 examples
                    logger.warning('{} Skipping fitment with missing Make for Part Number: {}'.format(
                        _LOG_PREFIX, part_number
                    ))
                continue
            
            model = get_str(get_value(item_data, 'Model'))
            if not model:
                skipped_no_model += 1
                if skipped_no_model <= 5:  # Log first 5 examples
                    logger.warning('{} Skipping fitment with missing Model for Part Number: {}'.format(
                        _LOG_PREFIX, part_number
                    ))
                continue
            
            # Get category_pcdb from "Category (PCDB)" column
            category_pcdb = get_str(get_value(item_data, 'Category (PCDB)'))
            
            # Get subcategory_pcdb from "Part Terminology Label" column
            subcategory_pcdb = get_str(get_value(item_data, 'Part Terminology Label'))
            
            fitment_instance = src_models.SDCPartFitment(
                sku=part_number,
                brand=sdc_brand,
                year=year,
                make=make,
                model=model,
                category_pcdb=category_pcdb,
                subcategory_pcdb=subcategory_pcdb,
                updated_at=timezone.now(),  # Explicitly set updated_at for bulk operations
            )
            
            fitment_instances.append(fitment_instance)
            
        except Exception as e:
            skipped_exception += 1
            if skipped_exception <= 5:  # Log first 5 examples
                logger.warning('{} Error transforming fitment data {}: {}. Skipping.'.format(
                    _LOG_PREFIX, item_data, str(e)
                ))
            continue
    
    # Log summary of skipped rows
    total_skipped = skipped_no_part_number + skipped_no_year + skipped_no_make + skipped_no_model + skipped_exception
    if total_skipped > 0:
        logger.info('{} Fitment transformation summary: Total rows: {}, Valid: {}, Skipped: {} (No Part Number: {}, No Year: {}, No Make: {}, No Model: {}, Exceptions: {})'.format(
            _LOG_PREFIX, len(items_data), len(fitment_instances), total_skipped,
            skipped_no_part_number, skipped_no_year, skipped_no_make, skipped_no_model, skipped_exception
        ))
    
    return fitment_instances