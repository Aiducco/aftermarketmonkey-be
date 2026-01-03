"""
Temporary one-time script to delete BigCommerce products.
Gets product IDs from company_destination_parts.destination_external_id
and deletes them from BigCommerce.

WARNING: This is a destructive operation.
"""
from django.core.management.base import BaseCommand

from src import enums as src_enums
from src import models as src_models
from src.integrations.ecommerce.bigcommerce.gateways import client as bigcommerce_client
from src.integrations.ecommerce.bigcommerce.gateways import exceptions as bigcommerce_exceptions

import logging

logger = logging.getLogger(__name__)

# Batch size for deleting products (BigCommerce API limit is typically 50 per request)
BATCH_SIZE = 250


class Command(BaseCommand):
    help = 'Temporary one-time script to delete BigCommerce products from company_destination_parts'

    def handle(self, *args, **options):
        logger.info('Starting BigCommerce products deletion script')
        self.stdout.write('=' * 80)
        self.stdout.write('BigCommerce Products Deletion Script')
        self.stdout.write('=' * 80)
        self.stdout.write('')

        # Get BigCommerce destinations
        bigcommerce_destinations = src_models.CompanyDestinations.objects.filter(
            destination_type=src_enums.IntegrationDestinationType.BIGCOMMERCE.value
        )
        
        if not bigcommerce_destinations.exists():
            logger.warning('No BigCommerce destinations found')
            self.stdout.write(self.style.ERROR('No BigCommerce destinations found.'))
            return
        
        destination_count = bigcommerce_destinations.count()
        logger.info(f'Found {destination_count} BigCommerce destination(s)')
        self.stdout.write(f'Found {destination_count} BigCommerce destination(s)')
        self.stdout.write('')

        total_deleted = 0
        total_failed = 0

        for destination in bigcommerce_destinations:
            logger.info(f'Processing destination: {destination.id} (Company: {destination.company.name})')
            self.stdout.write(f'Processing destination: {destination.id} (Company: {destination.company.name})')
            
            # Get all destination_external_id values for this destination
            company_destination_parts = src_models.CompanyDestinationParts.objects.all().order_by('-id')[:500]
            
            product_ids = []
            invalid_ids = []
            # Map product_id to CompanyDestinationParts objects for database deletion
            product_id_to_parts = {}
            for part in company_destination_parts:
                try:
                    # destination_external_id is stored as string, convert to int
                    product_id = int(part.destination_external_id)
                    product_ids.append(product_id)
                    # Store mapping for database deletion
                    if product_id not in product_id_to_parts:
                        product_id_to_parts[product_id] = []
                    product_id_to_parts[product_id].append(part)
                except (ValueError, TypeError):
                    invalid_id = part.destination_external_id
                    invalid_ids.append(invalid_id)
                    logger.warning(f'  Skipping invalid destination_external_id: {invalid_id} (destination_id: {destination.id})')
                    self.stdout.write(
                        self.style.WARNING(
                            f'  Skipping invalid destination_external_id: {invalid_id}'
                        )
                    )
            
            if invalid_ids:
                logger.warning(f'  Found {len(invalid_ids)} invalid destination_external_id values for destination {destination.id}')
            
            if not product_ids:
                logger.info(f'  No products found for destination {destination.id}')
                self.stdout.write(f'  No products found for destination {destination.id}')
                self.stdout.write('')
                continue
            
            logger.info(f'  Found {len(product_ids)} products to delete for destination {destination.id}')
            self.stdout.write(f'  Found {len(product_ids)} products to delete')
            
            # Delete products
            try:
                credentials = destination.credentials
                api_client = bigcommerce_client.BigCommerceApiClient(credentials=credentials)
                logger.info(f'  Initialized API client for destination {destination.id}')
            except ValueError as e:
                error_msg = f'Invalid credentials for destination {destination.id}: {str(e)}'
                logger.error(error_msg)
                self.stdout.write(self.style.ERROR(f'  {error_msg}'))
                total_failed += len(product_ids)
                self.stdout.write('')
                continue
            
            # Delete in batches
            deleted_count = 0
            failed_count = 0
            db_deleted_count = 0
            db_history_deleted_count = 0
            
            for i in range(0, len(product_ids), BATCH_SIZE):
                batch = product_ids[i:i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                total_batches = (len(product_ids) + BATCH_SIZE - 1) // BATCH_SIZE
                
                try:
                    logger.info(f'  Deleting batch {batch_num}/{total_batches} ({len(batch)} products) for destination {destination.id}')
                    self.stdout.write(
                        f'  Deleting batch {batch_num}/{total_batches} ({len(batch)} products)...',
                        ending=' '
                    )
                    
                    api_client.delete_products(batch)
                    deleted_count += len(batch)
                    total_deleted += len(batch)
                    
                    logger.info(f'  Successfully deleted batch {batch_num}/{total_batches} ({len(batch)} products) from BigCommerce for destination {destination.id}')
                    
                    # Delete from database after successful API deletion
                    parts_to_delete = []
                    for product_id in batch:
                        if product_id in product_id_to_parts:
                            parts_to_delete.extend(product_id_to_parts[product_id])
                    
                    if parts_to_delete:
                        # Get all part IDs for history deletion
                        part_ids = [part.id for part in parts_to_delete]
                        
                        # Delete history records first (explicitly, though CASCADE would handle it)
                        history_deleted = src_models.CompanyDestinationPartsHistory.objects.filter(
                            destination_part_id__in=part_ids
                        ).delete()
                        db_history_deleted_count += history_deleted[0]
                        logger.info(f'  Deleted {history_deleted[0]} history records from database')
                        
                        # Delete CompanyDestinationParts records
                        parts_deleted = src_models.CompanyDestinationParts.objects.filter(
                            id__in=part_ids
                        ).delete()
                        db_deleted_count += parts_deleted[0]
                        logger.info(f'  Deleted {parts_deleted[0]} CompanyDestinationParts records from database')
                    
                    self.stdout.write(self.style.SUCCESS('✓'))
                    
                except bigcommerce_exceptions.BigCommerceAPIException as e:
                    failed_count += len(batch)
                    total_failed += len(batch)
                    error_msg = f'Error deleting products batch {batch_num} for destination {destination.id}: {str(e)}'
                    logger.error(error_msg)
                    logger.exception(f'Error deleting products batch {batch_num} for destination {destination.id}')
                    self.stdout.write(self.style.ERROR(f'✗ Error: {str(e)}'))
                except Exception as e:
                    failed_count += len(batch)
                    total_failed += len(batch)
                    error_msg = f'Unexpected error deleting products batch {batch_num} for destination {destination.id}: {str(e)}'
                    logger.error(error_msg)
                    logger.exception(f'Unexpected error deleting products batch {batch_num} for destination {destination.id}')
                    self.stdout.write(self.style.ERROR(f'✗ Unexpected error: {str(e)}'))
            
            logger.info(f'  Destination {destination.id}: {deleted_count} deleted from BigCommerce, {failed_count} failed')
            logger.info(f'  Destination {destination.id}: {db_deleted_count} CompanyDestinationParts records deleted from database')
            logger.info(f'  Destination {destination.id}: {db_history_deleted_count} CompanyDestinationPartsHistory records deleted from database')
            self.stdout.write(f'  Destination {destination.id}: {deleted_count} deleted from BigCommerce, {failed_count} failed')
            self.stdout.write(f'  Database: {db_deleted_count} CompanyDestinationParts, {db_history_deleted_count} History records deleted')
            self.stdout.write('')

        # Summary
        logger.info(f'Deletion script completed. Total deleted: {total_deleted}, Total failed: {total_failed}')
        self.stdout.write('=' * 80)
        self.stdout.write('Summary:')
        self.stdout.write(f'  Deleted: {total_deleted} products')
        self.stdout.write(f'  Failed: {total_failed} products')
        self.stdout.write('=' * 80)

