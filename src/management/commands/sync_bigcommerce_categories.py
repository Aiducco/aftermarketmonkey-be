"""
One-time script to sync BigCommerce categories.
Fetches all categories from BigCommerce API and syncs them with the database:
- Creates categories that exist in API but not in DB
- Deletes categories that exist in DB but not in API
"""
from django.core.management.base import BaseCommand

from src import enums as src_enums
from src import models as src_models
from src.integrations.ecommerce.bigcommerce.gateways import client as bigcommerce_client
from src.integrations.ecommerce.bigcommerce.gateways import exceptions as bigcommerce_exceptions

import logging

logger = logging.getLogger(__name__)

_LOG_PREFIX = '[SYNC-BIGCOMMERCE-CATEGORIES]'


class Command(BaseCommand):
    help = 'One-time script to sync BigCommerce categories from API to database'

    def handle(self, *args, **options):
        logger.info('{} Starting BigCommerce categories sync script'.format(_LOG_PREFIX))
        self.stdout.write('=' * 80)
        self.stdout.write('BigCommerce Categories Sync Script')
        self.stdout.write('=' * 80)
        self.stdout.write('')

        # Get all BigCommerce destinations
        bigcommerce_destinations = src_models.CompanyDestinations.objects.filter(
            destination_type=src_enums.IntegrationDestinationType.BIGCOMMERCE.value
        )

        if not bigcommerce_destinations.exists():
            logger.warning('{} No BigCommerce destinations found'.format(_LOG_PREFIX))
            self.stdout.write(self.style.ERROR('No BigCommerce destinations found.'))
            return

        destination_count = bigcommerce_destinations.count()
        logger.info('{} Found {} BigCommerce destination(s)'.format(_LOG_PREFIX, destination_count))
        self.stdout.write('Found {} BigCommerce destination(s)'.format(destination_count))
        self.stdout.write('')

        total_created = 0
        total_deleted = 0
        total_errors = 0

        for destination in bigcommerce_destinations:
            company = destination.company
            self.stdout.write('Processing destination: {} (company: {})'.format(
                destination.id, company.name
            ))
            logger.info('{} Processing destination: {} (company: {})'.format(
                _LOG_PREFIX, destination.id, company.name
            ))

            try:
                api_client = bigcommerce_client.BigCommerceApiClient(credentials=destination.credentials)
            except ValueError as e:
                error_msg = 'Invalid credentials for destination: {} (company: {}). Error: {}. Skipping.'.format(
                    destination.id, company.name, str(e)
                )
                logger.error('{} {}'.format(_LOG_PREFIX, error_msg))
                self.stdout.write(self.style.ERROR('  {}'.format(error_msg)))
                total_errors += 1
                continue

            # Fetch all categories from API
            api_categories = []
            page = 1
            while page is not None:
                try:
                    categories_data, next_page = api_client.get_categories(page=page)
                    if categories_data:
                        api_categories.extend(categories_data)
                    page = next_page
                except bigcommerce_exceptions.BigCommerceAPIException as e:
                    error_msg = 'BigCommerce API error for destination: {} (company: {}), page: {}. Error: {}.'.format(
                        destination.id, company.name, page, str(e)
                    )
                    logger.error('{} {}'.format(_LOG_PREFIX, error_msg))
                    self.stdout.write(self.style.ERROR('  {}'.format(error_msg)))
                    total_errors += 1
                    break

            logger.info('{} Fetched {} categories from API for destination: {} (company: {})'.format(
                _LOG_PREFIX, len(api_categories), destination.id, company.name
            ))
            self.stdout.write('  Fetched {} categories from API'.format(len(api_categories)))

            # Build sets of external_ids from API
            api_external_ids = set()
            api_categories_by_id = {}
            for category in api_categories:
                category_id = category.get('category_id')
                if category_id is not None:
                    api_external_ids.add(category_id)
                    api_categories_by_id[category_id] = category

            # Get all categories from DB for this destination
            db_categories = src_models.BigCommerceCategories.objects.filter(
                company_destination=destination
            )

            # Build sets of external_ids from DB
            db_external_ids = set(db_categories.values_list('external_id', flat=True))

            # Find categories to create (in API but not in DB)
            categories_to_create = api_external_ids - db_external_ids
            created_count = 0
            for category_id in categories_to_create:
                category_data = api_categories_by_id.get(category_id)
                if not category_data:
                    continue

                try:
                    # Extract category fields
                    external_id = category_data.get('category_id')
                    name = category_data.get('name', '').strip()
                    parent_id = category_data.get('parent_id', 0)
                    tree_id = category_data.get('tree_id', 1)

                    if not external_id or not name:
                        logger.warning('{} Skipping category with missing external_id or name: {}'.format(
                            _LOG_PREFIX, category_data
                        ))
                        continue

                    # Check if category already exists by name and parent_id (in case external_id changed)
                    existing_by_name = src_models.BigCommerceCategories.objects.filter(
                        name=name,
                        parent_id=parent_id,
                        company_destination=destination,
                        tree_id=tree_id
                    ).first()

                    if existing_by_name:
                        # Update external_id if it changed
                        if existing_by_name.external_id != external_id:
                            logger.info('{} Updating external_id for category: {} (old: {}, new: {})'.format(
                                _LOG_PREFIX, name, existing_by_name.external_id, external_id
                            ))
                            existing_by_name.external_id = external_id
                            existing_by_name.save()
                        continue

                    # Create new category
                    src_models.BigCommerceCategories.objects.create(
                        external_id=external_id,
                        name=name,
                        parent_id=parent_id,
                        tree_id=tree_id,
                        company_destination=destination,
                    )
                    created_count += 1
                    logger.debug('{} Created category: {} (id: {}, parent_id: {})'.format(
                        _LOG_PREFIX, name, external_id, parent_id
                    ))
                except Exception as e:
                    error_msg = 'Error creating category (id: {}): {}.'.format(category_id, str(e))
                    logger.error('{} {}'.format(_LOG_PREFIX, error_msg))
                    total_errors += 1

            total_created += created_count
            self.stdout.write('  Created {} new categories'.format(created_count))

            # Find categories to delete (in DB but not in API)
            categories_to_delete = db_external_ids - api_external_ids
            deleted_count = 0
            for category_id in categories_to_delete:
                try:
                    category = db_categories.get(external_id=category_id)
                    category_name = category.name
                    category.delete()
                    deleted_count += 1
                    logger.debug('{} Deleted category: {} (id: {})'.format(
                        _LOG_PREFIX, category_name, category_id
                    ))
                except src_models.BigCommerceCategories.DoesNotExist:
                    # Already deleted or doesn't exist
                    pass
                except Exception as e:
                    error_msg = 'Error deleting category (id: {}): {}.'.format(category_id, str(e))
                    logger.error('{} {}'.format(_LOG_PREFIX, error_msg))
                    total_errors += 1

            total_deleted += deleted_count
            self.stdout.write('  Deleted {} categories not found in API'.format(deleted_count))
            self.stdout.write('')

        # Summary
        self.stdout.write('=' * 80)
        self.stdout.write('Summary:')
        self.stdout.write('  Created: {} categories'.format(total_created))
        self.stdout.write('  Deleted: {} categories'.format(total_deleted))
        self.stdout.write('  Errors: {}'.format(total_errors))
        self.stdout.write('=' * 80)

        logger.info('{} Completed sync. Created: {}, Deleted: {}, Errors: {}'.format(
            _LOG_PREFIX, total_created, total_deleted, total_errors
        ))
        self.stdout.write(self.style.SUCCESS('Successfully completed BigCommerce categories sync.'))

