# PaceBrand -> Brands mapping, resolved by sync_pace_brands_to_brands (exact AAIA code,
# compact key, fuzzy word-prefix -- same phases as the TSE/AllPro/Keystone/DLG syncs).
# Generated then stripped of unrelated drift operations.
import django.core.serializers.json
from django.db import migrations, models
import django.db.models.deletion

class Migration(migrations.Migration):
    dependencies = [('src', '0226_pace_consolidated')]
    operations = [migrations.CreateModel(name='BrandPaceBrandMapping', fields=[('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')), ('match_phase', models.CharField(blank=True, default='', max_length=16)), ('created_at', models.DateTimeField(auto_now_add=True)), ('updated_at', models.DateTimeField(auto_now=True))], options={'db_table': 'brand_pace_brand_mapping'}), migrations.AddField(model_name='brandpacebrandmapping', name='brand', field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='pace_brand_mappings', to='src.brands')), migrations.AddField(model_name='brandpacebrandmapping', name='pace_brand', field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='brand_mappings', to='src.pacebrand')), migrations.AddIndex(model_name='brandpacebrandmapping', index=models.Index(fields=['match_phase'], name='bpbm_phase_idx')), migrations.AlterUniqueTogether(name='brandpacebrandmapping', unique_together={('brand', 'pace_brand')})]
