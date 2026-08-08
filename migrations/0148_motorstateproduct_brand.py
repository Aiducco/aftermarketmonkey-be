# Add brand FK (-> MotorStateBrand) and brand_code to MotorStateProduct.

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0147_motorstate_raw_tables"),
    ]

    operations = [
        migrations.AddField(
            model_name="motorstateproduct",
            name="brand",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="products",
                to="src.motorstatebrand",
            ),
        ),
        migrations.AddField(
            model_name="motorstateproduct",
            name="brand_code",
            field=models.CharField(max_length=32, null=True),
        ),
    ]
