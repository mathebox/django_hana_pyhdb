from django.contrib.gis.db import models
from django.contrib.gis.db.backends.base.models import SpatialRefSysMixin
from django.utils.encoding import python_2_unicode_compatible


@python_2_unicode_compatible
class HanaGeometryColumns(models.Model):
    "Maps to the HANA ST_GEOMETRY_COLUMNS view."
    schema_name = models.CharField(max_length=256, null=False)
    table_name = models.CharField(max_length=256, null=False)
    column_name = models.CharField(max_length=256, null=False)
    srs_id = models.IntegerField(null=False)
    srs_name = models.CharField(max_length=256)
    data_type_name = models.CharField(max_length=16)

    class Meta:
        app_label = 'gis'
        db_table = 'ST_GEOMETRY_COLUMNS'
        managed = False

    @classmethod
    def table_name_col(cls):
        """
        Returns the name of the metadata column used to store the feature table
        name.
        """
        return 'table_name'

    @classmethod
    def geom_col_name(cls):
        """
        Returns the name of the metadata column used to store the feature
        geometry column.
        """
        return 'column_name'

    def __str__(self):
        return '%s - %s (SRID: %s)' % (self.table_name, self.column_name, self.srid)


class HanaSpatialRefSys(models.Model, SpatialRefSysMixin):
    "Maps to the SAP HANA SYS.ST_SPATIAL_REFERENCE_SYSTEMS view."
    owner_name = models.CharField(max_length=256)
    srs_id = models.IntegerField(null=False)
    srs_name = models.CharField(max_length=256, null=False)
    round_earth = models.CharField(max_length=7, null=False)
    axis_order = models.CharField(max_length=12, null=False)
    snap_to_grid = models.FloatField()
    tolerance = models.FloatField()
    semi_major_axis = models.FloatField()
    semi_minor_axis = models.FloatField()
    inv_flattening = models.FloatField()
    min_x = models.FloatField()
    max_x = models.FloatField()
    min_y = models.FloatField()
    max_y = models.FloatField()
    min_z = models.FloatField()
    max_z = models.FloatField()
    organization = models.CharField(max_length=256)
    organization_coordsys_id = models.IntegerField(null=False)
    srs_type = models.CharField(max_length=11, null=False)
    linear_unit_of_measure = models.CharField(max_length=256, null=False)
    angular_unit_of_measure = models.CharField(max_length=256)
    polygon_format = models.CharField(max_length=16, null=False)
    storage_format = models.CharField(max_length=8, null=False)
    definition = models.CharField(max_length=5000)
    transform_definition = models.CharField(max_length=5000)
    objects = models.GeoManager()

    class Meta:
        app_label = 'gis'
        db_table = 'ST_SPATIAL_REFERENCE_SYSTEMS'
        managed = False

    @property
    def wkt(self):
        return self.definition

    @classmethod
    def wkt_col(cls):
        return 'definition'
