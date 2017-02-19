from django.db import models


class SimpleModel(models.Model):
    char_field = models.CharField(max_length=100)

    class Meta:
        app_label = 'test_dhp'


class ComplexModel(models.Model):
    char_field = models.CharField(max_length=100)
    text_field = models.TextField()

    class Meta:
        app_label = 'test_dhp'
