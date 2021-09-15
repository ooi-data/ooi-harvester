def test_harvest_settings():
    from ooi_harvester.settings import harvest_settings
    from ooi_harvester.settings.main import HarvestSettings

    assert isinstance(harvest_settings, HarvestSettings)
    assert harvest_settings.storage_options.aws.key == 'minioadmin'
    assert harvest_settings.storage_options.aws.secret == 'minioadmin'
    assert harvest_settings.ooi_config.username == 'username'
    assert harvest_settings.ooi_config.token == 'token'
