dictionary = [
    dict(name='iso_country', desc='two-character country code', example='US'),
    dict(name='state_code',  desc='two-character state code',   example='PA'),
    dict(name='iata_code',  desc='three-character airport code',   example='HHW'),
    dict(name='country_index',  desc='numeric index for country',   example='HHW'),

]

transform = [
    dict(origin='i94res', name='residence_country_index', desc='country of birth?', example='US'),
    dict(origin='i94cit', name='origin_country_index', desc='country of residence', example='US'),
    dict(origin='i94port', name='destination_iata_code', desc='country of residence', example='US'),

    dict(origin='arrdate', name='destination_iata_code', desc='country of residence', example='US'),
    dict(origin='i94port', name='destination_iata_code', desc='country of residence', example='US'),
    dict(origin='i94port', name='destination_iata_code', desc='country of residence', example='US'),
    dict(origin='i94port', name='destination_iata_code', desc='country of residence', example='US'),

]