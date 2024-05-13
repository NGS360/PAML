
sb_credentials = {
    'US': {
        'api_endpoint': 'https://bms-api.sbgenomics.com/v2',
        'token': 'dummy',
        'profile': 'default'
    },
    'CN': {
        'api_endpoint': 'https://api.sevenbridges.cn/v2',
        'token': 'dummy',
        'profile': 'china'
    }
}

sb_execution_settings = {
    'US': {
        'use_elastic_disk': True,
        'use_memoization': True,
        'use_spot_instance': True,
        "max_parallel_instances": 1
    },
    'CN': {
        'use_elastic_disk': False,
        'use_memoization': True,
        'use_spot_instance': True,
        "max_parallel_instances": 1
    }
}
