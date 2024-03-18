### Collector for Global P-codes dataset
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-pcodes/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-pcodes/actions/workflows/run-python-tests.yaml) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-pcodes/badge.svg?branch=main)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-pcodes?branch=main)

This script collects the latest P-codes, administrative names, and dates from the CODs data on HDX and updates a new HDX dataset.

### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-pcodes** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: HDX_SITE, HDX_KEY, USER_AGENT, PREPREFIX
