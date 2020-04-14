# commands in this dockerfile were generated and modified from `shub image init` output
# for more details, see https://bit.ly/30BYuJs

# base image built from https://github.com/Kpler/kp-containers/tree/master/kp-scrapers
FROM 447157256452.dkr.ecr.eu-west-1.amazonaws.com/kp-scrapers-pdf-prod:base-scrapy1.8-py3.6-20200324

# scrapinghub expects project to be installed to `app`
# see https://github.com/scrapinghub/scrapinghub-stack-scrapy/blob/branch-1.8-py3/Dockerfile
WORKDIR /app

# install python dependencies
COPY requirements.txt .
RUN pip install --progress-bar off --requirement requirements.txt

# init project vars expected by scrapinghub
ENV SCRAPY_SETTINGS_MODULE kp_scrapers.settings

# install scrapers project
COPY . .
RUN python setup.py install
