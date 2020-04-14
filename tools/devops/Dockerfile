FROM kpler/scrape
MAINTAINER Kpler <dev@kpler.com>

# fake scrapinghub environment, that will activate dotscrapy and feed export
ENV SHUB_JOBKEY "whatever"
# required for AWS and Datadog extensions. Arbitrary set so that we can
# recognize it but one can overwrite it at runtime
ENV SCRAPY_PROJECT_ID 444

ENV DOTSCRAPY_ENABLED "false"

# tweack Scrapy behavior
ENV SCRAPY_LOG_LEVEL DEBUG

# FIXME doesn't work with Python 3
# ENV HISTORY_S3_BUCKET "scraping-data-${SCRAPY_PROJECT_ID}"

# credentials are meant to be overwritten
# enable interaction with AWS S3 (cache response, history, items storage)
# AWS_ACCESS_KEY_ID="AKIAJ4ABWS7J3BRGYUEA"
# AWS_SECRET_ACCESS_KEY="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"
# enable monitoring
# DATADOG_API_KEY="xxxx"
# DATADOG_APP_KEY="xxxx"

# those variables are runtime-specific and meant to be overwritten as well
# SCRAPY_JOB="444/2/3"
# SCRAPY_SPIDER_ID="2"
# SCRAPY_SPIDER="Kochi"

# we always crawl and export json lines
ENTRYPOINT ["scrapy", "crawl", "-t", "jl"]
# TODO add usage for local run with local export
# once again meant to be overwritten of course with the actual scrapy arguments
# real world usage for production cloning:
#
#     docker run -it --name exactais --rm \
#       -e "SCRAPY_PROJECT_ID=local" \
#       -e "SCRAPY_JOB=local/2/3" \
#       -e "SCRAPY_SPIDER=Kochi" \
#       -e "SCRAPY_JOB_ID=2" \
#       -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
#       -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
#       Kochi
#
CMD ["--help"]
