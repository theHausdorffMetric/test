#! /usr/bin/env bash

# unofficial strict mode
set -eo pipefail

#/ Usage: deploy.sh ENVIRONMENT [MESSAGE]
#/
#/   Deploy local project state to Scrapinghub. Includes systematic versioning
#/   and automated setup of envionment and requirements.
#/
#/   This script executes the following steps sequentially:
#/     1. build custom Docker image
#/     2. upload image to private AWS ECR repo
#/     3. deploy image to Scrapinghub (config defined in `scrapinghub.yml`)
#/
#/   Custom images are necessary because the default image provided by
#/   Scrapinghub does not support Java, which is required for many of our
#/   scrapers.
#/
#/   Note that this command should only be run in the project root for Docker
#/   to retrieve the correct build context.
#/
#/   For more info on the custom image contract established by Scrapinghub:
#/   https://shub.readthedocs.io/en/stable/deploy-custom-image.html#deployment
#/
#/ Examples:
#/   deploy.sh production
#/
#/ Options:
#/   --help: Show this message and exit
usage() { grep '^#/' "$0" | cut -c4- ; exit 0 ; }
expr "$*" : ".*--help" > /dev/null && usage


function logger() { echo -e "[ kp-scrapers ] $@"; }


# NOTE url to match what is defined in `scrapinghub.yml`
readonly AWS_REGION="eu-west-1"
readonly KPLER_IMAGE_REGISTRY="447157256452.dkr.ecr.eu-west-1.amazonaws.com"
readonly KPLER_IMAGE_REPO="kp-scrapers-pdf-prod"


function build_image() {
  local tag="$1"

  logger "building image ${tag} ..."
  docker build \
    --pull \
    --rm \
    --tag "${KPLER_IMAGE_REPO}" \
    .
}


function upload_image() {
  local tag=$1
  local target_image="${KPLER_IMAGE_REGISTRY}/${KPLER_IMAGE_REPO}"

  logger "uploading image version ${tag} to ${KPLER_IMAGE_REPO} repository ..."
  # TODO stop tagging branch images as `latest` when deploying on staging
  docker tag "${KPLER_IMAGE_REPO}:latest" "${target_image}:${tag}"
  docker push "${target_image}:${tag}"
}


function deploy_image() {
  local scrapinghub_env=$1

  logger "deploying image to ${scrapinghub_env} ..."
  shub image deploy \
    --username "${aws_username}" \
    --password "${aws_password}" \
    --version "$(_image_tag)" \
    "${scrapinghub_env}"
}


function _login_aws() {
  logger "login to AWS ..."

  local __aws_creds=$(aws ecr get-login --region "${AWS_REGION}" | sed 's/-e none //g')

  # global scope to make it available for `shub image deploy`
  readonly aws_username="$(echo "${__aws_creds}" | cut -d' ' -f 4)"
  readonly aws_password="$(echo "${__aws_creds}" | cut -d' ' -f 6)"
  eval "${__aws_creds}"
}


# get current app version
function _local_project_version() {
  echo "$(sed -n -e 's/^__version__ = //p' kp_scrapers/__init__.py | sed "s/'//g")"
}


# if not on master branch, tag image with branch name for clarity, else use project version
function _image_tag() {
  local branch="$(git rev-parse --abbrev-ref HEAD | tr '/' '-')"
  if [[ "$branch" != "master" ]]
  then
    echo "$branch"
  else
    echo "$(_local_project_version)"
  fi
}

# REQUIRED to supply scrapinghub env name
# see names as defined in `scrapinghub.yml`
readonly target_env=$1

# remove unexpected side effects
logger "removed python bytecode and cache"
make clean > /dev/null

# login to AWS
_login_aws

# build > upload > deploy image
build_image "$(_image_tag)"
upload_image "$(_image_tag)"
deploy_image "${target_env}"

logger "notifying monitoring backend of deployment results ..."
./tools/cli/kp-release --project "$target_env" --user "$USER" || true

logger "deployment complete"
