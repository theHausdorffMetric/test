# Shared configuration file
#
# Documentation guidelines :http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

## project-agnostic configuration

# no arguments will print help, safer than UNIX default
.DEFAULT_GOAL := help

BUILD_TIME := $(shell date +%FT%T%z)

GIT_COMMIT := $(shell git rev-parse --short HEAD)
GIT_USER   := $(shell git config --get user.name)

define is_installed
	@test -n "$(shell which $(1))" || echo "WARNING: $(1) not installed."
endef

## Language-related configuration

LINTER  ?= "flake8"
TESTER  := "nosetests"
VENV    := "workon"  # i.e. virtualenvwrapper

TARGET  ?= "kp_scrapers"
PROJECT ?= "kp_scrapers"
