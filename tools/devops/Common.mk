# Language agnostic Makefile settings

.PHONY: love
love:
	@echo "not war !"

.PHONY: help
help: ## print this message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)''

.PHONY: tasks
tasks: ## grep TODO and FIXME project-wide
	@grep --exclude-dir=.git --exclude-dir=node_modules -rEI "TODO|FIXME" .
