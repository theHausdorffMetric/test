# Root Makefile

include ./tools/devops/Config.mk
include ./tools/devops/Common.mk
include ./tools/devops/Python.mk

doc-build:  ## Build the sphinx documentation locally
	make -C doc html
	make -C doc coverage

doc-view:  ## View the spinx documentation
	open doc/build/html/index.html

# export `KPLER_DOC_SERVER_CREDENTIALS` if you want to skip password
doc-publish: doc-build  ## Publish the documentation to doc.kpler.com
	cd doc/build/html && zip -r /tmp/doc.zip * && cd ../../../
	curl --fail -X POST \
		--form name=kp-scrapers -F ":action=doc_upload" -F content=@/tmp/doc.zip \
		-u ${KPLER_DOKANG_CIRCLECI_USERNAME}:${KPLER_DOKANG_CIRCLECI_PASSWORD} \
		https://doc.kpler.com/upload
