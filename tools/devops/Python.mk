# Python-related directives

.PHONY: warn-requirements
warn-requirements: ## check your setup
	$(call is_installed, git-hooks)
	$(call is_installed, shub)
	$(call is_installed, $(VENV))
	$(call is_installed, $(TESTER))
	$(call is_installed, $(LINTER))

.PHONY: build
build: ## locally install library and dev requirements
	pip install -r dev-requirements.txt
	./tools/cli/kp-vault decrypt
	pip install -r kp-requirements.txt -r requirements.txt
	git hooks install

.PHONY: lint
lint: ## lint library: make lint
	git diff master -- '*.py' | $(LINTER) --diff

# TODO lint before also
.PHONY: test
test: warn-requirements ## run test suite
	$(TESTER) --verbose \
		--with-timer \
		--with-xunit --xunit-file=/tmp/$(PROJECT)-tests.xml \
		--with-coverage --cover-html --cover-erase --cover-package=$(TARGET) --cover-html-dir=./code_review/coverage \
		--with-doctest
	./tools/cli/acceptance.py --count 200 --xunit-files /tmp/$(PROJECT)-tests.xml

test-spider: warn-requirements ## helper for a specific test file
	$(TESTER) --verbose \
		--with-timer --with-doctest \
		$(TARGET)

.PHONY: clean
clean: ## remove buid artifacts and temporary files
	rm -rf *.egg-info dist build
	find . -name '__pycache__' -exec rm -rf {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +


index:
	ctags --recurse=yes --langmap=python:.py --python-kinds=-iv \
		--exclude=tests --exclude=tools/ --exclude=githooks/ --exclude=scheduling/ --exclude=code_review/ --exclude=doc/ --exclude=build --exclude=dist \
		$(PROJECT)
