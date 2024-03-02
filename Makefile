# Define make help functionality
.DEFAULT_GOAL := help
define PRINT_HELP_PYSCRIPT
import re, sys
for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

help: ## Get list of make commands to build and run this job
	@printf -- "Make commands for BCG case Study\n"
	@python3 -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

prep_to_run: ## Unzip the data
	unzip Data.zip -d . -x "__MACOSX*"

remove_unwanted_files: ## Remove unwanted dir that got created as a side effect of running build
	rm -rf build
	rm -rf dist
	rm -rf *.egg-info

build: remove_unwanted_files ## Packages all the python packages(each jobs and utils) in a single `.egg` file.
	/usr/bin/python3 setup.py bdist_egg
	rm -rf build
	rm -rf *.egg-info
