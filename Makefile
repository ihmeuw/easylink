# Check if we're running in Jenkins
ifdef JENKINS_URL
	# Files are already in workspace from shared library
	MAKE_INCLUDES := .
else
	# For local dev, search in parent directory
	MAKE_INCLUDES := ../vivarium_build_utils/resources/makefiles
endif

PACKAGE_NAME = easylink


# Include the makefiles
include $(MAKE_INCLUDES)/base.mk
include $(MAKE_INCLUDES)/test.mk

install: ENV_REQS?=dev
install:
# Don't install Graphviz for docs. This could break if we add doctests which require graphviz!
# Note that installing the python package graphviz doesn't actually require the graphviz system package.
	$(if $(filter-out docs,${ENV_REQS}),conda install -y python-graphviz,)
	$(MAKE) -f $(MAKE_INCLUDES)/base.mk install ENV_REQS=${ENV_REQS}