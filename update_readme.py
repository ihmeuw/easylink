""" This script updates the README.rst file with the latest information about
the project. It is intended to be run from the github "update README" workflow.
"""

import json
import re

from packaging.version import parse

# Load supported python versions
with open("python_versions.json", "r") as f:
    versions = json.load(f)
versions_str = ", ".join(versions)
versions = [parse(v) for v in versions]
max_version = max(versions).base_version

# Open README and replace python versions
with open("README.rst", "r") as file:
    readme = file.read()
# Update the list of supported python versions
# NOTE: this regex assumes the version format is always major.minor
readme = re.sub(
    r"Supported Python versions:\s*(?:\d+\.\d+\s*,\s*)+\d+\.\d+",
    r"Supported Python versions: " + versions_str,
    readme,
)
# Update the python version used in the installation code snipped example
readme = re.sub(r"python=\d+\.\d+", "python=" + max_version, readme)

# Write the updated README back to file
with open("README.rst", "w") as file:
    file.write(readme)
