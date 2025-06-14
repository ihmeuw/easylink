#!/usr/bin/env python
import json
import os
import sys

from packaging.version import parse
from setuptools import find_packages, setup

with open("python_versions.json", "r") as f:
    supported_python_versions = json.load(f)

python_versions = [parse(v) for v in supported_python_versions]
min_version = min(python_versions)
max_version = max(python_versions)
if not (
    min_version <= parse(".".join([str(v) for v in sys.version_info[:2]])) <= max_version
):
    py_version = ".".join([str(v) for v in sys.version_info[:3]])
    # NOTE: Python 3.5 does not support f-strings
    error = (
        "\n--------------------------------------------\n"
        "Error: EasyLink runs under python {min_version}-{max_version}.\n"
        "You are running python {py_version}.\n".format(
            min_version=min_version.base_version,
            max_version=max_version.base_version,
            py_version=py_version,
        )
        + "--------------------------------------------\n"
    )
    print(error, file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    base_dir = os.path.dirname(__file__)
    src_dir = os.path.join(base_dir, "src")

    about = {}
    with open(os.path.join(src_dir, "easylink", "__about__.py")) as f:
        exec(f.read(), about)

    with open(os.path.join(base_dir, "README.rst")) as f:
        long_description = f.read()

    install_requirements = [
        "click",
        "docker",
        "graphviz",
        "loguru",
        "layered_config_tree>=3.0.0",
        "networkx",
        "pandas",
        "pyyaml",
        "pyarrow",
        "requests",
        "snakemake>=8.0.0",
        "tqdm",
        # TODO MIC-4963: Resolve quoting issue and remove pin
        "snakemake-interface-executor-plugins<9.0.0",
        "snakemake-executor-plugin-slurm",
        # Type stubs
        "pandas-stubs",
        "pyarrow-stubs",
        "types-PyYAML",
    ]

    setup_requires = ["setuptools_scm"]

    # use "pip install -e .[dev]" to install required components + extra components
    test_requirements = [
        "pytest",
        "pytest-cov",
        "pytest-mock",
    ]
    doc_requirements = [
        "sphinx<8.2.0",
        "sphinx-rtd-theme",
        "sphinx-autodoc-typehints",
        "sphinx-click",
        "sphinx-autobuild",
        "typing_extensions",
    ]
    lint_requirements = [
        "black==22.3.0",
        "isort",
        "mypy",
    ]

    setup(
        name=about["__title__"],
        description=about["__summary__"],
        long_description=long_description,
        long_description_content_type="text/x-rst",
        license=about["__license__"],
        url=about["__uri__"],
        author=about["__author__"],
        author_email=about["__email__"],
        package_dir={"": "src"},
        packages=find_packages(where="src"),
        include_package_data=True,
        install_requires=install_requirements,
        extras_require={
            "docs": doc_requirements,
            "test": test_requirements,
            "dev": doc_requirements + test_requirements + lint_requirements,
        },
        zip_safe=False,
        use_scm_version={
            "write_to": "src/easylink/_version.py",
            "write_to_template": '__version__ = "{version}"\n',
            "tag_regex": r"^(?P<prefix>v)?(?P<version>[^\+]+)(?P<suffix>.*)?$",
        },
        setup_requires=setup_requires,
        entry_points="""
            [console_scripts]
            easylink=easylink.cli:easylink
        """,
    )
