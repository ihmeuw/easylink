#!/usr/bin/env python
import os

from setuptools import find_packages, setup

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
        "layered_config_tree",
        "networkx",
        "pandas",
        "pyyaml",
        "pyarrow",
        "snakemake>=8.0.0",
        # TODO MIC-4963: Resolve quoting issue and remove pin
        "snakemake-interface-executor-plugins<9.0.0",
        "snakemake-executor-plugin-slurm",
    ]

    setup_requires = ["setuptools_scm"]

    # use "pip install -e .[dev]" to install required components + extra components
    test_requirements = [
        "pytest",
        "pytest-cov",
        "pytest-mock",
    ]
    doc_requirements = [
        "sphinx>=4.0,<8.0.0",
        "sphinx-rtd-theme>=0.6",
        "sphinx-autodoc-typehints",
        "sphinx-click",
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
