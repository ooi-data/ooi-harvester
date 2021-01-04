from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

import os

from codecs import open

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

# Dependencies.
with open("requirements.txt") as f:
    requirements = f.readlines()
install_requires = [t.strip() for t in requirements]

with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="ooi_harvester",
    version="0.1.0",
    description="OOI Data Harvester",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    author="Landung Setiawan",
    author_email="landungs@uw.edu",
    maintainer="Landung Setiawan",
    maintainer_email="landungs@uw.edu",
    python_requires=">=3",
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering",
    ],
    keywords=["Data", "Access", "OOI"],
    include_package_data=True,
    packages=find_packages(),
    install_requires=install_requires,
    setup_requires=["setuptools>=38.6.0", "wheel>=0.31.0", "twine>=1.11.0"],
    entry_points=dict(
        console_scripts=["ooi-harvester = ooi_harvester.cli:app"]
    ),
)
