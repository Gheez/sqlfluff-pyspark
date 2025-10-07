#!/usr/bin/env python3
"""Setup script for sqlfluff-pyspark-templater."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="sqlfluff-pyspark-templater",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="SQLFluff templater for PySpark f-string SQL blocks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/sqlfluff-pyspark-templater",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.9",
    install_requires=[
        "pyspark>=4.0.1",
        "sqlfluff>=3.4.2",
    ],
    entry_points={
        "sqlfluff.templater": [
            "pyspark = sqlfluff_pyspark_templater.templater:PySparkTemplater",
        ],
        "console_scripts": [
            "sqlfluff-pyspark = sqlfluff_pyspark_templater.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)