from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="fuzzydata",
    version="0.1.0",
    scripts=['./scripts/fuzzydata'],
    entry_points={
        'console_scripts': [
            # Inside the package, not scripts/, so it ships in the sdist and wheel.
            'fuzzydata-corpus=fuzzydata.lineage.corpus_cli:main',
        ],
    },
    author="Suhail Rehman",
    author_email="suhailrehman@gmail.com",
    description="Fuzzy Data Benchmark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/suhailrehman/fuzzydata",
    project_urls={
        "Bug Tracker": "https://github.com/suhailrehman/fuzzydata/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(),
    # Aligned with the CI matrix floor. 3.7/3.8 were claimed but never tested.
    python_requires=">=3.9",
    include_package_data=True,
    install_requires=[
        'faker>=13.3.0',
        'pandas>=1.4.0',
        'networkx>=2.7',
        'SQLAlchemy>=2.0.0',
        # parquet is the recommended artifact format for corpus generation: csv round-trips
        # every value through text, so dtypes survive only by inference.
        'pyarrow>=10.0.0',
    ],
    extras_require={
        # The modin client is DEPRECATED as of 0.1.0 and is not covered by CI. It still
        # ships and still works; see fuzzydata/clients/modin.py.
        'modin': ['modin[all]>=0.13.2'],
        'dev': ['pytest', 'pytest-cov', 'pytest-dependency'],
    }
)
