from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="fuzzydata",
    version="0.0.4",
    scripts=['./scripts/fuzzydata'],
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
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(),
    python_requires=">=3.7",
    include_package_data=True,
    install_requires=[
        'faker>=13.3.0',
        'pandas>=1.4.0',
        'networkx>=2.7',
        'SQLAlchemy>=1.4.31'
    ],
    extras_require={
        'modin': ['modin[all]>=0.13.2']
    }
)
