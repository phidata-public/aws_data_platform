from setuptools import find_packages, setup

version = "0.1.0"

setup(
    name="data",
    version=version,
    author="Data Team",
    author_email="data@team.com",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "matplotlib",
        "pandas",
        "phidata",
        "psycopg2-binary",
        "sqlalchemy",
    ],
)
