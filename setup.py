from setuptools import find_packages, setup

version = "0.1.0"
min_phidata_version = "0.1.11"

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
        f"phidata>={min_phidata_version}",
        "psycopg2-binary",
        "sqlalchemy",
    ],
)
