from setuptools import find_packages, setup

setup(
    name="data_workspace",
    version="0.1.0",
    author="Data Team",
    author_email="data@team.com",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "matplotlib",
        "psycopg2-binary",
        # "phidata>="0.1.17"",
        # apache-airflow-providers-amazon 3.0.0 requires pandas<1.4,>=0.17.1
        "pandas<1.4",
        # apache-airflow 2.2.4 requires sqlalchemy<1.4.0,>=1.3.18
        "sqlalchemy<1.4.0",
        "tiingo",
        "apache-airflow==2.2.5",
        "apache-airflow-providers-postgres==4.0.0",
        "apache-airflow-providers-amazon==3.2.0",
    ],
)
