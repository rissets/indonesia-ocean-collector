from setuptools import find_packages, setup

setup(
    name="source-kkp-datamart",
    description="Airbyte Source Connector for KKP Datamart (vessel registry and tracking)",
    author="Maritime Data Team",
    author_email="mr.danangharissetiawan@gmail.com",
    packages=find_packages(),
    install_requires=[
        "airbyte-cdk==0.51.0",
        "requests>=2.31.0",
    ],
    package_data={"source_kkp_datamart": ["*.json"]},
    entry_points={
        "console_scripts": [
            "source-kkp-datamart=source_kkp_datamart.run:run",
        ],
    },
)
