from setuptools import find_packages, setup

setup(
    name="hello_dagster",
    packages=find_packages(exclude=["hello_dagster_tests"]),
    install_requires=["dagster", "dagster-cloud", "pandas"],
    extras_require={"dev": ["dagit", "pytest", "matplotlib", "wordcloud"]},
)
