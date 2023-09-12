from setuptools import find_packages, setup

with open("requirements/base.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="commonutils",
    version="1.0.0",
    author="1mg",
    author_email="devops@1mg.com",
    url="https://github.com/tata1mg/commonutils",
    description="Common utilities for python 3.7+",
    packages=find_packages(exclude=("requirements",)),
    install_requires=requirements,
)
