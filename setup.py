from setuptools import find_packages, setup

setup(
    name="queueclient",
    version="2.0.3",
    description="Generic Queue Client",
    packages=find_packages(exclude=("tests", "docs")),
    install_requires=["pika==1.1.0", "requests>=2,<3", "simplejson>=3,<4"],
)
