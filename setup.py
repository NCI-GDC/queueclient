from setuptools import setup, find_packages

setup(
    name="queueclient",
    version="1.1.0",
    description="Generic Queue Client",
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
        "pika==1.1.0",
        "requests>=2.7",
        "simplejson==3.16.0"
    ]
)