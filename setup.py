from setuptools import setup, find_packages

setup(
    name="GDC queues client",
    version="1.0.0",
    description=find_packages(exclude={'tests', 'docs'}),
    install_requires=[
        "pika==0.12.0",
        "requests>=2.7"
    ]
)