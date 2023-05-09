from setuptools import find_packages, setup

setup(
    name="queueclient",
    use_scm_version={
        "local_scheme": "no-local-version",
        "write_to": "queueclient/_version.py",
    },
    description="Queue Client Facade: A generalized API that allows for connections to multiple queue implementations.",
    license="Apache",
    packages=find_packages(exclude=("tests", "docs")),
    install_requires=["pika~=1.1.0", "requests>=2,<3", "simplejson>=3,<4"],
    setup_requires=["setuptools_scm~=6.4"],
)
