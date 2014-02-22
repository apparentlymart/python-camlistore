from ez_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages

setup(
    name="camlistore",
    version="dev",
    description="Camlistore Client Library",
    packages=find_packages(),
    author="Martin Atkins",
    author_email="mart@degeneration.co.uk",

    test_suite='nose.collector',

    setup_requires=[
        'nose>=1.0',
    ],
    tests_require=[
        'nose>=1.0',
        'coverage',
        'mock',
        'pep8',
    ],
    install_requires=[
        "requests",
    ],
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
    ]
)
