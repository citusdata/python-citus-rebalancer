from setuptools import setup, find_packages
from os import path
from io import open

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name='py-citus-rebalancer', # Required
    version='1.0.4',  # Required
    description='A python command line tool to provide the commands necessary to run on citus to have a balanced cluster',
    url='https://github.com/citusdata/python-citus-rebalancer',
    author='Louise Grandjonc',
    author_email='louise.grandjonc@microsoft.com',
    long_description=long_description,

    # Classifiers help users find your project by categorizing it.
    #
    # For a list of valid classifiers, see https://pypi.org/classifiers/
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers"
    ],

    keywords='postgresql citus rebalancer',  # Optional
    packages=find_packages(exclude=['tests']),  # Required
    python_requires='>=3.5',
    install_requires=['psycopg2', 'click'],  # Optional
    entry_points={  # Optional
        'console_scripts': [
            'citus-rebalancer=citus_rebalancer.command:find_ideal_rebalance',
        ],
    },

)
