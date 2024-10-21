from setuptools import setup, find_packages

setup(
    name='imdb_analysis',
    version='0.1',
    description='IMDb top 10 movies',
    packages=find_packages(exclude=['tests', 'tests.*']),
    tests_require=['pytest'],
    setup_requires=['pytest-runner'],
    include_package_data=True,
    zip_safe=True
)
