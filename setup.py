from setuptools import setup, find_packages

with open('README.md', 'r') as readme_file:
    long_description = readme_file.read()

requirements = ['pyspark>=2']

setup(
    name='py-dataframe-show-reader',
    version='1.0.0',
    author='Steve Whalen',
    author_email='sjwhalen@yahoo.com',
    description='Reads the output of a DataFrame.show() statement into a DataFrame',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/internetsystemsgroup/py-dataframe-show-reader',
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
