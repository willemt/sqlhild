from setuptools import setup, find_packages
import codecs
from os import path

here = path.abspath(path.dirname(__file__))


def long_description():
    with codecs.open('README.rst', encoding='utf8') as f:
        return f.read()


setup(
    name='sqlhild',
    version='0.1.0',

    description='sqlhild SQLifies everything',
    long_description=long_description(),

    # The project's main homepage.
    url='https://github.com/willemt/sqlhild',
    author='willemt',
    author_email='himself@willemthiart.com',
    license='BSD',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: System :: Logging',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='development logging',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=[
        'Pygments',
        'SQLAlchemy',
        'addict',
        'antlr4-python3-runtime',
        'astor',
        'attrs',
        'autopep8',
        'docopt',
        'docutils',
        'lmdb',
        'matchpy',
        'numba',
        'numpy',
        'protlib',
        'psutil',  # Only required for example
        'pygments',
        'tablib',
        'terminaltables',
        'typeguard',
    ],
    package_data={},
    data_files=[],
    entry_points={
        'console_scripts': [
            'sqlhild = sqlhild.__main__:main',
        ],
    },
)
