from setuptools import find_packages, setup


def readme():
    with open('README.rst') as f:
        return f.read()


setup(
    name='autosparkml',
    version='0.8',
    description='Auto-SparkML',
    long_description=readme(),
    platforms=['any'],
    packages=find_packages(),
    include_package_data=True,
    url='https://github.com/dwkim78',
    license='Apache v2.0',
    author='Dae-Won Kim',
    author_email='dwk@etri.re.kr',
    install_requires=['numpy>=1.13.0', 'protobuf>=3.4.0'],
    keywords=['automl', 'machine learning', 'big data', 'spark'],
    classifiers=[
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.5',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
    ]
)
