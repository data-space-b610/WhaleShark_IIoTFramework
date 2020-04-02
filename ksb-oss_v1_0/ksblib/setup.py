from setuptools import find_packages, setup


setup(
    name='ksblib',
    version='0.1.0',
    description='KSB Python Common Library',
    platforms=['any'],
    packages=find_packages(),
    include_package_data=True,
    url='',
    license='Apache v2.0',
    author='Dae-Won Kim',
    author_email='dwk@etri.re.kr',
    install_requires=['numpy>=1.13.0', 'tornado>=5.0'],
    keywords=['KSB', 'Python', 'Library', 'Dockerize'],
    classifiers=[
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.5',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
    ]
)
