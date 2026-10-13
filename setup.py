from setuptools import setup, find_packages  # type: ignore

with open('README.md') as f:
    long_description = f.read()

package_name = 'django-manticore'


setup(
    name=package_name,
    version='0.3.0',
    python_requires='>=3.10,<3.14',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(where='.', exclude=['testproject', 'testproject.*']),
    url='https://github.com/just-work/django-manticore',
    license='MIT',
    author='Sergey Tikhonov',
    author_email='zimbler@gmail.com',
    description='Manticoresearch database backend for Django',
    install_requires=[
        'Django>=4.2,<6; python_version < "3.12"',
        'Django>=4.2.8,<6; python_version == "3.12"',
        'Django>=5.1.3,<6; python_version == "3.13"',
        'mysqlclient>=2.2.7,<3',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Framework :: Django :: 4.2',
        'Framework :: Django :: 5.0',
        'Framework :: Django :: 5.1',
        'Framework :: Django :: 5.2',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Software Development :: Libraries',
        'Topic :: System :: Distributed Computing',
    ]
)
