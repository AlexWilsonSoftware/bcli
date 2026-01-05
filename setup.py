from setuptools import setup

setup(
    name='bcli',
    version='0.1.0',
    py_modules=['bcli'],
    install_requires=[
        'click==8.1.7',
    ],
    entry_points={
        'console_scripts': [
            'bcli=bcli:main',
        ],
    },
)
