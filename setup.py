from setuptools import setup, find_packages

setup(
    name='mac-dot-report',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'iniconfig==2.0.0',
        'Jinja2==3.1.4',
        'MarkupSafe==2.1.5',
        'numpy==2.0.1',
        'packaging==24.1',
        'pandas==2.2.2',
        'pluggy==1.5.0',
        'pytest==8.3.3',
        'python-dateutil==2.9.0.post0',
        'pytz==2024.1',
        'PyYAML==6.0.2',
        'regex==2024.7.24',
        'six==1.16.0',
        'tzdata==2024.1',
    ],
    entry_points={
        'console_scripts': [
            'dotreport=main:main',  # Adjust this to point to your main function
        ],
    },
)