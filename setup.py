"""
Setup script for dial_file_generator package.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding='utf-8') if readme_file.exists() else ""

# Read requirements from requirements.txt
requirements_file = Path(__file__).parent / "requirements.txt"
if requirements_file.exists():
    install_requires = [
        line.strip()
        for line in requirements_file.read_text(encoding='utf-8').splitlines()
        if line.strip() and not line.strip().startswith('#')
    ]
else:
    install_requires = []

setup(
    name="dial-file-generator",
    version="0.1.0",
    description="Python module for generating dial files from Google Sheets and processing call data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/dial-file-generator",
    packages=find_packages(),
    install_requires=install_requires,
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    entry_points={
        "console_scripts": [
            "import-customers=dial_file_generator.import_customers:main",
            "create-filter-file=dial_file_generator.create_filter_file:main",
        ],
    },
)

