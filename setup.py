from setuptools import setup, find_packages
import os


def read_requirements():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    requirements_path = os.path.join(current_dir, 'req.txt')
    with open(requirements_path) as req:
        return req.read().splitlines()


setup(
        name="orc",
        version="0.1",
        packages=find_packages(),
        install_requires=read_requirements(),
        author="Mice Pápai",
        author_email="hello@micepapai.com",
        description="Multiprocessing data pipeline orchestrator",
        long_description=open('README.md').read(),
        long_description_content_type='text/markdown',
        url="https://github.com/qwhex/orc",
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Developers",
            "License :: Other/Proprietary License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
        ],
        python_requires='>=3.9',
        license="All Rights Reserved",
)
