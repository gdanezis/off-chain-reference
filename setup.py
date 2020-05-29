# Copyright (c) The Libra Core Contributors
# SPDX-License-Identifier: Apache-2.0

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="calibra-offchainapi",
    author="Calibra",
    author_email="",
    description="An implementation of the Calibra Off-chain API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/calibra/off-chain-api",
    packages=setuptools.find_packages(where='src'),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=[
        'aiohttp',
        'bech32',
        'jwcrypto',
    ],
    version="0.0.1.dev1",
)
