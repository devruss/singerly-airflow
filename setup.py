import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="singerly_airflow",
    version="0.0.1",
    author="Singerly",
    author_email="noreply@singerly.co",
    description="Singerly Airflow Operator and DAG generator package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(),
    install_required=['boto3'],
    python_requires=">=3.6",
)