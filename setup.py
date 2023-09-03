from setuptools import setup, find_packages


def readme():
    with open("README.md") as f:
        return f.read()


setup(
    name="mindexer",
    version="0.3.0",
    description="Experimental Index Recommendation Tool for MongoDB",
    long_description=readme(),
    long_description_content_type="text/markdown",
    keywords="mongodb indexes recommendations",
    url="http://github.com/mongodb-labs/mindexer",
    author="Thomas Rueckstiess",
    author_email="thomas.rueckstiess@mongodb.com",
    license="MIT",
    packages=find_packages(),
    scripts=["bin/mindexer"],
    install_requires=["numpy", "pandas", "pymongo"],
    zip_safe=False,
)
