import os

from setuptools import find_packages, setup

from kp_scrapers import __package__ as app_name


def get_long_description() -> str:
    """Use README.md file as long description."""
    root = os.path.abspath(os.path.dirname(__file__))
    readme = os.path.join(root, "README.md")
    with open(readme, mode="rt") as stream:
        return stream.read()


# NOTE `shub` cli will throw an exception if a main sentinel is used to scope the following
# TODO potential bug ? to be raised at https://github.com/scrapinghub/shub/issues
setup(
    name=app_name,
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description="Gather, normalize, validate data across multiple sources and schemas",
    # NOTE disabled for now, as `shub` cli throws an exception
    # long_description=get_long_description(),
    # long_description_content_type="text/markdown",
    url="https://github.com/Kpler/kp-scrapers",
    author="Kpler",
    author_email="engineering@kpler.com",
    license="Copyright",
    classifiers=[
        "License :: Other/Proprietary License",
        "Intended Audience :: Other Audience",
        "Development Status :: 5 - Production/Stable",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    keywords="scrapers normalisation validation",
    packages=find_packages(exclude=["tests*"]),
    entry_points={"scrapy": ["settings = kp_scrapers.settings"]},
    python_requires=">=3.6.0",
    scripts=[
        # FIXME `kp-shub` relies on `libshub` which is not installed,
        # hence we can't use the script globally
        # "tools/cli/kp-shub",
        "tools/cli/kp-vault",
        "tools/cli/jl_to_csv.py",
        "tools/cli/kp-state-snapshot",
        "tools/cli/kp-notify",
    ],
    include_package_data=True,  # include files found in MANIFEST.in
)
