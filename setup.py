from setuptools import setup, find_packages

setup(
    name="clearml_to_slurm",
    version="0.2.0",
    packages=find_packages(),
    package_data={
        "clearml_to_slurm": ["templates/*.j2"],
    },
    install_requires=[
        "clearml",
        "clearml-agent",
        "jinja2",
    ],
    entry_points={
        "console_scripts": [
            "to_slurm=clearml_to_slurm.main:main",
        ],
    },
    author="Willy P",
    description="Long running job that pulls pending clearml jobs from your queue and converts them into slurm jobs",
    python_requires=">=3.10",
)
