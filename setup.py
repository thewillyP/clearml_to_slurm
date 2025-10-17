from setuptools import setup, find_packages

setup(
    name="clearml_to_slurm",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "clearml",
        "clearml-agent",
    ],
    entry_points={
        "console_scripts": [
            "to_slurm=clearml_to_slurm.main:main",
        ],
    },
    author="Willy P",
    description="Long running job that pulls pending clearml jobs from your queue and converts them into slurm jobs",
    python_requires=">=3.6",
)
