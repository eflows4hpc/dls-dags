# DLS DAGs

[![DOI](https://zenodo.org/badge/508962576.svg)](https://zenodo.org/badge/latestdoi/508962576)

This repository contains all DAGs that are (or can be) used in the [DLS](https://github.com/eflows4hpc/data-logistics-service).

To develop your own DAG, please see the airflow documentation and/ or look at the example DAGs in this repository.

The DLS deployments for the eflows4HPC project pull updates from this repository in regular intervals (WIP at the moment), which means newly created or modified DAGs are deployed automatically (although with some delay).

## Developement
Run tests with

```
python -m unittest discover tests
```
