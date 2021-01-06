# ooi_harvester

This is the home for OOI Data Harvester Functions used by Cable Array Value Add Team.

## Setup Development

To set up development version for this package, conda environment is used below.

```bash
# Assuming that you are in the root folder or repository
conda create -n harvest-dev -c conda-forge python=3.7 --file requirements.txt --file requirements-dev.txt

conda activate harvest-dev

pip install -e .
```
