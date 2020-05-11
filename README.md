# pyspark-interview

PySpark project for interview coding activity
## Project requirements

- Python 3.7 or newer
- pipenv tool
- PyCharm (optional)

## Project initialization
```pipenv install --dev```

To run tests

```pipenv run python -m unittest test/test_*.py  ```

To run sample job

```pipenv run spark-submit --master local\[4\] jobs/job_example.py```

## Known issues:
#### Warning: the environment variable LANG is not set!

```export LANG=en_US.UTF-8```

Best to add it to your .zshrc or .bashrc profiles
