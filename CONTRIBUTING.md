# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

## Types of Contributions

### Report Bugs


Report bugs at https://github.com/dataverbinders/nl-open-data/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with *bug* (and possibly *help
wanted*, though not necessarily) is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with *enhancement*
(and, again, possibly with *help wanted*) is open to whoever wants to implement it.

### Write Documentation

`nl-open-data` could always use more documentation, whether as part of the
official `nl-open-data` docs, in docstrings, or even on the web in blog posts,
articles, and such.

### Submit Feedback

The best way to send feedback is to file an issue at https://github.com/dataverbinders/nl-open-data/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

## Get Started!

Ready to contribute? Here's how to set up `staline-bq` for local development.

1. Fork the `staline-bq` repo on GitHub.
2. Clone your fork locally:    
    ```
    $ git clone git@github.com:your_name_here/staline-bq.git
    ```
3. Install your local copy using Poetry. Assuming you have Poetry installed, this is how you set up your fork for local development::
    ```
    $ cd staline-bq/
    $ poetry install
    ```
4. Create a branch for local development:
    ```
    $ git checkout -b name-of-your-bugfix-or-feature
    ```
   Now you can make your changes locally.
   &nbsp;
5. When you're done making changes, check that your changes pass flake8 and the tests, including testing other Python versions with tox:

    ```
    $ cd staline-bq/
    $ poetry shell    
    $ poetry run pytest
    ```
6. Commit your changes and push your branch to GitHub:
    
    ```
    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature
    ```

7. Submit a pull request through the GitHub website.

### Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.md.
3. The pull request should work for Python 3.5, 3.6, 3.7 and 3.8, and for PyPy. Check
   https://travis-ci.com/dkapitan/nl_open_data/pull_requests
   and make sure that the tests pass for all supported Python versions.

### Tips

To run a subset of tests::


    $ python -m unittest tests.test_nl_open_data

### Deploying

A reminder for the maintainers on how to deploy.
Make sure all your changes are committed (including an entry in HISTORY.rst).
Then run::

$ bump2version patch # possible: major / minor / patch
$ git push
$ git push --tags

Travis will then deploy to PyPI if tests pass.
