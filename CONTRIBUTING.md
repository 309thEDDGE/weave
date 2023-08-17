<h1>Weave Contributing Guide</h1>
This contribution guide gives instructions on contributing to the Weave repository. Anyone is welcome to create a pull request for this project as long as they follow these guidelines. This guide will include repository organization, coding standards, process for submitting a pull request, and our Weave style guide.
<br>
<h2>Repository Organization</h2>
<h3>Root</h3>
The root folder contains all of the necessary information about Weave, including a <a href="https://github.com/309thEDDGE/weave/blob/main/README.md">README</a> and a <a href="https://github.com/309thEDDGE/weave/blob/main/setup.py">setup.py</a> file to setup the current version of Weave.
<h3>.github/workflows</h3>
The <a href="https://github.com/309thEDDGE/weave/blob/main/.github/workflows/pytest_and_ruff.yml">.github/workflows</a> folder contains the workflow for Weave and will run tests. There are three tests run in the Weave CI/CD workflow: pytest, ruff, and pylint. Pytest ensures that all of the pytests created in the repository pass. Ruff will statically analyze your code. Pylint will check if your code follows the PEP-8 style guide, which is the style guide that we follow for this repository. 
Note: Pylint MUST pass with a score of 10/10.
