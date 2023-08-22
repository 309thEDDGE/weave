<h1>Weave Contributing Guide</h1>
This contribution guide gives instructions on contributing to the Weave repository. Anyone is welcome to create a pull request for this project as long as they follow these guidelines. This guide will include repository organization, coding standards, process for submitting a pull request, and our Weave style guide.

<br>
<h2>Repository Organization</h2>

<h3>Root</h3>
The root folder contains all of the necessary information about Weave, including a <a href="https://github.com/309thEDDGE/weave/blob/main/README.md">README</a> and a <a href="https://github.com/309thEDDGE/weave/blob/main/setup.py">setup.py</a> file to setup the current version of Weave.

<h3>.github/workflows</h3>
The <a href="https://github.com/309thEDDGE/weave/blob/main/.github/workflows/pytest_and_ruff.yml">.github/workflows</a> folder contains the workflow for Weave and will run tests. There are three tests run in the Weave CI/CD workflow: pytest, ruff, and pylint. Pytest ensures that all of the pytests created in the repository pass. Ruff will statically analyze your code. Pylint will check if your code follows the PEP-8 style guide, which is the style guide that we follow for this repository. 
<br>
Note: Pylint MUST pass with a score of 10/10.

<h3>License</h3>
The <a href="https://github.com/309thEDDGE/weave/blob/main/license/LICENSE.txt">license</a> folder contains the license for using this repository. The license should not be edited by anyone besides the owners of the repository.

<h3>Weave</h3>
The <a href="https://github.com/309thEDDGE/weave/tree/main/weave">weave</a> folder contains the functionality of this repository. 
The files in the weave folder contain the functionality for creating and maintaining complex data warehouses. 
<br>
The files in the <a href="https://github.com/309thEDDGE/weave/tree/main/weave/tests">test</a> folder are used to test the main functionality files in the weave folder. These tests should be written using pytest. 
<br>
The files in the <a href="https://github.com/309thEDDGE/weave/tree/main/weave/index">index</a> folder are used for the functionality of indexes in complex data warehouses.

<h2>Coding Standards</h2>
The entire repository follows the PEP-8 style guide. Please refer to the PEP-8 official website <a href="https://pep8.org">https://pep8.org</a> for the guidelines on following the PEP-8 style. 
<br>
When a pull request is created, the code must pass the three checks in the Weave CI/CD workflow expalined in the.github/workflows folder. This helps ensures that the code follows our repository style guidelines. 
<br>

<h2>Process for Submitting a Pull Request</h2>
Changes to this repository should always be done through pull requests. Anyone is welcome to submit a pull request to be reviewed for merging into the main branch. However, these changes may not always be accepted by the owners of the repository.
<br>
<br>
- 1. Clone the repository in your local environment using ```git clone```
<br>
- 2. 


