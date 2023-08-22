# Weave Contributing Guide
This contribution guide gives instructions on contributing to the Weave repository. Anyone is welcome to create a pull request for this project as long as they follow these guidelines. This guide will include repository organization, coding standards, process for submitting a pull request, and our Weave style guide.

## Repository Organization

### Root
The root folder contains all of the necessary information about Weave, including a <a href="https://github.com/309thEDDGE/weave/blob/main/README.md">README</a> and a <a href="https://github.com/309thEDDGE/weave/blob/main/setup.py">setup.py</a> file to setup the current version of Weave.

### .github/workflows
The <a href="https://github.com/309thEDDGE/weave/blob/main/.github/workflows/pytest_and_ruff.yml">.github/workflows</a> folder contains the workflow for Weave and will run tests. There are three tests run in the Weave CI/CD workflow: pytest, ruff, and pylint. 
Pytest ensures that all of the pytests created in the repository pass. Ruff will statically analyze your code. Pylint will check if your code follows the PEP-8 style guide, which is the style guide that we follow for this repository.
<br>
Note: Pylint MUST pass with a score of 10/10.

### License
The <a href="https://github.com/309thEDDGE/weave/blob/main/license/LICENSE.txt">license</a> folder contains the license for using this repository. The license should not be edited by anyone besides the owners of the repository.

### Weave
The <a href="https://github.com/309thEDDGE/weave/tree/main/weave">weave</a> folder contains the functionality of this repository. 
The files in the weave folder contain the functionality for creating and maintaining complex data warehouses. 
<br>
The files in the <a href="https://github.com/309thEDDGE/weave/tree/main/weave/tests">test</a> folder are used to test the main functionality files in the weave folder. These tests should be written using pytest. 
<br>
The files in the <a href="https://github.com/309thEDDGE/weave/tree/main/weave/index">index</a> folder are used for the functionality of indexes in complex data warehouses.

## Coding Standards
The entire repository follows the PEP-8 style guide. Please refer to the PEP-8 official website <a href="https://pep8.org">https://pep8.org</a> for the guidelines on following the PEP-8 style. 
<br>
When a pull request is created, the code must pass the three checks in the Weave CI/CD workflow expalined in the.github/workflows folder. This helps ensures that the code follows our repository style guidelines. 

## Process for Submitting a Pull Request
Changes to this repository should always be done through pull requests. Anyone is welcome to submit a pull request to be reviewed for merging into the main branch. However, these changes may not always be accepted by the owners of the repository.
<br>
<br>
- 1: Clone the repository in your local environment using `git clone https://github.com/309thEDDGE/weave.git`
- 2: Use `git pull` to get any recent changes on your local environment.
- 3: Create a branch from main to make your changes using `git branch <your branch name>`
- 4: Switch to your newly created branch using `git switch <your branch name>`
- 5: Make any changes to the code in your branch. DO NOT make any changes to the code directly in the main branch. Use `git status` to see what branch you 
are currently in.
- 6: After all of your requested changes have been made to the code, you may add, commit, and push your code to your branch on GitHub.
  Note: These steps may be done at any time to update your branch on github.
  - 1. Add your code to be committed by using `git add .` in the root directory. This will add all of your changes to the staging area to be committed.
  - 2. Commit your changes by using `git commit -m "<your commit message>"`. This will commit all of your changes in the staging area.
  - 3. Push your changes to your branch on GitHub by using `git push`
- 7: You can now create a pull request by clicking on the Pull requests tab on GitHub.
- 8: Click on New Pull Request.
- 9: Find your branch on the compare drop down menu and select it. Note: if you want to request a merge into main, the base should be set to main.
- 10: Click Create pull request.
<br>
After the pull request has been created, members of the 309th SWEG EDDGE team will review your code and may request some changes.
