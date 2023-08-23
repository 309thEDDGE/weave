# Weave Contributing Guide
This contribution guide gives instructions on contributing to the Weave repository. Anyone is welcome to create a pull request for this project as long as they follow these guidelines. This guide will include repository organization, coding standards, process for submitting a pull request, and our Weave style guide.

## Repository Organization

### Root
The root folder contains all of the necessary information about Weave, including a <a href="https://github.com/309thEDDGE/weave/blob/main/README.md">README</a> and a <a href="https://github.com/309thEDDGE/weave/blob/main/setup.py">setup.py</a> file to setup the current version of Weave.

### .github/workflows
<p>The <a href="https://github.com/309thEDDGE/weave/blob/main/.github/workflows/pytest_and_ruff.yml">.github/workflows</a> folder contains the workflow for Weave and will run tests. There are three tests run in the Weave CI/CD workflow: pytest, ruff, and pylint. </p>

<p>Pytest ensures that all of the pytests created in the repository pass. Ruff will statically analyze your code. Pylint will check if your code follows the PEP-8 style guide, which is the style guide that we follow for this repository.</p>

<p>Note: Pylint MUST pass with a score of 10/10.</p>

### License
The <a href="https://github.com/309thEDDGE/weave/blob/main/license/LICENSE.txt">license</a> folder contains the license for using this repository. The license should not be edited by anyone besides the owners of the repository.

### Weave
<p>The <a href="https://github.com/309thEDDGE/weave/tree/main/weave">weave</a> folder contains the functionality of this repository.</p>

<p>The files in the weave folder contain the functionality for creating and maintaining complex data warehouses.</p>

<p>The files in the <a href="https://github.com/309thEDDGE/weave/tree/main/weave/tests">test</a> folder are used to test the main functionality files in the weave folder. These tests should be written using pytest.</p>

<p>The files in the <a href="https://github.com/309thEDDGE/weave/tree/main/weave/index">index</a> folder are used for the functionality of indexes in complex data warehouses.</p>

## Coding Standards
<p>The entire repository follows the PEP-8 style guide. Please refer to the PEP-8 official website <a href="https://pep8.org">https://pep8.org</a> for the guidelines on following the PEP-8 style.</p>

<p>When a pull request is created, the code must pass the three checks in the Weave CI/CD workflow expalined in the.github/workflows folder. This helps ensures that the code follows our repository style guidelines.</p>

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

## Weave Style Guide
<p>Welcome to the weave Coding Style Guide. This comprehensive guide is designed to ensure uniformity, readability, and maintainability in our codebase. By following these guidelines, you'll contribute to a consistent and efficient development process, enhancing collaborative efforts, and streamlining code reviews. As part of our commitment to excellence we aim for not only a smooth code execution but also strive for rigorous adherence to quality; therefore, we aspire for your code to pass the rigorous tests of both ruff and pylint at a 10/10 rating. We are also following PEP 8 standards.</p>

### Classes
<p>Class names should be written in Pascal Case, with no underscores between words.</p>

<p>See PEP-8 <a href="https://peps.python.org/pep-0008/#class-names">Class Name</a> guide.</p>

### Functions and Methods
<p>Function and Method names should be written in all lowercase, with underscores between each word.</p>

<p>For Function and Method Docstrings, go to Docstrings.</p>

<p>If you have a function that exceeds the line limit, check <a href="https://peps.python.org/pep-0008/#indentation">PEP-8 Indentation</a> guide.</p>

<p>See PEP-8 <a href="https://peps.python.org/pep-0008/#function-and-variable-names">Function and Variable</a> guide.</p>

### Variables
<p>Variable names should we written in all lowercase with underscores between each word.</p>

<p>It is encouraged that Variable names be consistent between Classes, Functions/Methods, and different files. We do this because we want to increase readability and simplicity. If we name an object 'temp_basket_dir' in a couple functions, then we should strive to keep that name consistent throughout weave. (encouraged to make consistent names, but not required)</p>

<p>Variable names should be short, but descriptive.</p>

<p>Variable names must be 3 letters or longer.</p>

<p>If a parameter in one function is the same as a parameter in another function, the parameter names should be the same. For example, if you need a basket directory as a parameter, both should be 'basket_dir' instead of three separate parameters being 'bask_directory,' 'bask_dir,' or 'basket_directory.' (encouraged to make consistent names, but not required)</p>

<p>Must keep kwargs variable names consistent between kwargs throughout weave.</p>

<p>See PEP-8 <a href="https://peps.python.org/pep-0008/#function-and-variable-names">Function and Variable</a> guide.</p>

### Docstrings
<p>Docstrings serve as crucial string literals, providing descriptions for Classes, Functions, and Methods. We prioritize creating succinct docstrings that offer brief explanations about the purpose of a class or function. This practice is designed to assist individuals who are unfamiliar with the codebase in understanding the functionality of each component.</p>

<p>Docstrings have 4 sections: <i>Brief description, Detailed Description, Parameters, Returns, Use Case Example</i>.</p>

<p>A well-structured docstring opens with a succinct overview limited to the very first line, outlining the class's purpose. Following a blank line, a more detailed description can be added if necessary. The subsequent "<i>Parameters</i>" section should enumerate parameter names, types, and succinct explanations, maintaining the same order as their appearance in the function call. If the function has a kwargs, then those should be listed separately after the rest of the parameters. Afterwards, a "<i>Returns</i>" section follows, applicable only when the function produces a return value. In this section the return value's type and purpose are described. This format ensures coherent documentation of classes and functions, promoting clarity throughout the codebase.</p>

<p>As an optional section, you can add an example use case to help end users know how to use the class or function.</p>

<p>See PEP-8 <a href="https://peps.python.org/pep-0257/">Docstring Conventions</a> guide.</p>

Here is an example of what a Docstring should look like for a function:













