# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
from importlib import import_module
sys.path.insert(0, os.path.abspath('..'))


project = 'dlairflow'
copyright = '2024, Astro Data Lab'
author = 'Astro Data Lab'
package = import_module(project)
version = '.'.join(package.__version__.split('.')[:2])
release = package.__version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.viewcode',
    'sphinx.ext.todo',
    'sphinx.ext.githubpages',
    'sphinx_rtd_theme'
]

templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# If true, keep warnings as "system message" paragraphs in the built documents.
keep_warnings = True

# Include functions that begin with an underscore, e.g. _private().
napoleon_include_private_with_doc = True

# We don't necessarily need a full installation to build documentation.
autodoc_mock_imports = []
for missing in ('airflow', ):
    try:
        foo = import_module(missing)
    except ImportError:
        autodoc_mock_imports.append(missing)

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
# html_static_path = ['_static']

# -- Options for intersphinx extension ---------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html#configuration

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'airflow': ('https://airflow.apache.org/docs/apache-airflow/stable/', None)
}

# -- Options for todo extension ----------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/todo.html#configuration

todo_include_todos = True
