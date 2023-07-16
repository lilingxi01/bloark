# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Blocks Architecture (BloArk)'
copyright = '2023, Lingxi Li. All rights reserved'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.mathjax",
    "sphinx.ext.autodoc",
    "sphinx.ext.coverage",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    "myst_parser",
    "sphinx_copybutton",
    'sphinx_favicon',
]

suppress_warnings = ["myst.xref_missing", "myst.iref_ambiguous"]

autodoc = {
    'unstable': True,
}

templates_path = ['_templates']
exclude_patterns = ['.DS_Store']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_book_theme'
html_static_path = ['_static']
html_title = "BloArk"
html_theme_options = {
    "logo": {
        "image_light": "./_static/logo.png",
        "image_dark": "./_static/logo_dark.png",
    },
    "use_download_button": False,
    "use_fullscreen_button": False,
    "repository_url": "https://github.com/lilingxi01/bloark",
    "use_repository_button": True,
    "show_toc_level": 3,
    "announcement": "<b>WARNING:</b> BloArk is under active development and is not stable yet!",
}

favicons = [
    {"href": "favicon.ico"},
]


def mark_unstable(app, what, name, obj, options, lines):
    if getattr(obj, '__unstable__', False):
        unstable_since = getattr(obj, '__unstable_since__', None)
        unstable_message = getattr(obj, '__unstable_message__', None)
        unstable_mark_formats = [
            '.. caution::',
            '',
        ]
        if unstable_since:
            unstable_mark_formats += [
                '   This %s is marked as **unstable** since version ``%s``.' % (what, unstable_since),
                '',
            ]
        else:
            unstable_mark_formats += [
                '   This %s is marked as **unstable**.' % what,
                '',
            ]
        if unstable_message:
            unstable_mark_formats += [
                '.. admonition:: Unstable notice',
                '',
                '   %s' % unstable_message,
                '',
            ]
        lines[:0] = unstable_mark_formats

    if getattr(obj, '__deprecated__', False):
        deprecation_version = getattr(obj, '__deprecated_version__', None)
        if not deprecation_version:
            return
        deprecation_message = getattr(obj, '__deprecated_message__', 'This %s is deprecated.' % what)
        lines[:0] = [
            '.. deprecated:: v%s' % deprecation_version,
            '',
            '   %s' % deprecation_message,
            '',
        ]


def setup(app):
    app.connect('autodoc-process-docstring', mark_unstable)
