# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   = sphinx-build
SOURCEDIR     = source
BUILDDIR      = build
HTMLBUILDDIR  = htmlbuild
PDFBUILDDIR   = pdfbuild

# Put it first so that "make" without argument is like "make help".
help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  clean      to delete output"
	@echo "  html       to make standalone HTML files"
	@echo "  latexpdf   to make LaTeX files and run them through pdflatex"
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

.PHONY: clean
clean:
	rm -rf $(BUILDDIR); \
	rm -rf $(HTMLBUILDDIR); \
	rm -rf ${PDFBUILDDIR};

.PHONY: html
html:
	mkdir -p ${HTMLBUILDDIR}; \
	$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(HTMLBUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: latexpdf
latexpdf:
	mkdir -p ${PDFBUILDDIR}; \
	$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(PDFBUILDDIR)" $(SPHINXOPTS) $(O)

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
