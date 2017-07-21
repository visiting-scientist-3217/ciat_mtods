MODULES = cassava_ontology.py chado.py cx_oracle.py migration.py mtods.py \
	resetter.py table_guru.py task_storage.py unittest_helper.py utility.py
MODULES_doc = $(foreach i, $(MODULES), --add-module $(i))

.PHONY: test docs clean
test:
	. ../.env && ./unittests.py
docs:
	rm -rf apidocs
	-pydoctor $(MODULES_doc) --docformat=plaintext
clean:
	rm -rf apidocs
	rm -f *.pyc
	rm -f tags
	ls -lhB *.dump

# vim: noet ts=4 sts=4 sw=4
