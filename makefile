.PHONY: test compile-tests docs no-warn

MocvVersion = 0.10.2
MocvPath = $(shell mocv bin $$MocvVersion)

set-moc-version:
	mocv use $(MocvVersion)

set-dfx-moc-path: set-moc-version
	export DFX_MOC_PATH=$(MocvPath)/moc

test: set-moc-version
	mops test

check: set-moc-version
	find src -type f -name '*.mo' -print0 | xargs -0 $(MocvPath)/moc -r $(shell mops sources) -Werror -wasi-system-api

docs:  set-moc-version
	$(MocvPath)/mo-doc
	$(MocvPath)/mo-doc --format plain

bench: set-dfx-moc-path
	mops bench

canister-tests:
	zx -i ./z-scripts/canister-tests.mjs 
	