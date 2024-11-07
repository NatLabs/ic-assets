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

actor-test:
	dfx deploy asset-test
	dfx ledger fabricate-cycles --canister asset-test

	# Add the asset-test canister as a controller of itself
	dfx canister update-settings asset-test --add-controller $$(dfx canister id asset-test) 

	# Run the tests
	dfx canister call asset-test run_query_tests
	dfx canister call asset-test run_tests
	dfx canister call asset-test get_test_details