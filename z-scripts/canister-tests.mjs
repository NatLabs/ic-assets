#!/usr/bin/env zx
import { parse_json, print_test_result, entitle } from "./utils.mjs";
import chalk from "chalk";

$.verbose = true; // set to true so that we can see the deployment logs and respond if ther are input prompts
const is_deploy = "deploy" in argv ? argv.deploy : true;

if (is_deploy) {
  entitle("Deploying canister-tests canister...");
  await $`dfx deploy canister-tests`;
}

await $`dfx ledger fabricate-cycles --canister canister-tests`;

entitle("Adding canister-tests canister as a controller...");
// Add the canister-tests canister as a controller of itself
await $`dfx canister update-settings canister-tests --add-controller $(dfx canister id canister-tests)`;

$.verbose = false;
entitle("Retrieving test details...");
const test_details_raw_test =
  await $`dfx canister call canister-tests get_test_details | idl2json -c`;

const test_details = parse_json(test_details_raw_test.stdout);

let passed = 0;
let failed = 0;

const test_prefix = argv?.test || "";

entitle("\nRunning tests...\n");

for (let test of test_details) {
  if (!is_deploy && !test.name.startsWith(test_prefix)) {
    continue;
  }

  let args = `("${test.name}")`;

  try {
    let raw_test = test.is_query
      ? await $`dfx canister call canister-tests run_query_test ${args} | idl2json -c`
      : await $`dfx canister call canister-tests run_test  ${args} | idl2json -c`;

    test = { ...test, ...parse_json(raw_test.stdout) };
  } catch (e) {
    let raw_test =
      await $`dfx canister call canister-tests get_test_result ${args} | idl2json -c`;

    test = { ...test, ...parse_json(raw_test.stdout) };
    test.result = false;
    test.print_log.push(`${chalk.red("[Canister Error] ")} ${e.stderr}`);
  }

  print_test_result(test);

  if (test.result) {
    passed += 1;
  } else {
    failed += 1;
  }
}

console.log("\n" + `Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
  console.log(chalk.bold.green("All tests passed!"));
} else {
  process.exit(1);
}
