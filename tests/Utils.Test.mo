import { test } "mo:test";
import Debug "mo:base@0/Debug";
import Utils "../src/BaseAssets/Utils";

test(
  "format key",
  func() {
    let testCases = [
      ("path/to/file", "/path/to/file"),
      ("/path/to/file", "/path/to/file"),
      ("path/to/file/", "/path/to/file"),
      ("/path/to/file/", "/path/to/file"),
      ("path/to/file//", "/path/to/file"),
      ("/path/to/file//", "/path/to/file"),
    ];
    for (testCase in testCases.vals()) {
      let (input, expected) = testCase;
      let result = Utils.format_key(input);
      if (result != expected) {
        Debug.trap("Test failed:\nInput\n" # input # "\nExpected\n" # expected # "\nActual\n" # result);
      };
    };
  },
);
