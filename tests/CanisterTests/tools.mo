import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Result "mo:base/Result";
import Text "mo:base/Text";

import Map "mo:map/Map";
import Serde "mo:serde"

module {
    type Result<A, B> = Result.Result<A, B>;

    let { thash } = Map;

    public type TestTools = {
        ts_assert : AssertFn;
        ts_print : PrintFn;
        ts_assert_or_print : AssertOrPrintFn;
    };

    public type AssertFn = (Bool) -> ();
    public type PrintFn = (Text) -> ();
    public type AssertOrPrintFn = (Bool, Text) -> ();

    public type QueryTestFn = (TestTools) -> ();
    public type UpdateTestFn = (TestTools) -> async ();

    public type TestFn = {
        #Query : QueryTestFn;
        #Update : UpdateTestFn;
    };

    public type Test = {
        test_fn : TestFn;
        var result : ?Bool;
        print_log : Buffer.Buffer<Text>;
    };

    public type TestResult = {
        name : Text;
        result : Bool;
        print_log : [Text];
    };

    public type TestDetails = {
        name : Text;
        is_query : Bool;
    };

    public class Suite() {

        let tests = Map.new<Text, Test>();

        public func add(name : Text, test_fn : UpdateTestFn) : () {
            ignore Map.put<Text, Test>(
                tests,
                thash,
                name,
                {
                    var result = null;
                    test_fn = #Update(test_fn);
                    print_log = Buffer.Buffer<Text>(8);
                },
            );
        };

        public func add_query(name : Text, test_fn : QueryTestFn) : () {
            ignore Map.put<Text, Test>(
                tests,
                thash,
                name,
                {
                    var result = null;
                    test_fn = #Query(test_fn);
                    print_log = Buffer.Buffer<Text>(8);
                },
            );
        };

        func get_test_tools(test : Test) : TestTools {
            let test_tools = {
                ts_assert = func(result : Bool) = switch (test.result) {
                    case (null) test.result := ?result;
                    case (?old_result) test.result := ?(old_result and result);
                };
                ts_print = func(msg : Text) {
                    test.print_log.add(msg);
                };
                ts_assert_or_print = func(result : Bool, msg : Text) {
                    switch (test.result) {
                        case (null) test.result := ?result;
                        case (?old_result) test.result := ?(old_result and result);
                    };

                    if (not result) {
                        test_tools.ts_print(msg);
                    };
                };

            };

            test_tools;
        };

        public func run(test_name : Text) : async (TestResult, Text) {

            let test = switch (Map.get(tests, thash, test_name)) {
                case (?test) test;
                case (null) Debug.trap("Test '" # test_name # "'' not found");
            };

            switch (test.test_fn) {
                case (#Update(test_fn)) { await test_fn(get_test_tools(test)) };
                case (#Query(test_fn)) {
                    Debug.trap("Test '" # test_name # "'' is a query test, use run_query instead");
                };
            };

            let test_result = {
                name = test_name;
                result = switch (test.result) {
                    case (?result) result;
                    case (null) true;
                };
                print_log = Buffer.toArray(test.print_log);
            };

            let test_result_candid = to_candid (test_result);
            let #ok(test_result_in_json) = Serde.JSON.toText(test_result_candid, ["result", "name", "print_log"], null);
            (test_result, test_result_in_json);
        };

        public func run_query(test_name : Text) : (TestResult, Text) {
            let test = switch (Map.get(tests, thash, test_name)) {
                case (?test) { test };
                case (null) Debug.trap("Test '" # test_name # "'' not found");
            };

            switch (test.test_fn) {
                case (#Update(test_fn)) {
                    Debug.trap("Test '" # test_name # "'' is an update test, use run instead");
                };
                case (#Query(test_fn)) { test_fn(get_test_tools(test)) };
            };

            let test_result = {
                name = test_name;
                result = switch (test.result) {
                    case (?result) result;
                    case (null) true;
                };
                print_log = Buffer.toArray(test.print_log);
            };

            let test_result_candid = to_candid (test_result);
            let #ok(test_result_in_json) = Serde.JSON.toText(test_result_candid, ["result", "name", "print_log"], null);
            (test_result, test_result_in_json);
        };

        public func get_test_result(test_name : Text) : (TestResult, Text) {
            let test = switch (Map.get(tests, thash, test_name)) {
                case (?test) { test };
                case (null) Debug.trap("Test '" # test_name # "'' not found");
            };

            let test_result = {
                name = test_name;
                result = switch (test.result) {
                    case (?result) result;
                    case (null) true;
                };
                print_log = Buffer.toArray(test.print_log);
            };

            let test_result_candid = to_candid (test_result);
            let #ok(test_result_in_json) = Serde.JSON.toText(test_result_candid, ["result", "name", "print_log"], null);
            (test_result, test_result_in_json);
        };

        public func get_finished_test_results() : ([TestResult], Text) {
            let test_results = Buffer.Buffer<TestResult>(Map.size(tests));

            for ((name, test) in Map.entries(tests)) {
                test_results.add({
                    name;
                    result = switch (test.result) {
                        case (?result) result;
                        case (null) true;
                    };
                    print_log = Buffer.toArray(test.print_log);
                });
            };

            let test_results_array = Buffer.toArray(test_results);
            let test_results_candid = to_candid (test_results_array);
            let #ok(test_results_in_json) = Serde.JSON.toText(test_results_candid, ["result", "name", "print_log"], null);

            (test_results_array, test_results_in_json);

        };

        public func get_test_details() : ([TestDetails], Text) {
            let test_details = Buffer.Buffer<TestDetails>(Map.size(tests));

            for ((name, test) in Map.entries(tests)) {
                test_details.add({
                    name;
                    is_query = switch (test.test_fn) {
                        case (#Query(_)) true;
                        case (#Update(_)) false;
                    };
                });
            };

            let test_details_array = Buffer.toArray(test_details);
            let test_details_candid = to_candid (test_details_array);
            let #ok(test_details_in_json) = Serde.JSON.toText(test_details_candid, ["name", "is_query"], null);

            (test_details_array, test_details_in_json);

        };

    };

    public func exists_in<A>(array : [A], equal : (A, A) -> Bool, value : A) : Bool {
        for (element in array.vals()) {
            if (equal(element, value)) { return true };
        };
        false;
    };
};
