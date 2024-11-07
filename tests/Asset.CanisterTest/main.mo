import Tests "tests";

actor {
    public func test() : async () {
        await Tests.test();
    };
};
